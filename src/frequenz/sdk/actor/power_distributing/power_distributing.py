# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Actor to distribute power between batteries.

When charge/discharge method is called the power should be distributed so that
the SoC in batteries stays at the same level. That way of distribution
prevents using only one battery, increasing temperature, and maximize the total
amount power to charge/discharge.

Purpose of this actor is to keep SoC level of each component at the equal level.
"""

from __future__ import annotations

import asyncio
import logging
from asyncio.tasks import ALL_COMPLETED
from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil, floor
from typing import (  # pylint: disable=unused-import
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import grpc
from frequenz.channels import Bidirectional, Peekable, Receiver
from google.protobuf.empty_pb2 import Empty  # pylint: disable=no-name-in-module

from ...actor._decorator import actor
from ...microgrid import ComponentGraph
from ...microgrid.client import MicrogridApiClient
from ...microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    InverterData,
)
from ...power import DistributionAlgorithm, InvBatPair
from .request import Request
from .result import Error, Ignored, OutOfBound, PartialFailure, Result, Success

_logger = logging.getLogger(__name__)


@dataclass
class _User:
    """User definitions."""

    user_id: str
    """The unique identifier for a user of the power distributing actor."""

    # Channel for the communication
    channel: Bidirectional.Handle[Result, Request]
    """The bidirectional channel to communicate with the user."""


class _BrokenComponents:
    """Store components marked as broken."""

    def __init__(self, timeout_sec: float) -> None:
        """Create object instance.

        Args:
            timeout_sec: How long the component should be marked as broken.
        """
        self._broken: Dict[int, datetime] = {}
        self._timeout_sec = timeout_sec

    def mark_as_broken(self, component_id: int) -> None:
        """Mark component as broken.

        After marking component as broken it would be considered as broken for
        self._timeout_sec.

        Args:
            component_id: component id
        """
        self._broken[component_id] = datetime.now(timezone.utc)

    def update_retry(self, timeout_sec: float) -> None:
        """Change how long the component should be marked as broken.

        Args:
            timeout_sec: New retry time after sec.
        """
        self._timeout_sec = timeout_sec

    def get_working_subset(self, components_ids: Set[int]) -> Set[int]:
        """Get subset of batteries that are not marked as broken.

        If all given batteries are broken, then mark them as working and return them.
        This is temporary workaround to not block user command.

        Args:
            components_ids: set of component ids

        Returns:
            Subset of given components_ids with working components.
        """
        working = set(filter(lambda cid: not self.is_broken(cid), components_ids))

        if len(working) == 0:
            _logger.warning(
                "All requested components: %s are marked as broken. "
                "Marking them as working to not block command.",
                str(components_ids),
            )

            for cid in components_ids:
                self._broken.pop(cid, None)

            working = components_ids

        return working

    def is_broken(self, component_id: int) -> bool:
        """Check if component is marked as broken.

        Args:
            component_id: component id

        Returns:
            True if component is broken, False otherwise.
        """
        if component_id in self._broken:
            last_broken = self._broken[component_id]
            if (
                datetime.now(timezone.utc) - last_broken
            ).total_seconds() < self._timeout_sec:
                return True

            del self._broken[component_id]
        return False


@actor
class PowerDistributingActor:
    # pylint: disable=too-many-instance-attributes
    """Tool to distribute power between batteries in microgrid.

    The purpose of this tool is to keep equal SoC level in the batteries.
    PowerDistributor can have many users. Each user should first register in order
    to get its id, channel for sending request, and channel for receiving response.

    It is recommended to wait for PowerDistributor output with timeout. Otherwise if
    the processing function fail then the response will never come.
    The timeout should be Result:request_timeout_sec + time for processing the request.

    Edge cases:
    * If there are 2 requests to be processed for the same subset of batteries, then
    only the latest request will be processed. Older request will be ignored. User with
    older request will get response with Result.Status.IGNORED.

    * If there are 2 requests and their subset of batteries is different but they
    overlap (they have at least one common battery), then then both batteries
    will be processed. However it is not expected so the proper error log will be
    printed.

    Example:
        ``` python
        import grpc.aio as grpcaio

        from frequenz.sdk.microgrid.graph import _MicrogridComponentGraph
        from frequenz.sdk.microgrid.component import ComponentCategory
        from frequenz.sdk.actor.power_distribution import (
            PowerDistributor,
            Request,
            Result,
            Success,
            Error,
            PartialFailure,
            Ignored,
        )


        target = f"{host}:{port}"
        grpc_channel = grpcaio.insecure_channel(target)
        api = MicrogridGrpcClient(grpc_channel, target)

        graph = _MicrogridComponentGraph()
        await graph.refresh_from_api(api)

        batteries = graph.components(component_category={ComponentCategory.BATTERY})
        batteries_ids = {c.component_id for c in batteries}

        channel = Bidirectional[Request, Result]("user1", "power_distributor")
        power_distributor = PowerDistributor(
            mock_api, component_graph, {"user1": channel.service_handle}
        )

        client_handle = channel.client_handle

        # Set power 1200W to given batteries.
        request = Request(power=1200, batteries=batteries_ids, request_timeout_sec=10.0)
        await client_handle.send(request)

        # It is recommended to use timeout when waiting for the response!
        result: Result = await asyncio.wait_for(client_handle.receive(), timeout=10)

        if isinstance(result, Success):
            print("Command succeed")
        elif isinstance(result, PartialFailure):
            print(
                f"Batteries {result.failed_batteries} failed, total failed power" \
                    f"{result.failed_power}")
        elif isinstance(result, Ignored):
            print(f"Request was ignored, because of newer request")
        elif isinstance(result, Error):
            print(f"Request failed with error: {result.msg}")
        ```
    """

    def __init__(
        self,
        microgrid_api: MicrogridApiClient,
        component_graph: ComponentGraph,
        users_channels: Dict[str, Bidirectional.Handle[Result, Request]],
        wait_for_data_sec: float = 2,
    ) -> None:
        """Create class instance.

        Args:
            microgrid_api: api for sending the requests.
            component_graph: component graph of the given microgrid api.
            users_channels: BidirectionalHandle for each user. Key should be
                user id and value should be BidirectionalHandle.
            wait_for_data_sec: How long actor should wait before processing first
                request. It is a time needed to collect first components data.
        """
        self._api = microgrid_api
        self._wait_for_data_sec = wait_for_data_sec

        # Max permitted time when the component should send any information.
        # After that timeout the component will be treated as not existing.
        # Formulas will put 0 in place of data from this components.
        # This will happen until component starts sending data.
        self.component_data_timeout_sec: float = 60.0
        self.broken_component_timeout_sec: float = 30.0
        self.power_distributor_exponent: float = 1.0

        # distributor_exponent and timeout_sec should be get from ConfigManager
        self.distribution_algorithm = DistributionAlgorithm(
            self.power_distributor_exponent
        )
        self._broken_components = _BrokenComponents(self.broken_component_timeout_sec)

        self._bat_inv_map, self._inv_bat_map = self._get_components_pairs(
            component_graph
        )
        self._battery_receivers: Dict[int, Peekable[BatteryData]] = {}
        self._inverter_receivers: Dict[int, Peekable[InverterData]] = {}

        # The components in different requests be for the same components, or for
        # completely different components. They should not overlap.
        # Otherwise the PowerDistributor has no way to decide what request is more
        # important. It will execute both. And later request will override the previous
        # one.
        # That is why the queue of maxsize = total number of batteries should be enough.
        self._request_queue: asyncio.Queue[Tuple[Request, _User]] = asyncio.Queue(
            maxsize=len(self._bat_inv_map)
        )

        self._users_channels: Dict[
            str, Bidirectional.Handle[Result, Request]
        ] = users_channels
        self._create_users_tasks()
        self._started = asyncio.Event()

    def _create_users_tasks(self) -> None:
        """For each user create a task to wait for request."""
        for user, handler in self._users_channels.items():
            asyncio.create_task(self._wait_for_request(_User(user, handler)))

    def _get_upper_bound(self, batteries: Set[int]) -> int:
        """Get total upper bound of power to be set for given batteries.

        Note, output of that function doesn't guarantee that this bound will be
        the same when the request is processed.

        Args:
            batteries: List of batteries

        Returns:
            Upper bound for `set_power` operation.
        """
        pairs_data: List[InvBatPair] = self._get_components_data(batteries)
        bound = sum(
            min(battery.power_upper_bound, inverter.active_power_upper_bound)
            for battery, inverter in pairs_data
        )
        return floor(bound)

    def _get_lower_bound(self, batteries: Set[int]) -> int:
        """Get total lower bound of power to be set for given batteries.

        Note, output of that function doesn't guarantee that this bound will be
        the same when the request is processed.

        Args:
            batteries: List of batteries

        Returns:
            Lower bound for `set_power` operation.
        """
        pairs_data: List[InvBatPair] = self._get_components_data(batteries)
        bound = sum(
            max(battery.power_lower_bound, inverter.active_power_lower_bound)
            for battery, inverter in pairs_data
        )
        return ceil(bound)

    async def run(self) -> None:
        """Run actor main function.

        It waits for new requests in task_queue and process it, and send
        `set_power` request with distributed power.
        The output of the `set_power` method is processed.
        Every battery and inverter that failed or didn't respond in time will be marked
        as broken for some time.
        """
        await self._create_channels()

        # Wait few seconds to get data from the channels created above.
        await asyncio.sleep(self._wait_for_data_sec)
        self._started.set()
        while True:
            request, user = await self._request_queue.get()

            try:
                pairs_data: List[InvBatPair] = self._get_components_data(
                    request.batteries
                )
            except KeyError as err:
                await user.channel.send(Error(request, str(err)))
                continue

            if len(pairs_data) == 0:
                error_msg = f"No data for the given batteries {str(request.batteries)}"
                await user.channel.send(Error(request, str(error_msg)))
                continue

            try:
                distribution = self.distribution_algorithm.distribute_power(
                    request.power, pairs_data
                )
            except ValueError as err:
                error_msg = f"Couldn't distribute power, error: {str(err)}"
                await user.channel.send(Error(request, str(error_msg)))
                continue

            distributed_power_value = request.power - distribution.remaining_power
            battery_distribution = {
                self._inv_bat_map[bat_id]: dist
                for bat_id, dist in distribution.distribution.items()
            }
            _logger.debug(
                "%s: Distributing power %d between the batteries %s",
                user.user_id,
                distributed_power_value,
                str(battery_distribution),
            )

            tasks = {
                inverter_id: asyncio.create_task(
                    self._api.set_power(inverter_id, power)
                )
                for inverter_id, power in distribution.distribution.items()
            }

            _, pending = await asyncio.wait(
                tasks.values(),
                timeout=request.request_timeout_sec,
                return_when=ALL_COMPLETED,
            )

            await self._cancel_tasks(pending)
            failed_power, failed_batteries = self._parse_result(
                tasks, distribution.distribution, request.request_timeout_sec
            )

            if len(failed_batteries) > 0:
                succeed_batteries = set(battery_distribution.keys()) - failed_batteries
                await user.channel.send(
                    PartialFailure(
                        request=request,
                        succeed_power=distributed_power_value,
                        succeed_batteries=succeed_batteries,
                        failed_power=failed_power,
                        failed_batteries=failed_batteries,
                        excess_power=distribution.remaining_power,
                    )
                )
            else:
                await user.channel.send(
                    Success(
                        request=request,
                        succeed_power=distributed_power_value,
                        used_batteries=set(battery_distribution.keys()),
                        excess_power=distribution.remaining_power,
                    )
                )

    def _check_request(self, request: Request) -> Optional[Result]:
        """Check whether the given request if correct.

        Args:
            request: request to check

        Returns:
            Result for the user if the request is wrong, None otherwise.
        """
        for battery in request.batteries:
            if battery not in self._battery_receivers:
                msg = (
                    f"No battery {battery}, available batteries: "
                    f"{list(self._battery_receivers.keys())}"
                )
                return Error(request, msg)

        if not request.adjust_power:
            if request.power < 0:
                bound = self._get_lower_bound(request.batteries)
                if request.power < bound:
                    return OutOfBound(request, bound)
            else:
                bound = self._get_upper_bound(request.batteries)
                if request.power > bound:
                    return OutOfBound(request, bound)

        return None

    def _remove_duplicated_requests(
        self, request: Request, user: _User
    ) -> List[asyncio.Task[bool]]:
        """Remove duplicated requests from the queue.

        Remove old requests in which set of batteries are the same as in new request.
        If batteries in new request overlap with batteries in old request but are not
        equal, then log error and process both messages.

        Args:
            request: request to check
            user: User who sent the request.

        Returns:
            Tasks with result sent to the users which requests were duplicated.
        """
        batteries = request.batteries

        good_requests: List[Tuple[Request, _User]] = []
        to_ignore: List[asyncio.Task[bool]] = []

        while not self._request_queue.empty():
            prev_request, prev_user = self._request_queue.get_nowait()
            # Generators seems to be the fastest
            if prev_request.batteries == batteries:
                task = asyncio.create_task(
                    prev_user.channel.send(Ignored(prev_request))
                )
                to_ignore.append(task)
            # Use generators as generators seems to be the fastest.
            elif any(battery_id in prev_request.batteries for battery_id in batteries):
                # If that happen PowerDistributor has no way to distinguish what
                # request is more important. This should not happen
                _logger.error(
                    "Batteries in two requests overlap! Actor: %s requested %s "
                    "and Actor: %s requested %s",
                    user.user_id,
                    str(request),
                    prev_user.user_id,
                    str(prev_request),
                )
                good_requests.append((prev_request, prev_user))
            else:
                good_requests.append((prev_request, prev_user))

        for good_request in good_requests:
            self._request_queue.put_nowait(good_request)
        return to_ignore

    async def _wait_for_request(self, user: _User) -> None:
        """Wait for the request from user.

        Check if request is correct. If request is not correct send ERROR response
        to the user. If request is correct, then add it to the main queue to be
        process.

        If main queue has request for the same subset of batteries, then remove older
        request, and send its user response with Result.Status.IGNORED.
        Only new request will re processed.
        If set of batteries are not the same but have common elements, then both
        batteries will be processed.

        Args:
            user: User that sends the requests.
        """
        while True:
            request: Optional[Request] = await user.channel.receive()
            if request is None:
                _logger.info(
                    "Send channel for user %s was closed. User will be unregistered.",
                    user.user_id,
                )

                self._users_channels.pop(user.user_id)
                if len(self._users_channels) == 0:
                    _logger.error("No users in PowerDistributor!")
                return

            # Wait for PowerDistributor to start.
            if not self._started.is_set():
                await self._started.wait()

            # We should discover as fast as possible that request is wrong.
            check_result = self._check_request(request)
            if check_result is not None:
                await user.channel.send(check_result)
                continue

            tasks = self._remove_duplicated_requests(request, user)
            if self._request_queue.full():
                q_size = (self._request_queue.qsize(),)
                msg = (
                    f"Request queue is full {q_size}, can't process this request. "
                    "Consider increasing size of the queue."
                )
                _logger.error(msg)
                await user.channel.send(Error(request, str(msg)))
            else:
                self._request_queue.put_nowait((request, user))
                await asyncio.gather(*tasks)

    def _get_components_pairs(
        self, component_graph: ComponentGraph
    ) -> Tuple[Dict[int, int], Dict[int, int]]:
        """Create maps between battery and adjacent inverter.

        Args:
            component_graph: component graph

        Returns:
            Tuple where first element is map between battery and adjacent inverter,
                second element of the tuple is map between inverter and adjacent
                battery.
        """
        bat_inv_map: Dict[int, int] = {}
        inv_bat_map: Dict[int, int] = {}

        batteries: Iterable[Component] = component_graph.components(
            component_category={ComponentCategory.BATTERY}
        )

        for battery in batteries:
            inverters: List[Component] = [
                component
                for component in component_graph.predecessors(battery.component_id)
                if component.category == ComponentCategory.INVERTER
            ]

            if len(inverters) == 0:
                _logger.error("No inverters for battery %d", battery.component_id)
                continue

            if len(inverters) > 1:
                _logger.error(
                    "Battery %d has more then one inverter. It is not supported now.",
                    battery.component_id,
                )

            bat_inv_map[battery.component_id] = inverters[0].component_id
            inv_bat_map[inverters[0].component_id] = battery.component_id

        return bat_inv_map, inv_bat_map

    def _get_components_data(self, batteries: Iterable[int]) -> List[InvBatPair]:
        """Get data for the given batteries and adjacent inverters.

        Args:
            batteries: Batteries that needs data.

        Raises:
            KeyError: If any battery in the given list doesn't exists in microgrid.

        Returns:
            Pairs of battery and adjacent inverter data.
        """
        pairs_data: List[InvBatPair] = []

        for battery_id in self._broken_components.get_working_subset(batteries):
            if battery_id not in self._battery_receivers:
                raise KeyError(
                    f"No battery {battery_id}, "
                    f"available batteries: {list(self._battery_receivers.keys())}"
                )

            inverter_id: int = self._bat_inv_map[battery_id]

            battery_data: Optional[BatteryData] = self._battery_receivers[
                battery_id
            ].peek()

            if not self._is_component_data_valid(battery_id, battery_data):
                continue

            inverter_data: Optional[InverterData] = self._inverter_receivers[
                inverter_id
            ].peek()

            if not self._is_component_data_valid(inverter_id, inverter_data):
                continue

            # None case already checked but mypy don't see that.
            if battery_data is not None and inverter_data is not None:
                pairs_data.append(InvBatPair(battery_data, inverter_data))
        return pairs_data

    def _is_component_data_valid(
        self, component_id: int, component_data: Union[None, BatteryData, InverterData]
    ) -> bool:
        """Check whether the component data from microgrid are correct.

        Args:
            component_id: component id
            component_data: component data instance

        Returns:
            True if data are correct, false otherwise
        """
        if component_data is None:
            _logger.warning(
                "No data from component %d.",
                component_id,
            )
            return False

        now = datetime.now(timezone.utc)
        time_delta = now - component_data.timestamp
        if time_delta.total_seconds() > self.component_data_timeout_sec:
            _logger.warning(
                "Component %d data are stale. Last timestamp: %s, now: %s",
                component_id,
                str(component_data.timestamp),
                str(now),
            )
            return False

        return True

    async def _create_channels(self) -> None:
        """Create channels to get data of components in microgrid."""
        for battery_id, inverter_id in self._bat_inv_map.items():
            bat_recv: Receiver[BatteryData] = await self._api.battery_data(battery_id)
            self._battery_receivers[battery_id] = bat_recv.into_peekable()

            inv_recv: Receiver[InverterData] = await self._api.inverter_data(
                inverter_id
            )
            self._inverter_receivers[inverter_id] = inv_recv.into_peekable()

    def _parse_result(
        self,
        # type comment to quiet pylint and mypy `unused-import` error
        tasks,  # type: Dict[int, asyncio.Task[Empty]]
        distribution: Dict[int, int],
        request_timeout_sec: float,
    ) -> Tuple[int, Set[int]]:
        """Parse result of `set_power` requests.

        Check if any task failed and why. If any task didn't success, then corresponding
        battery is marked as broken.

        Args:
            tasks: Dictionary where key is inverter id and value is task that set power
                for this inverter. Each tasks should be finished or cancelled.
            distribution: Dictionary where key is inverter id and value is how much
                power was set to the corresponding inverter.
            request_timeout_sec: timeout which has been used for request.

        Returns:
            Tuple where first element is total failed power, and the second element
            set of batteries that failed.
        """
        failed_power: int = 0
        failed_batteries: Set[int] = set()

        for inverter_id, aws in tasks.items():
            battery_id = self._inv_bat_map[inverter_id]
            try:
                aws.result()
            except grpc.aio.AioRpcError as err:
                failed_power += distribution[inverter_id]
                failed_batteries.add(battery_id)
                if err.code() == grpc.StatusCode.OUT_OF_RANGE:
                    _logger.debug(
                        "Set power for battery %d failed, error %s",
                        battery_id,
                        str(err),
                    )
                else:
                    _logger.warning(
                        "Set power for battery %d failed, error %s. Mark it as broken.",
                        battery_id,
                        str(err),
                    )
                    self._broken_components.mark_as_broken(battery_id)
            except asyncio.exceptions.CancelledError:
                failed_power += distribution[inverter_id]
                failed_batteries.add(battery_id)
                _logger.warning(
                    "Battery %d didn't respond in %f sec. Mark it as broken.",
                    battery_id,
                    request_timeout_sec,
                )
                self._broken_components.mark_as_broken(battery_id)

        return failed_power, failed_batteries

    async def _cancel_tasks(self, tasks: Iterable[asyncio.Task[Any]]) -> None:
        """Cancel given asyncio tasks and wait for them.

        Args:
            tasks: tasks to cancel.
        """
        for aws in tasks:
            aws.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
