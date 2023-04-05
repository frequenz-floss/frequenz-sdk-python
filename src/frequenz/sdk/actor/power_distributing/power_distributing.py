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
from dataclasses import dataclass, replace
from math import ceil, floor, isnan
from typing import (  # pylint: disable=unused-import
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
)

import grpc
from frequenz.channels import Bidirectional, Peekable, Receiver, Sender
from google.protobuf.empty_pb2 import Empty  # pylint: disable=no-name-in-module

from ..._internal.asyncio import cancel_and_await
from ...actor._decorator import actor
from ...microgrid import ComponentGraph, connection_manager
from ...microgrid.client import MicrogridApiClient
from ...microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    InverterData,
)
from ...power import DistributionAlgorithm, DistributionResult, InvBatPair
from ._battery_pool_status import BatteryPoolStatus, BatteryStatus
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


@actor
class PowerDistributingActor:
    # pylint: disable=too-many-instance-attributes
    """Actor to distribute the power between batteries in a microgrid.

    The purpose of this tool is to keep an equal SoC level in all batteries.
    The PowerDistributingActor can have many concurrent users which at this time
    need to be known at construction time.

    For each user a bidirectional channel needs to be created through which
    they can send and receive requests and responses.

    It is recommended to wait for PowerDistributingActor output with timeout. Otherwise if
    the processing function fails then the response will never come.
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
        from frequenz.sdk.actor.power_distributing import (
            PowerDistributingActor,
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
        power_distributor = PowerDistributingActor(
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
        users_channels: Dict[str, Bidirectional.Handle[Result, Request]],
        battery_status_sender: Sender[BatteryStatus],
        wait_for_data_sec: float = 2,
    ) -> None:
        """Create class instance.

        Args:
            users_channels: BidirectionalHandle for each user. Key should be
                user id and value should be BidirectionalHandle.
            battery_status_sender: Channel for sending information which batteries are
                working.
            wait_for_data_sec: How long actor should wait before processing first
                request. It is a time needed to collect first components data.
        """
        self._wait_for_data_sec = wait_for_data_sec

        # NOTE: power_distributor_exponent should be received from ConfigManager
        self.power_distributor_exponent: float = 1.0
        self.distribution_algorithm = DistributionAlgorithm(
            self.power_distributor_exponent
        )

        self._bat_inv_map, self._inv_bat_map = self._get_components_pairs(
            connection_manager.get().component_graph
        )
        self._battery_receivers: Dict[int, Peekable[BatteryData]] = {}
        self._inverter_receivers: Dict[int, Peekable[InverterData]] = {}

        # The components in different requests be for the same components, or for
        # completely different components. They should not overlap.
        # Otherwise the PowerDistributingActor has no way to decide what request is more
        # important. It will execute both. And later request will override the previous
        # one.
        # That is why the queue of maxsize = total number of batteries should be enough.
        self._request_queue: asyncio.Queue[Tuple[Request, _User]] = asyncio.Queue(
            maxsize=len(self._bat_inv_map)
        )

        self._users_channels: Dict[
            str, Bidirectional.Handle[Result, Request]
        ] = users_channels
        self._users_tasks = self._create_users_tasks()
        self._started = asyncio.Event()

        self._all_battery_status = BatteryPoolStatus(
            battery_ids=set(self._bat_inv_map.keys()),
            battery_status_sender=battery_status_sender,
            max_blocking_duration_sec=30.0,
            max_data_age_sec=10.0,
        )

    def _create_users_tasks(self) -> List[asyncio.Task[None]]:
        """For each user create a task to wait for request.

        Returns:
            List with users tasks.
        """
        tasks = []
        for user, handler in self._users_channels.items():
            tasks.append(
                asyncio.create_task(self._wait_for_request(_User(user, handler)))
            )
        return tasks

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

        api = connection_manager.get().api_client

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
                await user.channel.send(Error(request=request, msg=str(err)))
                continue

            if len(pairs_data) == 0:
                error_msg = f"No data for the given batteries {str(request.batteries)}"
                await user.channel.send(Error(request=request, msg=str(error_msg)))
                continue

            try:
                distribution = self.distribution_algorithm.distribute_power(
                    request.power, pairs_data
                )
            except ValueError as err:
                error_msg = f"Couldn't distribute power, error: {str(err)}"
                await user.channel.send(Error(request=request, msg=str(error_msg)))
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

            failed_power, failed_batteries = await self._set_distributed_power(
                api, distribution, request.request_timeout_sec
            )

            response: Success | PartialFailure
            if len(failed_batteries) > 0:
                succeed_batteries = set(battery_distribution.keys()) - failed_batteries
                response = PartialFailure(
                    request=request,
                    succeeded_power=distributed_power_value,
                    succeeded_batteries=succeed_batteries,
                    failed_power=failed_power,
                    failed_batteries=failed_batteries,
                    excess_power=distribution.remaining_power,
                )
            else:
                succeed_batteries = set(battery_distribution.keys())
                response = Success(
                    request=request,
                    succeeded_power=distributed_power_value,
                    succeeded_batteries=succeed_batteries,
                    excess_power=distribution.remaining_power,
                )

            asyncio.gather(
                *[
                    self._all_battery_status.update_status(
                        succeed_batteries, failed_batteries
                    ),
                    user.channel.send(response),
                ]
            )

    async def _set_distributed_power(
        self,
        api: MicrogridApiClient,
        distribution: DistributionResult,
        timeout_sec: float,
    ) -> Tuple[int, Set[int]]:
        """Send distributed power to the inverters.

        Args:
            api: Microgrid api client
            distribution: Distribution result
            timeout_sec: How long wait for the response

        Returns:
            Tuple where first element is total failed power, and the second element
            set of batteries that failed.
        """
        tasks = {
            inverter_id: asyncio.create_task(api.set_power(inverter_id, power))
            for inverter_id, power in distribution.distribution.items()
        }

        _, pending = await asyncio.wait(
            tasks.values(),
            timeout=timeout_sec,
            return_when=ALL_COMPLETED,
        )

        await self._cancel_tasks(pending)

        return self._parse_result(tasks, distribution.distribution, timeout_sec)

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
                return Error(request=request, msg=msg)

        if not request.adjust_power:
            if request.power < 0:
                bound = self._get_lower_bound(request.batteries)
                if request.power < bound:
                    return OutOfBound(request=request, bound=bound)
            else:
                bound = self._get_upper_bound(request.batteries)
                if request.power > bound:
                    return OutOfBound(request=request, bound=bound)

        return None

    def _remove_duplicated_requests(
        self, request: Request, user: _User
    ) -> List[asyncio.Task[None]]:
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
        to_ignore: List[asyncio.Task[None]] = []

        while not self._request_queue.empty():
            prev_request, prev_user = self._request_queue.get_nowait()
            # Generators seems to be the fastest
            if prev_request.batteries == batteries:
                task = asyncio.create_task(
                    prev_user.channel.send(Ignored(request=prev_request))
                )
                to_ignore.append(task)
            # Use generators as generators seems to be the fastest.
            elif any(battery_id in prev_request.batteries for battery_id in batteries):
                # If that happen PowerDistributingActor has no way to distinguish what
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

        Already existing requests for the same subset of batteries will be
        removed and their users will be notified with a Result.Status.IGNORED response.
        Only new request will re processed.
        If the sets of batteries are not the same but they have common elements,
        then both batteries will be processed.

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
                    _logger.error("No users in PowerDistributingActor!")
                return

            # Wait for PowerDistributingActor to start.
            if not self._started.is_set():
                await self._started.wait()

            # We should discover as fast as possible that request is wrong.
            error = self._check_request(request)
            if error is not None:
                await user.channel.send(error)
                continue

            tasks = self._remove_duplicated_requests(request, user)
            if self._request_queue.full():
                q_size = (self._request_queue.qsize(),)
                msg = (
                    f"Request queue is full {q_size}, can't process this request. "
                    "Consider increasing size of the queue."
                )
                _logger.error(msg)
                await user.channel.send(Error(request=request, msg=str(msg)))
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

    def _get_working_batteries(self, batteries: Set[int]) -> Set[int]:
        """Get subset with working batteries.

        If none of the given batteries are working, then treat all of them
        as working.

        Args:
            batteries: requested batteries

        Returns:
            Subset with working batteries or input set if none of the given batteries
                are working.
        """
        working_batteries = self._all_battery_status.get_working_batteries(batteries)
        if len(working_batteries) == 0:
            return batteries
        return working_batteries

    def _get_components_data(self, batteries: Set[int]) -> List[InvBatPair]:
        """Get data for the given batteries and adjacent inverters.

        Args:
            batteries: Batteries that needs data.

        Raises:
            KeyError: If any battery in the given list doesn't exists in microgrid.

        Returns:
            Pairs of battery and adjacent inverter data.
        """
        pairs_data: List[InvBatPair] = []
        working_batteries = self._get_working_batteries(batteries)

        for battery_id in working_batteries:
            if battery_id not in self._battery_receivers:
                raise KeyError(
                    f"No battery {battery_id}, "
                    f"available batteries: {list(self._battery_receivers.keys())}"
                )

            inverter_id: int = self._bat_inv_map[battery_id]

            data = self._get_battery_inverter_data(battery_id, inverter_id)
            if data is None:
                _logger.warning(
                    "Skipping battery %d because its message isn't correct.",
                    battery_id,
                )
                continue

            pairs_data.append(data)
        return pairs_data

    def _get_battery_inverter_data(
        self, battery_id: int, inverter_id: int
    ) -> Optional[InvBatPair]:
        """Get battery and inverter data if they are correct.

        Each float data from the microgrid can be "NaN".
        We can't do math operations on "NaN".
        So check all the metrics and:
        * if power bounds are NaN, then try to replace it with the corresponding
          power bounds from the adjacent component. If metric in the adjacent component
          is also NaN, then return None.
        * if other metrics are NaN then return None. We can't assume anything for other
          metrics.

        Args:
            battery_id: battery id
            inverter_id: inverter id

        Returns:
            Data for the battery and adjacent inverter without NaN values.
                Return None if we could not replace NaN values.
        """
        battery_data = self._battery_receivers[battery_id].peek()
        inverter_data = self._inverter_receivers[inverter_id].peek()

        # It means that nothing has been send on this channels, yet.
        # This should be handled by BatteryStatus. BatteryStatus should not return
        # this batteries as working.
        if battery_data is None or inverter_data is None:
            _logger.error(
                "Battery %d or inverter %d send no data, yet. They should be not used.",
                battery_id,
                inverter_id,
            )
            return None

        not_replaceable_metrics = [
            battery_data.soc,
            battery_data.soc_lower_bound,
            battery_data.soc_upper_bound,
            # We could replace capacity with 0, but it won't change distribution.
            # This battery will be ignored in distribution anyway.
            battery_data.capacity,
        ]
        if any(map(isnan, not_replaceable_metrics)):
            _logger.debug("Some metrics for battery %d are NaN", battery_id)
            return None

        replaceable_metrics = [
            battery_data.power_lower_bound,
            battery_data.power_upper_bound,
            inverter_data.active_power_lower_bound,
            inverter_data.active_power_upper_bound,
        ]

        # If all values are ok then return them.
        if not any(map(isnan, replaceable_metrics)):
            return InvBatPair(battery_data, inverter_data)

        # Replace NaN with the corresponding value in the adjacent component.
        # If both metrics are None, return None to ignore this battery.
        replaceable_pairs = [
            ("power_lower_bound", "active_power_lower_bound"),
            ("power_upper_bound", "active_power_upper_bound"),
        ]

        battery_new_metrics = {}
        inverter_new_metrics = {}
        for bat_attr, inv_attr in replaceable_pairs:
            bat_bound = getattr(battery_data, bat_attr)
            inv_bound = getattr(inverter_data, inv_attr)
            if isnan(bat_bound) and isnan(inv_bound):
                _logger.debug("Some metrics for battery %d are NaN", battery_id)
                return None
            if isnan(bat_bound):
                battery_new_metrics[bat_attr] = inv_bound
            elif isnan(inv_bound):
                inverter_new_metrics[inv_attr] = bat_bound

        return InvBatPair(
            replace(battery_data, **battery_new_metrics),
            replace(inverter_data, **inverter_new_metrics),
        )

    async def _create_channels(self) -> None:
        """Create channels to get data of components in microgrid."""
        api = connection_manager.get().api_client
        for battery_id, inverter_id in self._bat_inv_map.items():
            bat_recv: Receiver[BatteryData] = await api.battery_data(battery_id)
            self._battery_receivers[battery_id] = bat_recv.into_peekable()

            inv_recv: Receiver[InverterData] = await api.inverter_data(inverter_id)
            self._inverter_receivers[inverter_id] = inv_recv.into_peekable()

    def _parse_result(
        self,
        # type comment to quiet pylint and mypy `unused-import` error
        tasks,  # type: Dict[int, asyncio.Task[Empty]]
        distribution: Dict[int, int],
        request_timeout_sec: float,
    ) -> Tuple[int, Set[int]]:
        """Parse the results of `set_power` requests.

        Check if any task has failed and determine the reason for failure.
        If any task did not succeed, then the corresponding battery is marked as broken.

        Args:
            tasks: A dictionary where the key is the inverter ID and the value is the task that
                set the power for this inverter. Each task should be finished or cancelled.
            distribution: A dictionary where the key is the inverter ID and the value is how much
                power was set to the corresponding inverter.
            request_timeout_sec: The timeout that was used for the request.

        Returns:
            A tuple where the first element is the total failed power, and the second element is
            the set of batteries that failed.
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
            except asyncio.exceptions.CancelledError:
                failed_power += distribution[inverter_id]
                failed_batteries.add(battery_id)
                _logger.warning(
                    "Battery %d didn't respond in %f sec. Mark it as broken.",
                    battery_id,
                    request_timeout_sec,
                )

        return failed_power, failed_batteries

    async def _cancel_tasks(self, tasks: Iterable[asyncio.Task[Any]]) -> None:
        """Cancel given asyncio tasks and wait for them.

        Args:
            tasks: tasks to cancel.
        """
        for aws in tasks:
            aws.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _stop_actor(self) -> None:
        """Stop all running async tasks."""
        await asyncio.gather(*[cancel_and_await(t) for t in self._users_tasks])
        await self._all_battery_status.stop()
        await self._stop()  # type: ignore # pylint: disable=no-member
