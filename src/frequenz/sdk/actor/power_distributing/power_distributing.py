# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Actor to distribute active_power between batteries.

When charge/discharge method is called the active_power should be distributed so that
the SoC in batteries stays at the same level. That way of distribution
prevents using only one battery, increasing temperature, and maximize the total
amount active_power to charge/discharge.

Purpose of this actor is to keep SoC level of each component at the equal level.
"""

from __future__ import annotations

import asyncio
import logging
import time
from asyncio.tasks import ALL_COMPLETED
from collections import abc
from dataclasses import dataclass, replace
from datetime import timedelta
from math import isnan
from typing import Any, Dict, Iterable, List, Optional, Self, Set, Tuple

import grpc
from frequenz.channels import Peekable, Receiver, Sender

from ...actor import ChannelRegistry
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
from .result import Error, OutOfBound, PartialFailure, Result, Success

_logger = logging.getLogger(__name__)


@dataclass
class _CacheEntry:
    """Represents an entry in the cache with expiry time."""

    inv_bat_pair: InvBatPair
    """The inverter and adjacent battery data pair."""

    expiry_time: int
    """The expiration time (taken from the monotonic clock) of the cache entry."""

    @classmethod
    def from_ttl(
        cls, inv_bat_pair: InvBatPair, ttl: timedelta = timedelta(hours=2.5)
    ) -> Self:
        """Initialize a CacheEntry instance from a TTL (Time-To-Live).

        Args:
            inv_bat_pair: the inverter and adjacent battery data pair to cache.
            ttl: the time a cache entry is kept alive.

        Returns:
            this class instance.
        """
        return cls(inv_bat_pair, time.monotonic_ns() + int(ttl.total_seconds() * 1e9))

    def has_expired(self) -> bool:
        """Check whether the cache entry has expired.

        Returns:
            whether the cache entry has expired.
        """
        return time.monotonic_ns() >= self.expiry_time


@actor
class PowerDistributingActor:
    # pylint: disable=too-many-instance-attributes
    """Actor to distribute the active_power between batteries in a microgrid.

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
        ```python
        from frequenz.sdk import microgrid
        from frequenz.sdk.microgrid.component import ComponentCategory
        from frequenz.sdk.actor import ResamplerConfig
        from frequenz.sdk.actor.power_distributing import (
            PowerDistributingActor,
            Request,
            Result,
            Success,
            Error,
            PartialFailure,
            Ignored,
        )
        from frequenz.channels import Broadcast, Receiver, Sender
        from datetime import timedelta
        from frequenz.sdk import actor

        HOST = "localhost"
        PORT = 50051

        await microgrid.initialize(
            HOST,
            PORT,
            ResamplerConfig(resampling_period=timedelta(seconds=1))
        )

        graph = microgrid.connection_manager.get().component_graph

        batteries = graph.components(component_category={ComponentCategory.BATTERY})
        batteries_ids = {c.component_id for c in batteries}

        battery_status_channel = Broadcast[BatteryStatus]("battery-status")

        channel = Broadcast[Request]("active_power_distributor")
        channel_registry = ChannelRegistry(name="active_power_distributor")
        active_power_distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        sender = channel.new_sender()
        namespace: str = "namespace"
        # Set active_power 1200W to given batteries.
        request = Request(
            namespace=namespace,
            active_power=1200.0,
            batteries=batteries_ids,
            request_timeout_sec=10.0
        )
        await sender.send(request)
        result_rx = channel_registry.new_receiver(namespace)

        # It is recommended to use timeout when waiting for the response!
        result: Result = await asyncio.wait_for(result_rx.receive(), timeout=10)

        if isinstance(result, Success):
            print("Command succeed")
        elif isinstance(result, PartialFailure):
            print(
                f"Batteries {result.failed_batteries} failed, total failed active_power" \
                f"{result.failed_active_power}"
            )
        elif isinstance(result, Ignored):
            print("Request was ignored, because of newer request")
        elif isinstance(result, Error):
            print(f"Request failed with error: {result.msg}")
        ```
    """

    def __init__(
        self,
        requests_receiver: Receiver[Request],
        channel_registry: ChannelRegistry,
        battery_status_sender: Sender[BatteryStatus],
        wait_for_data_sec: float = 2,
    ) -> None:
        """Create class instance.

        Args:
            requests_receiver: Receiver for receiving active_power requests from other actors.
            channel_registry: Channel registry for creating result channels dynamically
                for each request namespace.
            battery_status_sender: Channel for sending information which batteries are
                working.
            wait_for_data_sec: How long actor should wait before processing first
                request. It is a time needed to collect first components data.
        """
        self._requests_receiver = requests_receiver
        self._channel_registry = channel_registry
        self._wait_for_data_sec = wait_for_data_sec
        self._result_senders: Dict[str, Sender[Result]] = {}
        """Dictionary of result senders for each request namespace.

        They are for channels owned by the channel registry, we just hold a reference
        to their senders, for fast access.
        """

        # NOTE: active_power_distributor_exponent should be received from ConfigManager
        self.active_power_distributor_exponent: float = 1.0
        self.distribution_algorithm = DistributionAlgorithm(
            self.active_power_distributor_exponent
        )

        self._bat_inv_map, self._inv_bat_map = self._get_components_pairs(
            connection_manager.get().component_graph
        )
        self._battery_receivers: Dict[int, Peekable[BatteryData]] = {}
        self._inverter_receivers: Dict[int, Peekable[InverterData]] = {}

        self._all_battery_status = BatteryPoolStatus(
            battery_ids=set(self._bat_inv_map.keys()),
            battery_status_sender=battery_status_sender,
            max_blocking_duration_sec=30.0,
            max_data_age_sec=10.0,
        )

        self._cached_metrics: dict[int, _CacheEntry | None] = {
            bat_id: None for bat_id, _ in self._bat_inv_map.items()
        }

    def _get_upper_bound(self, batteries: abc.Set[int], include_broken: bool) -> float:
        """Get total upper bound of active_power to be set for given batteries.

        Note, output of that function doesn't guarantee that this bound will be
        the same when the request is processed.

        Args:
            batteries: List of batteries
            include_broken: whether all batteries in the batteries set in the
                request must be used regardless the status.

        Returns:
            Upper bound for `set_active_power` operation.
        """
        pairs_data: List[InvBatPair] = self._get_components_data(
            batteries, include_broken
        )
        return sum(
            min(battery.active_power_upper_bound, inverter.active_power_upper_bound)
            for battery, inverter in pairs_data
        )

    def _get_lower_bound(self, batteries: abc.Set[int], include_broken: bool) -> float:
        """Get total lower bound of active_power to be set for given batteries.

        Note, output of that function doesn't guarantee that this bound will be
        the same when the request is processed.

        Args:
            batteries: List of batteries
            include_broken: whether all batteries in the batteries set in the
                request must be used regardless the status.

        Returns:
            Lower bound for `set_active_power` operation.
        """
        pairs_data: List[InvBatPair] = self._get_components_data(
            batteries, include_broken
        )
        return sum(
            max(battery.active_power_lower_bound, inverter.active_power_lower_bound)
            for battery, inverter in pairs_data
        )

    async def _send_result(self, namespace: str, result: Result) -> None:
        """Send result to the user.

        Args:
            namespace: namespace of the sender, to identify the result channel with.
            result: Result to send out.
        """
        if not namespace in self._result_senders:
            self._result_senders[namespace] = self._channel_registry.new_sender(
                namespace
            )

        await self._result_senders[namespace].send(result)

    async def run(self) -> None:
        """Run actor main function.

        It waits for new requests in task_queue and process it, and send
        `set_active_power` request with distributed active_power.
        The output of the `set_active_power` method is processed.
        Every battery and inverter that failed or didn't respond in time will be marked
        as broken for some time.
        """
        await self._create_channels()

        api = connection_manager.get().api_client

        # Wait few seconds to get data from the channels created above.
        await asyncio.sleep(self._wait_for_data_sec)

        async for request in self._requests_receiver:
            error = self._check_request(request)
            if error:
                await self._send_result(request.namespace, error)
                continue

            try:
                pairs_data: List[InvBatPair] = self._get_components_data(
                    request.batteries, request.include_broken_batteries
                )
            except KeyError as err:
                await self._send_result(
                    request.namespace, Error(request=request, msg=str(err))
                )
                continue

            if not pairs_data and not request.include_broken_batteries:
                error_msg = f"No data for the given batteries {str(request.batteries)}"
                await self._send_result(
                    request.namespace, Error(request=request, msg=str(error_msg))
                )
                continue

            try:
                distribution = self._get_active_power_distribution(request, pairs_data)
            except ValueError as err:
                error_msg = f"Couldn't distribute active_power, error: {str(err)}"
                await self._send_result(
                    request.namespace, Error(request=request, msg=str(error_msg))
                )
                continue

            distributed_active_power_value = (
                request.active_power - distribution.remaining_active_power
            )
            battery_distribution = {
                self._inv_bat_map[bat_id]: dist
                for bat_id, dist in distribution.distribution.items()
            }
            _logger.debug(
                "Distributing active_power %d between the batteries %s",
                distributed_active_power_value,
                str(battery_distribution),
            )

            (
                failed_active_power,
                failed_batteries,
            ) = await self._set_distributed_active_power(
                api, distribution, request.request_timeout_sec
            )

            response: Success | PartialFailure
            if len(failed_batteries) > 0:
                succeed_batteries = set(battery_distribution.keys()) - failed_batteries
                response = PartialFailure(
                    request=request,
                    succeeded_active_power=distributed_active_power_value,
                    succeeded_batteries=succeed_batteries,
                    failed_active_power=failed_active_power,
                    failed_batteries=failed_batteries,
                    excess_active_power=distribution.remaining_active_power,
                )
            else:
                succeed_batteries = set(battery_distribution.keys())
                response = Success(
                    request=request,
                    succeeded_active_power=distributed_active_power_value,
                    succeeded_batteries=succeed_batteries,
                    excess_active_power=distribution.remaining_active_power,
                )

            asyncio.gather(
                *[
                    self._all_battery_status.update_status(
                        succeed_batteries, failed_batteries
                    ),
                    self._send_result(request.namespace, response),
                ]
            )

    async def _set_distributed_active_power(
        self,
        api: MicrogridApiClient,
        distribution: DistributionResult,
        timeout_sec: float,
    ) -> Tuple[float, Set[int]]:
        """Send distributed active_power to the inverters.

        Args:
            api: Microgrid api client
            distribution: Distribution result
            timeout_sec: How long wait for the response

        Returns:
            Tuple where first element is total failed active_power, and the second element
            set of batteries that failed.
        """
        tasks = {
            inverter_id: asyncio.create_task(
                api.set_active_power(inverter_id, active_power)
            )
            for inverter_id, active_power in distribution.distribution.items()
        }

        _, pending = await asyncio.wait(
            tasks.values(),
            timeout=timeout_sec,
            return_when=ALL_COMPLETED,
        )

        await self._cancel_tasks(pending)

        return self._parse_result(tasks, distribution.distribution, timeout_sec)

    def _get_active_power_distribution(
        self, request: Request, inv_bat_pairs: List[InvBatPair]
    ) -> DistributionResult:
        """Get active_power distribution result for the batteries in the request.

        Args:
            request: the active_power request to process.
            inv_bat_pairs: the battery and adjacent inverter data pairs.

        Returns:
            the active_power distribution result.
        """
        available_bat_ids = {battery.component_id for battery, _ in inv_bat_pairs}
        unavailable_bat_ids = request.batteries - available_bat_ids
        unavailable_inv_ids = {
            self._bat_inv_map[battery_id] for battery_id in unavailable_bat_ids
        }

        if request.include_broken_batteries and not available_bat_ids:
            return self.distribution_algorithm.distribute_active_power_equally(
                request.active_power, unavailable_inv_ids
            )

        result = self.distribution_algorithm.distribute_active_power(
            request.active_power, inv_bat_pairs
        )

        if request.include_broken_batteries and unavailable_inv_ids:
            additional_result = (
                self.distribution_algorithm.distribute_active_power_equally(
                    result.remaining_active_power, unavailable_inv_ids
                )
            )

            for inv_id, active_power in additional_result.distribution.items():
                result.distribution[inv_id] = active_power
            result.remaining_active_power = 0.0

        return result

    def _check_request(self, request: Request) -> Optional[Result]:
        """Check whether the given request if correct.

        Args:
            request: request to check

        Returns:
            Result for the user if the request is wrong, None otherwise.
        """
        if not request.batteries:
            return Error(request=request, msg="Empty battery IDs in the request")

        for battery in request.batteries:
            if battery not in self._battery_receivers:
                msg = (
                    f"No battery {battery}, available batteries: "
                    f"{list(self._battery_receivers.keys())}"
                )
                return Error(request=request, msg=msg)

        if not request.adjust_active_power:
            if request.active_power < 0:
                bound = self._get_lower_bound(
                    request.batteries, request.include_broken_batteries
                )
                if request.active_power < bound:
                    return OutOfBound(request=request, bound=bound)
            else:
                bound = self._get_upper_bound(
                    request.batteries, request.include_broken_batteries
                )
                if request.active_power > bound:
                    return OutOfBound(request=request, bound=bound)

        return None

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

    def _get_components_data(
        self, batteries: abc.Set[int], include_broken: bool
    ) -> List[InvBatPair]:
        """Get data for the given batteries and adjacent inverters.

        Args:
            batteries: Batteries that needs data.
            include_broken: whether all batteries in the batteries set in the
                request must be used regardless the status.

        Raises:
            KeyError: If any battery in the given list doesn't exists in microgrid.

        Returns:
            Pairs of battery and adjacent inverter data.
        """
        pairs_data: List[InvBatPair] = []
        working_batteries = (
            batteries
            if include_broken
            else self._all_battery_status.get_working_batteries(batteries)
        )

        for battery_id in working_batteries:
            if battery_id not in self._battery_receivers:
                raise KeyError(
                    f"No battery {battery_id}, "
                    f"available batteries: {list(self._battery_receivers.keys())}"
                )

            inverter_id: int = self._bat_inv_map[battery_id]

            data = self._get_battery_inverter_data(battery_id, inverter_id)
            if not data and include_broken:
                cached_entry = self._cached_metrics[battery_id]
                if cached_entry and not cached_entry.has_expired():
                    data = cached_entry.inv_bat_pair
                else:
                    data = None
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
        * if active_power bounds are NaN, then try to replace it with the corresponding
          active_power bounds from the adjacent component. If metric in the adjacent component
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
            battery_data.active_power_lower_bound,
            battery_data.active_power_upper_bound,
            inverter_data.active_power_lower_bound,
            inverter_data.active_power_upper_bound,
        ]

        # If all values are ok then return them.
        if not any(map(isnan, replaceable_metrics)):
            inv_bat_pair = InvBatPair(battery_data, inverter_data)
            self._cached_metrics[battery_id] = _CacheEntry.from_ttl(inv_bat_pair)
            return inv_bat_pair

        # Replace NaN with the corresponding value in the adjacent component.
        # If both metrics are None, return None to ignore this battery.
        replaceable_pairs = [
            ("active_power_lower_bound", "active_power_lower_bound"),
            ("active_power_upper_bound", "active_power_upper_bound"),
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

        inv_bat_pair = InvBatPair(
            replace(battery_data, **battery_new_metrics),
            replace(inverter_data, **inverter_new_metrics),
        )
        self._cached_metrics[battery_id] = _CacheEntry.from_ttl(inv_bat_pair)
        return inv_bat_pair

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
        tasks: Dict[int, asyncio.Task[None]],
        distribution: Dict[int, float],
        request_timeout_sec: float,
    ) -> Tuple[float, Set[int]]:
        """Parse the results of `set_active_power` requests.

        Check if any task has failed and determine the reason for failure.
        If any task did not succeed, then the corresponding battery is marked as broken.

        Args:
            tasks: A dictionary where the key is the inverter ID and the value is the task that
                set the active_power for this inverter. Each task should be finished or cancelled.
            distribution: A dictionary where the key is the inverter ID and the value is how much
                active_power was set to the corresponding inverter.
            request_timeout_sec: The timeout that was used for the request.

        Returns:
            A tuple where the first element is the total failed active_power,
            and the second element is
            the set of batteries that failed.
        """
        failed_active_power: float = 0.0
        failed_batteries: Set[int] = set()

        for inverter_id, aws in tasks.items():
            battery_id = self._inv_bat_map[inverter_id]
            try:
                aws.result()
            except grpc.aio.AioRpcError as err:
                failed_active_power += distribution[inverter_id]
                failed_batteries.add(battery_id)
                if err.code() == grpc.StatusCode.OUT_OF_RANGE:
                    _logger.debug(
                        "Set active_power for battery %d failed, error %s",
                        battery_id,
                        str(err),
                    )
                else:
                    _logger.warning(
                        "Set active_power for battery %d failed, error %s. Mark it as broken.",
                        battery_id,
                        str(err),
                    )
            except asyncio.exceptions.CancelledError:
                failed_active_power += distribution[inverter_id]
                failed_batteries.add(battery_id)
                _logger.warning(
                    "Battery %d didn't respond in %f sec. Mark it as broken.",
                    battery_id,
                    request_timeout_sec,
                )

        return failed_active_power, failed_batteries

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
        await self._all_battery_status.stop()
        await self._stop()  # type: ignore # pylint: disable=no-member
