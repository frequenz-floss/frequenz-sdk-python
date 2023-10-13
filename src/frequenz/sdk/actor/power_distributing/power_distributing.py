# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Actor to distribute power between batteries.

When charge/discharge method is called the power should be distributed so that
the SoC in batteries stays at the same level. That way of distribution
prevents using only one battery, increasing temperature, and maximize the total
amount power to charge/discharge.

Purpose of this actor is to keep SoC level of each component at the equal level.
"""


import asyncio
import logging
import time
from asyncio.tasks import ALL_COMPLETED
from collections import abc
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from math import isnan
from typing import Any, Self, TypeVar, cast

import grpc
from frequenz.channels import Peekable, Receiver, Sender

from frequenz.sdk.timeseries._quantities import Power

from ..._internal._math import is_close_to_zero
from ...actor._actor import Actor
from ...microgrid import ComponentGraph, connection_manager
from ...microgrid.client import MicrogridApiClient
from ...microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    InverterData,
)
from ._battery_pool_status import BatteryPoolStatus, BatteryStatus
from ._distribution_algorithm import (
    AggregatedBatteryData,
    DistributionAlgorithm,
    DistributionResult,
    InvBatPair,
)
from .request import Request
from .result import Error, OutOfBounds, PartialFailure, PowerBounds, Result, Success

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


def _get_all(source: dict[int, frozenset[int]], keys: abc.Set[int]) -> set[int]:
    """Get all values for the given keys from the given map.

    Args:
        source: map to get values from.
        keys: keys to get values for.

    Returns:
        Set of values for the given keys.
    """
    return set().union(*[source[key] for key in keys])


class PowerDistributingActor(Actor):
    # pylint: disable=too-many-instance-attributes
    """Actor to distribute the power between batteries in a microgrid.

    The purpose of this tool is to keep an equal SoC level in all batteries.
    The PowerDistributingActor can have many concurrent users which at this time
    need to be known at construction time.

    For each user a bidirectional channel needs to be created through which
    they can send and receive requests and responses.

    It is recommended to wait for PowerDistributingActor output with timeout. Otherwise if
    the processing function fails then the response will never come.
    The timeout should be Result:request_timeout + time for processing the request.

    Edge cases:
    * If there are 2 requests to be processed for the same subset of batteries, then
    only the latest request will be processed. Older request will be ignored. User with
    older request will get response with Result.Status.IGNORED.

    * If there are 2 requests and their subset of batteries is different but they
    overlap (they have at least one common battery), then then both batteries
    will be processed. However it is not expected so the proper error log will be
    printed.
    """

    def __init__(
        self,
        requests_receiver: Receiver[Request],
        results_sender: Sender[Result],
        battery_status_sender: Sender[BatteryStatus],
        wait_for_data_sec: float = 2,
        *,
        name: str | None = None,
    ) -> None:
        """Create class instance.

        Args:
            requests_receiver: Receiver for receiving power requests from the power manager.
            results_sender: Sender for sending results to the power manager.
            battery_status_sender: Channel for sending information which batteries are
                working.
            wait_for_data_sec: How long actor should wait before processing first
                request. It is a time needed to collect first components data.
            name: The name of the actor. If `None`, `str(id(self))` will be used. This
                is used mostly for debugging purposes.
        """
        super().__init__(name=name)
        self._requests_receiver = requests_receiver
        self._result_sender = results_sender
        self._wait_for_data_sec = wait_for_data_sec

        # NOTE: power_distributor_exponent should be received from ConfigManager
        self.power_distributor_exponent: float = 1.0
        """The exponent for the power distribution algorithm.

        The exponent determines how fast the batteries should strive to the
        equal SoC level.
        """

        self.distribution_algorithm = DistributionAlgorithm(
            self.power_distributor_exponent
        )
        """The distribution algorithm used to distribute power between batteries."""

        (
            self._bat_inv_map,
            self._inv_bat_map,
            self._bat_bats_map,
            self._inv_invs_map,
        ) = self._get_components_pairs(connection_manager.get().component_graph)

        self._battery_receivers: dict[int, Peekable[BatteryData]] = {}
        self._inverter_receivers: dict[int, Peekable[InverterData]] = {}

        self._all_battery_status = BatteryPoolStatus(
            battery_ids=set(self._bat_inv_map.keys()),
            battery_status_sender=battery_status_sender,
            max_blocking_duration_sec=30.0,
            max_data_age_sec=10.0,
        )

        self._cached_metrics: dict[frozenset[int], _CacheEntry | None] = {
            bat_ids: None for bat_ids in self._bat_bats_map.values()
        }

    def _get_bounds(
        self,
        pairs_data: list[InvBatPair],
    ) -> PowerBounds:
        """Get power bounds for given batteries.

        Args:
            pairs_data: list of battery and adjacent inverter data pairs.

        Returns:
            Power bounds for given batteries.
        """
        return PowerBounds(
            inclusion_lower=sum(
                max(
                    battery.power_inclusion_lower_bound,
                    sum(
                        inverter.active_power_inclusion_lower_bound
                        for inverter in inverters
                    ),
                )
                for battery, inverters in pairs_data
            ),
            inclusion_upper=sum(
                min(
                    battery.power_inclusion_upper_bound,
                    sum(
                        inverter.active_power_inclusion_upper_bound
                        for inverter in inverters
                    ),
                )
                for battery, inverters in pairs_data
            ),
            exclusion_lower=min(
                sum(battery.power_exclusion_lower_bound for battery, _ in pairs_data),
                sum(
                    inverter.active_power_exclusion_lower_bound
                    for _, inverters in pairs_data
                    for inverter in inverters
                ),
            ),
            exclusion_upper=max(
                sum(battery.power_exclusion_upper_bound for battery, _ in pairs_data),
                sum(
                    inverter.active_power_exclusion_upper_bound
                    for _, inverters in pairs_data
                    for inverter in inverters
                ),
            ),
        )

    async def _run(self) -> None:  # pylint: disable=too-many-locals
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

        async for request in self._requests_receiver:
            try:
                pairs_data: list[InvBatPair] = self._get_components_data(
                    request.batteries, request.include_broken_batteries
                )
            except KeyError as err:
                await self._result_sender.send(Error(request=request, msg=str(err)))
                continue

            if not pairs_data and not request.include_broken_batteries:
                error_msg = (
                    "No data for at least one of the given "
                    f"batteries {str(request.batteries)}"
                )
                await self._result_sender.send(
                    Error(request=request, msg=str(error_msg))
                )
                continue

            error = self._check_request(request, pairs_data)
            if error:
                await self._result_sender.send(error)
                continue

            try:
                distribution = self._get_power_distribution(request, pairs_data)
            except ValueError as err:
                _logger.exception("Couldn't distribute power")
                error_msg = f"Couldn't distribute power, error: {str(err)}"
                await self._result_sender.send(
                    Error(request=request, msg=str(error_msg))
                )
                continue

            distributed_power_value = (
                request.power.as_watts() - distribution.remaining_power
            )
            battery_distribution: dict[int, float] = {}
            for inverter_id, dist in distribution.distribution.items():
                for battery_id in self._inv_bat_map[inverter_id]:
                    battery_distribution[battery_id] = (
                        battery_distribution.get(battery_id, 0.0) + dist
                    )

            _logger.debug(
                "Distributing power %d between the batteries %s",
                distributed_power_value,
                str(battery_distribution),
            )

            failed_power, failed_batteries = await self._set_distributed_power(
                api, distribution, request.request_timeout
            )

            response: Success | PartialFailure
            if len(failed_batteries) > 0:
                succeed_batteries = set(battery_distribution.keys()) - failed_batteries
                response = PartialFailure(
                    request=request,
                    succeeded_power=Power.from_watts(distributed_power_value),
                    succeeded_batteries=succeed_batteries,
                    failed_power=Power.from_watts(failed_power),
                    failed_batteries=failed_batteries,
                    excess_power=Power.from_watts(distribution.remaining_power),
                )
            else:
                succeed_batteries = set(battery_distribution.keys())
                response = Success(
                    request=request,
                    succeeded_power=Power.from_watts(distributed_power_value),
                    succeeded_batteries=succeed_batteries,
                    excess_power=Power.from_watts(distribution.remaining_power),
                )

            asyncio.gather(
                *[
                    self._all_battery_status.update_status(
                        succeed_batteries, failed_batteries
                    ),
                    self._result_sender.send(response),
                ]
            )

    async def _set_distributed_power(
        self,
        api: MicrogridApiClient,
        distribution: DistributionResult,
        timeout: timedelta,
    ) -> tuple[float, set[int]]:
        """Send distributed power to the inverters.

        Args:
            api: Microgrid api client
            distribution: Distribution result
            timeout: How long wait for the response

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
            timeout=timeout.total_seconds(),
            return_when=ALL_COMPLETED,
        )

        await self._cancel_tasks(pending)

        return self._parse_result(tasks, distribution.distribution, timeout)

    def _get_power_distribution(
        self, request: Request, inv_bat_pairs: list[InvBatPair]
    ) -> DistributionResult:
        """Get power distribution result for the batteries in the request.

        Args:
            request: the power request to process.
            inv_bat_pairs: the battery and adjacent inverter data pairs.

        Returns:
            the power distribution result.
        """
        available_bat_ids = _get_all(
            self._bat_bats_map, {pair.battery.component_id for pair in inv_bat_pairs}
        )

        unavailable_bat_ids = request.batteries - available_bat_ids
        unavailable_inv_ids: set[int] = set()

        for inverter_ids in [
            self._bat_inv_map[battery_id_set] for battery_id_set in unavailable_bat_ids
        ]:
            unavailable_inv_ids = unavailable_inv_ids.union(inverter_ids)

        if request.include_broken_batteries and not available_bat_ids:
            return self.distribution_algorithm.distribute_power_equally(
                request.power.as_watts(), unavailable_inv_ids
            )

        result = self.distribution_algorithm.distribute_power(
            request.power.as_watts(), inv_bat_pairs
        )

        if request.include_broken_batteries and unavailable_inv_ids:
            additional_result = self.distribution_algorithm.distribute_power_equally(
                result.remaining_power, unavailable_inv_ids
            )

            for inv_id, power in additional_result.distribution.items():
                result.distribution[inv_id] = power
            result.remaining_power = 0.0

        return result

    def _check_request(
        self,
        request: Request,
        pairs_data: list[InvBatPair],
    ) -> Result | None:
        """Check whether the given request if correct.

        Args:
            request: request to check
            pairs_data: list of battery and adjacent inverter data pairs.

        Returns:
            Result for the user if the request is wrong, None otherwise.
        """
        if not request.batteries:
            return Error(request=request, msg="Empty battery IDs in the request")

        for battery in request.batteries:
            _logger.debug("Checking battery %d", battery)
            if battery not in self._battery_receivers:
                msg = (
                    f"No battery {battery}, available batteries: "
                    f"{list(self._battery_receivers.keys())}"
                )
                return Error(request=request, msg=msg)

        bounds = self._get_bounds(pairs_data)

        power = request.power.as_watts()

        # Zero power requests are always forwarded to the microgrid API, even if they
        # are outside the exclusion bounds.
        if is_close_to_zero(power):
            return None

        if request.adjust_power:
            # Automatic power adjustments can only bring down the requested power down
            # to the inclusion bounds.
            #
            # If the requested power is in the exclusion bounds, it is NOT possible to
            # increase it so that it is outside the exclusion bounds.
            if bounds.exclusion_lower < power < bounds.exclusion_upper:
                return OutOfBounds(request=request, bounds=bounds)
        else:
            in_lower_range = bounds.inclusion_lower <= power <= bounds.exclusion_lower
            in_upper_range = bounds.exclusion_upper <= power <= bounds.inclusion_upper
            if not (in_lower_range or in_upper_range):
                return OutOfBounds(request=request, bounds=bounds)

        return None

    def _get_components_pairs(
        self, component_graph: ComponentGraph
    ) -> tuple[
        dict[int, frozenset[int]],
        dict[int, frozenset[int]],
        dict[int, frozenset[int]],
        dict[int, frozenset[int]],
    ]:
        """Create maps between battery and adjacent inverters.

        Args:
            component_graph: component graph

        Returns:
            Tuple of four maps:
                battery to inverters,
                inverter to batteries,
                battery to batteries,
                inverter to inverters.
        """
        bat_inv_map: dict[int, set[int]] = {}
        inv_bat_map: dict[int, set[int]] = {}
        bat_bats_map: dict[int, set[int]] = {}
        inv_invs_map: dict[int, set[int]] = {}

        batteries: Iterable[Component] = component_graph.components(
            component_category={ComponentCategory.BATTERY}
        )

        for battery in batteries:
            inverters: set[int] = set(
                component.component_id
                for component in component_graph.predecessors(battery.component_id)
                if component.category == ComponentCategory.INVERTER
            )

            if len(inverters) == 0:
                _logger.error("No inverters for battery %d", battery.component_id)
                continue

            _logger.debug(
                "Battery %d has inverter %s", battery.component_id, list(inverters)
            )

            bat_inv_map[battery.component_id] = inverters
            bat_bats_map.setdefault(battery.component_id, set()).update(
                set(
                    component.component_id
                    for inverter in inverters
                    for component in component_graph.successors(inverter)
                )
            )

            for inverter in inverters:
                inv_bat_map.setdefault(inverter, set()).add(battery.component_id)
                inv_invs_map.setdefault(inverter, set()).update(bat_inv_map)

        # Convert sets to frozensets to make them hashable.
        return (
            {k: frozenset(v) for k, v in bat_inv_map.items()},
            {k: frozenset(v) for k, v in inv_bat_map.items()},
            {k: frozenset(v) for k, v in bat_bats_map.items()},
            {k: frozenset(v) for k, v in inv_invs_map.items()},
        )

    def _get_components_data(
        self, batteries: abc.Set[int], include_broken: bool
    ) -> list[InvBatPair]:
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
        pairs_data: list[InvBatPair] = []

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

        connected_inverters = _get_all(self._bat_inv_map, batteries)

        # Check to see if inverters are involved that are connected to batteries
        # that were not requested.
        batteries_from_inverters = _get_all(self._inv_bat_map, connected_inverters)

        if batteries_from_inverters != batteries:
            extra_batteries = batteries_from_inverters - batteries
            raise KeyError(
                f"Inverters {_get_all(self._bat_inv_map, extra_batteries)} "
                f"are connected to batteries that were not requested: {extra_batteries}"
            )

        # set of set of batteries one for each working_battery
        battery_sets: frozenset[frozenset[int]] = frozenset(
            self._bat_bats_map[working_battery] for working_battery in working_batteries
        )

        for battery_ids in battery_sets:
            inverter_ids: frozenset[int] = self._bat_inv_map[next(iter(battery_ids))]

            data = self._get_battery_inverter_data(battery_ids, inverter_ids)
            if not data and include_broken:
                cached_entry = self._cached_metrics[battery_ids]
                if cached_entry and not cached_entry.has_expired():
                    data = cached_entry.inv_bat_pair
                else:
                    data = None
            if data is None:
                _logger.warning(
                    "Skipping battery set %s because at least one of its messages isn't correct.",
                    list(battery_ids),
                )
                continue

            assert len(data.inverter) > 0
            pairs_data.append(data)
        return pairs_data

    def _get_battery_inverter_data(
        self, battery_ids: frozenset[int], inverter_ids: frozenset[int]
    ) -> InvBatPair | None:
        """Get battery and inverter data if they are correct.

        Each float data from the microgrid can be "NaN".
        We can't do math operations on "NaN".
        So check all the metrics and if any are "NaN" then return None.

        Args:
            battery_ids: battery ids
            inverter_ids: inverter ids

        Returns:
            Data for the battery and adjacent inverter without NaN values.
                Return None if we could not replace NaN values.
        """
        battery_data_none = [
            self._battery_receivers[battery_id].peek() for battery_id in battery_ids
        ]
        inverter_data_none = [
            self._inverter_receivers[inverter_id].peek() for inverter_id in inverter_ids
        ]

        # It means that nothing has been send on this channels, yet.
        # This should be handled by BatteryStatus. BatteryStatus should not return
        # this batteries as working.
        if not all(battery_data_none) or not all(inverter_data_none):
            _logger.error(
                "Battery %s or inverter %s send no data, yet. They should be not used.",
                battery_ids,
                inverter_ids,
            )
            return None

        battery_data = cast(list[BatteryData], battery_data_none)
        inverter_data = cast(list[InverterData], inverter_data_none)

        DataType = TypeVar("DataType", BatteryData, InverterData)

        def metric_is_nan(data: DataType, metrics: list[str]) -> bool:
            """Check if non-replaceable metrics are NaN."""
            assert data is not None
            return any(map(lambda metric: isnan(getattr(data, metric)), metrics))

        def nan_metric_in_list(data: list[DataType], metrics: list[str]) -> bool:
            """Check if any metric is NaN."""
            return any(map(lambda datum: metric_is_nan(datum, metrics), data))

        crucial_metrics_bat = [
            "soc",
            "soc_lower_bound",
            "soc_upper_bound",
            "capacity",
            "power_inclusion_lower_bound",
            "power_inclusion_upper_bound",
        ]

        crucial_metrics_inv = [
            "active_power_inclusion_lower_bound",
            "active_power_inclusion_upper_bound",
        ]

        if nan_metric_in_list(battery_data, crucial_metrics_bat):
            _logger.debug("Some metrics for battery set %s are NaN", list(battery_ids))
            return None

        if nan_metric_in_list(inverter_data, crucial_metrics_inv):
            _logger.debug(
                "Some metrics for inverter set %s are NaN", list(inverter_ids)
            )
            return None

        inv_bat_pair = InvBatPair(AggregatedBatteryData(battery_data), inverter_data)
        self._cached_metrics[battery_ids] = _CacheEntry.from_ttl(inv_bat_pair)
        return inv_bat_pair

    async def _create_channels(self) -> None:
        """Create channels to get data of components in microgrid."""
        api = connection_manager.get().api_client
        for battery_id, inverter_ids in self._bat_inv_map.items():
            bat_recv: Receiver[BatteryData] = await api.battery_data(battery_id)
            self._battery_receivers[battery_id] = bat_recv.into_peekable()

            for inverter_id in inverter_ids:
                inv_recv: Receiver[InverterData] = await api.inverter_data(inverter_id)
                self._inverter_receivers[inverter_id] = inv_recv.into_peekable()

    def _parse_result(
        self,
        tasks: dict[int, asyncio.Task[None]],
        distribution: dict[int, float],
        request_timeout: timedelta,
    ) -> tuple[float, set[int]]:
        """Parse the results of `set_power` requests.

        Check if any task has failed and determine the reason for failure.
        If any task did not succeed, then the corresponding battery is marked as broken.

        Args:
            tasks: A dictionary where the key is the inverter ID and the value is the task that
                set the power for this inverter. Each task should be finished or cancelled.
            distribution: A dictionary where the key is the inverter ID and the value is how much
                power was set to the corresponding inverter.
            request_timeout: The timeout that was used for the request.

        Returns:
            A tuple where the first element is the total failed power, and the second element is
            the set of batteries that failed.
        """
        failed_power: float = 0.0
        failed_batteries: set[int] = set()

        for inverter_id, aws in tasks.items():
            battery_ids = self._inv_bat_map[inverter_id]
            try:
                aws.result()
            except grpc.aio.AioRpcError as err:
                failed_power += distribution[inverter_id]
                failed_batteries = failed_batteries.union(battery_ids)
                if err.code() == grpc.StatusCode.OUT_OF_RANGE:
                    _logger.debug(
                        "Set power for battery %s failed, error %s",
                        battery_ids,
                        str(err),
                    )
                else:
                    _logger.warning(
                        "Set power for battery %s failed, error %s. Mark it as broken.",
                        battery_ids,
                        str(err),
                    )
            except asyncio.exceptions.CancelledError:
                failed_power += distribution[inverter_id]
                failed_batteries = failed_batteries.union(battery_ids)
                _logger.warning(
                    "Battery %s didn't respond in %f sec. Mark it as broken.",
                    battery_ids,
                    request_timeout.total_seconds(),
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

    async def stop(self, msg: str | None = None) -> None:
        """Stop this actor.

        Args:
            msg: The message to be passed to the tasks being cancelled.
        """
        await self._all_battery_status.stop()
        await super().stop(msg)
