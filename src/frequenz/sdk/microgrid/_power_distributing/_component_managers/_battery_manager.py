# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Manage batteries and inverters for the power distributor."""

import asyncio
import collections.abc
import logging
import math
import typing
from datetime import timedelta

from frequenz.channels import LatestValueCache, Receiver, Sender
from frequenz.client.microgrid import (
    ApiClientError,
    BatteryData,
    ComponentCategory,
    InverterData,
    OperationOutOfRange,
)
from frequenz.quantities import Power
from typing_extensions import override

from ...._internal._math import is_close_to_zero
from ... import connection_manager
from .._component_pool_status_tracker import ComponentPoolStatusTracker
from .._component_status import BatteryStatusTracker, ComponentPoolStatus
from .._distribution_algorithm import (
    AggregatedBatteryData,
    BatteryDistributionAlgorithm,
    DistributionResult,
    InvBatPair,
)
from ..request import Request
from ..result import Error, OutOfBounds, PartialFailure, PowerBounds, Result, Success
from ._component_manager import ComponentManager

_logger = logging.getLogger(__name__)


def _get_all_from_map(
    source: dict[int, frozenset[int]], keys: collections.abc.Set[int]
) -> set[int]:
    """Get all values for the given keys from the given map.

    Args:
        source: map to get values from.
        keys: keys to get values for.

    Returns:
        Set of values for the given keys.
    """
    return set().union(*[source[key] for key in keys])


def _get_battery_inverter_mappings(
    battery_ids: collections.abc.Set[int],
    *,  # force keyword arguments
    inv_bats: bool = True,
    bat_bats: bool = True,
    inv_invs: bool = True,
) -> dict[str, dict[int, frozenset[int]]]:
    """Create maps between battery and adjacent inverters.

    Args:
        battery_ids: set of battery ids
        inv_bats: whether to create the inverter to batteries map
        bat_bats: whether to create the battery to batteries map
        inv_invs: whether to create the inverter to inverters map

    Returns:
        a dict of the requested maps, using the following keys:
            * "bat_invs": battery to inverters map
            * "inv_bats": inverter to batteries map
            * "bat_bats": battery to batteries map
            * "inv_invs": inverter to inverters map
    """
    bat_invs_map: dict[int, set[int]] = {}
    inv_bats_map: dict[int, set[int]] | None = {} if inv_bats else None
    bat_bats_map: dict[int, set[int]] | None = {} if bat_bats else None
    inv_invs_map: dict[int, set[int]] | None = {} if inv_invs else None
    component_graph = connection_manager.get().component_graph

    for battery_id in battery_ids:
        inverters: set[int] = set(
            component.component_id
            for component in component_graph.predecessors(battery_id)
            if component.category == ComponentCategory.INVERTER
        )

        if len(inverters) == 0:
            _logger.error("No inverters for battery %d", battery_id)
            continue

        bat_invs_map[battery_id] = inverters
        if bat_bats_map is not None:
            bat_bats_map.setdefault(battery_id, set()).update(
                set(
                    component.component_id
                    for inverter in inverters
                    for component in component_graph.successors(inverter)
                )
            )

        for inverter in inverters:
            if inv_bats_map is not None:
                inv_bats_map.setdefault(inverter, set()).add(battery_id)
            if inv_invs_map is not None:
                inv_invs_map.setdefault(inverter, set()).update(bat_invs_map)

    mapping: dict[str, dict[int, frozenset[int]]] = {}

    # Convert sets to frozensets to make them hashable.
    def _add(key: str, value: dict[int, set[int]] | None) -> None:
        if value is not None:
            mapping[key] = {k: frozenset(v) for k, v in value.items()}

    _add("bat_invs", bat_invs_map)
    _add("inv_bats", inv_bats_map)
    _add("bat_bats", bat_bats_map)
    _add("inv_invs", inv_invs_map)

    return mapping


class BatteryManager(ComponentManager):  # pylint: disable=too-many-instance-attributes
    """Class to manage the data streams for batteries."""

    @override
    def __init__(
        self,
        component_pool_status_sender: Sender[ComponentPoolStatus],
        results_sender: Sender[Result],
        api_power_request_timeout: timedelta,
    ):
        """Initialize this instance.

        Args:
            component_pool_status_sender: Channel sender to send the status of the
                battery pool to.  This status is used by the battery pool metric
                streams, to dynamically adjust the values based on the health of the
                individual batteries.
            results_sender: Channel sender to send the power distribution results to.
            api_power_request_timeout: Timeout to use when making power requests to
                the microgrid API.
        """
        self._results_sender = results_sender
        self._api_power_request_timeout = api_power_request_timeout
        self._batteries = connection_manager.get().component_graph.components(
            component_categories={ComponentCategory.BATTERY}
        )
        self._battery_ids = {battery.component_id for battery in self._batteries}

        maps = _get_battery_inverter_mappings(self._battery_ids)

        self._bat_invs_map = maps["bat_invs"]
        self._inv_bats_map = maps["inv_bats"]
        self._bat_bats_map = maps["bat_bats"]
        self._inv_invs_map = maps["inv_invs"]

        self._battery_caches: dict[int, LatestValueCache[BatteryData]] = {}
        self._inverter_caches: dict[int, LatestValueCache[InverterData]] = {}

        self._component_pool_status_tracker = ComponentPoolStatusTracker(
            component_ids=set(self._battery_ids),
            component_status_sender=component_pool_status_sender,
            max_blocking_duration=timedelta(seconds=30.0),
            max_data_age=timedelta(seconds=10.0),
            component_status_tracker_type=BatteryStatusTracker,
        )

        # NOTE: power_distributor_exponent should be received from ConfigManager
        self._power_distributor_exponent: float = 1.0
        """The exponent for the power distribution algorithm.

        The exponent determines how fast the batteries should strive to the
        equal SoC level.
        """

        self._distribution_algorithm = BatteryDistributionAlgorithm(
            self._power_distributor_exponent
        )
        """The distribution algorithm used to distribute power between batteries."""

    @override
    def component_ids(self) -> collections.abc.Set[int]:
        """Return the set of component ids."""
        return self._battery_ids

    @override
    async def start(self) -> None:
        """Start the battery data manager."""
        await self._create_channels()

    @override
    async def stop(self) -> None:
        """Stop the battery data manager."""
        for bat_cache in self._battery_caches.values():
            await bat_cache.stop()
        for inv_cache in self._inverter_caches.values():
            await inv_cache.stop()
        await self._component_pool_status_tracker.stop()

    @override
    async def distribute_power(self, request: Request) -> None:
        """Distribute the requested power to the components.

        Args:
            request: Request to get the distribution for.
        """
        distribution_result = await self._get_distribution(request)
        if not isinstance(distribution_result, DistributionResult):
            result = distribution_result
        else:
            result = await self._distribute_power(request, distribution_result)
        await self._results_sender.send(result)

    async def _get_distribution(self, request: Request) -> DistributionResult | Result:
        """Get the distribution of the batteries.

        Args:
            request: Request to get the distribution for.

        Returns:
            Distribution of the batteries.
        """
        try:
            pairs_data: list[InvBatPair] = self._get_components_data(
                request.component_ids
            )
        except KeyError as err:
            return Error(request=request, msg=str(err))

        if not pairs_data:
            error_msg = (
                "No data for at least one of the given "
                f"batteries {str(request.component_ids)}"
            )
            return Error(request=request, msg=str(error_msg))

        error = self._check_request(request, pairs_data)
        if error:
            return error

        try:
            distribution = self._get_power_distribution(request, pairs_data)
        except ValueError as err:
            _logger.exception("Couldn't distribute power")
            error_msg = f"Couldn't distribute power, error: {str(err)}"
            return Error(request=request, msg=str(error_msg))

        return distribution

    async def _distribute_power(
        self, request: Request, distribution: DistributionResult
    ) -> Result:
        """Set the distributed power to the batteries.

        Args:
            request: Request to set the power for.
            distribution: Distribution to set.

        Returns:
            Result from the microgrid API.
        """
        distributed_power_value = (
            request.power.as_watts() - distribution.remaining_power
        )
        battery_distribution: dict[int, float] = {}
        for inverter_id, dist in distribution.distribution.items():
            for battery_id in self._inv_bats_map[inverter_id]:
                battery_distribution[battery_id] = (
                    battery_distribution.get(battery_id, 0.0) + dist
                )
        _logger.debug(
            "Distributing power %d between the batteries %s",
            distributed_power_value,
            str(battery_distribution),
        )

        failed_power, failed_batteries = await self._set_distributed_power(
            distribution, self._api_power_request_timeout
        )

        response: Success | PartialFailure
        if len(failed_batteries) > 0:
            succeed_batteries = set(battery_distribution.keys()) - failed_batteries
            response = PartialFailure(
                request=request,
                succeeded_power=Power.from_watts(
                    distributed_power_value - failed_power
                ),
                succeeded_components=succeed_batteries,
                failed_power=Power.from_watts(failed_power),
                failed_components=failed_batteries,
                excess_power=Power.from_watts(distribution.remaining_power),
            )
        else:
            succeed_batteries = set(battery_distribution.keys())
            response = Success(
                request=request,
                succeeded_power=Power.from_watts(distributed_power_value),
                succeeded_components=succeed_batteries,
                excess_power=Power.from_watts(distribution.remaining_power),
            )

        await asyncio.gather(
            *[
                self._component_pool_status_tracker.update_status(
                    succeed_batteries, failed_batteries
                ),
            ]
        )

        return response

    async def _create_channels(self) -> None:
        """Create channels to get data of components in microgrid."""
        api = connection_manager.get().api_client
        manager_id = f"{type(self).__name__}«{hex(id(self))}»"
        for battery_id, inverter_ids in self._bat_invs_map.items():
            bat_recv: Receiver[BatteryData] = await api.battery_data(battery_id)
            self._battery_caches[battery_id] = LatestValueCache(
                bat_recv,
                unique_id=f"{manager_id}:battery«{battery_id}»",
            )

            for inverter_id in inverter_ids:
                inv_recv: Receiver[InverterData] = await api.inverter_data(inverter_id)
                self._inverter_caches[inverter_id] = LatestValueCache(
                    inv_recv, unique_id=f"{manager_id}:inverter«{inverter_id}»"
                )

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
                    battery.power_bounds.inclusion_lower,
                    sum(
                        inverter.active_power_inclusion_lower_bound
                        for inverter in inverters
                    ),
                )
                for battery, inverters in pairs_data
            ),
            inclusion_upper=sum(
                min(
                    battery.power_bounds.inclusion_upper,
                    sum(
                        inverter.active_power_inclusion_upper_bound
                        for inverter in inverters
                    ),
                )
                for battery, inverters in pairs_data
            ),
            exclusion_lower=min(
                sum(battery.power_bounds.exclusion_lower for battery, _ in pairs_data),
                sum(
                    inverter.active_power_exclusion_lower_bound
                    for _, inverters in pairs_data
                    for inverter in inverters
                ),
            ),
            exclusion_upper=max(
                sum(battery.power_bounds.exclusion_upper for battery, _ in pairs_data),
                sum(
                    inverter.active_power_exclusion_upper_bound
                    for _, inverters in pairs_data
                    for inverter in inverters
                ),
            ),
        )

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
        if not request.component_ids:
            return Error(request=request, msg="Empty battery IDs in the request")

        for battery in request.component_ids:
            _logger.debug("Checking battery %d", battery)
            if battery not in self._battery_caches:
                msg = (
                    f"No battery {battery}, available batteries: "
                    f"{list(self._battery_caches.keys())}"
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
        # It means that nothing has been send on these channels, yet.
        # This should be handled by BatteryStatus. BatteryStatus should not return
        # this batteries as working.
        if not all(
            self._battery_caches[bat_id].has_value() for bat_id in battery_ids
        ) or not all(
            self._inverter_caches[inv_id].has_value() for inv_id in inverter_ids
        ):
            _logger.error(
                "Battery %s or inverter %s send no data, yet. They should be not used.",
                battery_ids,
                inverter_ids,
            )
            return None

        battery_data = [
            self._battery_caches[battery_id].get() for battery_id in battery_ids
        ]
        inverter_data = [
            self._inverter_caches[inverter_id].get() for inverter_id in inverter_ids
        ]

        DataType = typing.TypeVar("DataType", BatteryData, InverterData)

        def metric_is_nan(data: DataType, metrics: list[str]) -> bool:
            """Check if non-replaceable metrics are NaN."""
            assert data is not None
            return any(map(lambda metric: math.isnan(getattr(data, metric)), metrics))

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

        return InvBatPair(AggregatedBatteryData(battery_data), inverter_data)

    def _get_components_data(
        self, batteries: collections.abc.Set[int]
    ) -> list[InvBatPair]:
        """Get data for the given batteries and adjacent inverters.

        Args:
            batteries: Batteries that needs data.

        Raises:
            KeyError: If any battery in the given list doesn't exists in microgrid.

        Returns:
            Pairs of battery and adjacent inverter data.
        """
        pairs_data: list[InvBatPair] = []

        working_batteries = self._component_pool_status_tracker.get_working_components(
            batteries
        )

        for battery_id in working_batteries:
            if battery_id not in self._battery_caches:
                raise KeyError(
                    f"No battery {battery_id}, "
                    f"available batteries: {list(self._battery_caches.keys())}"
                )

        connected_inverters = _get_all_from_map(self._bat_invs_map, batteries)

        # Check to see if inverters are involved that are connected to batteries
        # that were not requested.
        batteries_from_inverters = _get_all_from_map(
            self._inv_bats_map, connected_inverters
        )

        if batteries_from_inverters != batteries:
            extra_batteries = batteries_from_inverters - batteries
            raise KeyError(
                f"Inverters {_get_all_from_map(self._bat_invs_map, extra_batteries)} "
                f"are connected to batteries that were not requested: {extra_batteries}"
            )

        # set of set of batteries one for each working_battery
        battery_sets: frozenset[frozenset[int]] = frozenset(
            self._bat_bats_map[working_battery] for working_battery in working_batteries
        )

        for battery_ids in battery_sets:
            inverter_ids: frozenset[int] = self._bat_invs_map[next(iter(battery_ids))]

            data = self._get_battery_inverter_data(battery_ids, inverter_ids)
            if data is None:
                _logger.warning(
                    "Skipping battery set %s because at least one of its messages isn't correct.",
                    list(battery_ids),
                )
                continue

            assert len(data.inverter) > 0
            pairs_data.append(data)
        return pairs_data

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
        available_bat_ids = _get_all_from_map(
            self._bat_bats_map, {pair.battery.component_id for pair in inv_bat_pairs}
        )

        unavailable_bat_ids = request.component_ids - available_bat_ids
        unavailable_inv_ids: set[int] = set()

        for inverter_ids in [
            self._bat_invs_map[battery_id_set] for battery_id_set in unavailable_bat_ids
        ]:
            unavailable_inv_ids = unavailable_inv_ids.union(inverter_ids)

        result = self._distribution_algorithm.distribute_power(
            request.power.as_watts(), inv_bat_pairs
        )

        return result

    async def _set_distributed_power(
        self,
        distribution: DistributionResult,
        timeout: timedelta,
    ) -> tuple[float, set[int]]:
        """Send distributed power to the inverters.

        Args:
            distribution: Distribution result
            timeout: How long wait for the response

        Returns:
            Tuple where first element is total failed power, and the second element
            set of batteries that failed.
        """
        api = connection_manager.get().api_client

        tasks = {
            inverter_id: asyncio.create_task(api.set_power(inverter_id, power))
            for inverter_id, power in distribution.distribution.items()
        }

        _, pending = await asyncio.wait(
            tasks.values(),
            timeout=timeout.total_seconds(),
            return_when=asyncio.ALL_COMPLETED,
        )

        await self._cancel_tasks(pending)

        return self._parse_result(tasks, distribution.distribution, timeout)

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
            battery_ids = self._inv_bats_map[inverter_id]
            failed = True
            try:
                aws.result()
                failed = False
            except OperationOutOfRange as err:
                _logger.debug(
                    "Set power for battery %s failed due to out of range error: %s",
                    battery_ids,
                    err,
                )
            except ApiClientError as err:
                _logger.warning(
                    "Set power for battery %s failed, mark it as broken. Error: %s",
                    battery_ids,
                    err,
                )
            except asyncio.exceptions.CancelledError:
                _logger.warning(
                    "Battery %s didn't respond in %f sec. Mark it as broken.",
                    battery_ids,
                    request_timeout.total_seconds(),
                )
            except Exception:  # pylint: disable=broad-except
                _logger.exception(
                    "Unknown error while setting power to batteries: %s",
                    battery_ids,
                )

            if failed:
                failed_power += distribution[inverter_id]
                failed_batteries.update(battery_ids)

        return failed_power, failed_batteries

    async def _cancel_tasks(
        self, tasks: collections.abc.Iterable[asyncio.Task[typing.Any]]
    ) -> None:
        """Cancel given asyncio tasks and wait for them.

        Args:
            tasks: tasks to cancel.
        """
        for aws in tasks:
            aws.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
