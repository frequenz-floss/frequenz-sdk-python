# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Module that defines how to aggregate metrics from battery-inverter components."""


import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Mapping, Set
from datetime import datetime, timezone
from typing import Generic, TypeVar

from frequenz.client.microgrid import ComponentMetricId
from frequenz.quantities import Energy, Percentage, Power, Temperature

from ... import timeseries
from ..._internal import _math
from ...microgrid._power_distributing._component_managers._battery_manager import (
    _get_battery_inverter_mappings,
)
from ...microgrid._power_distributing._distribution_algorithm._battery_distribution_algorithm import (  # noqa: E501 # pylint: disable=line-too-long
    _aggregate_battery_power_bounds,
)
from ...microgrid._power_distributing.result import PowerBounds
from .._base_types import Sample, SystemBounds
from ._component_metrics import ComponentMetricsData

_logger = logging.getLogger(__name__)

_MIN_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc)
"""Minimal timestamp that can be used in the formula."""


# Formula output types class have no common interface
# Print all possible types here.
T = TypeVar("T", Sample[Percentage], Sample[Energy], SystemBounds, Sample[Temperature])
"""Type variable of the formula output."""


class MetricCalculator(ABC, Generic[T]):
    """Define how to calculate high level metrics from many components data.

    It specifies:
        * what components and metrics its needs to calculate the results,
        * how to calculate the result,
    """

    def __init__(self, batteries: Set[int]) -> None:
        """Create class instance.

        Args:
            batteries: From what batteries the data should be aggregated.
        """
        self._batteries = batteries

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        """Return name of the formula.

        Returns:
            Name of the formula
        """

    @property
    def batteries(self) -> Set[int]:
        """Return set of batteries that should be used to calculate the metrics.

        Some batteries given in constructor can be discarded
        because of the failing preconditions. This method returns set of
        batteries that can be used in the calculator.

        Returns:
            Set of batteries that should be used.
        """
        return self._batteries

    @property
    @abstractmethod
    def battery_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each battery.

        Returns:
            Map between battery id and set of required metrics id.
        """

    @property
    @abstractmethod
    def inverter_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each inverter.

        Returns:
            Map between inverter id and set of required metrics id.
        """

    @abstractmethod
    def calculate(
        self,
        metrics_data: dict[int, ComponentMetricsData],
        working_batteries: set[int],
    ) -> T:
        """Aggregate the metrics_data and calculate high level metric.

        Missing components will be ignored. Formula will be calculated for all
        working batteries that are in metrics_data.

        Args:
            metrics_data: Components metrics data, that should be used to calculate the
                result.
            working_batteries: working batteries. These batteries will be used
                to calculate the result. It should be subset of the batteries given in a
                constructor.

        Returns:
            High level metric calculated from the given metrics.
            Return None if there are no component metrics.
        """


class CapacityCalculator(MetricCalculator[Sample[Energy]]):
    """Define how to calculate Capacity metrics."""

    def __init__(self, batteries: Set[int]) -> None:
        """Create class instance.

        Args:
            batteries: What batteries should be used for calculation.
        """
        super().__init__(batteries)

        self._metrics = [
            ComponentMetricId.CAPACITY,
            ComponentMetricId.SOC_LOWER_BOUND,
            ComponentMetricId.SOC_UPPER_BOUND,
        ]

    @classmethod
    def name(cls) -> str:
        """Return name of the calculator.

        Returns:
            Name of the calculator
        """
        return "Capacity"

    @property
    def battery_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each battery.

        Returns:
            Map between battery id and set of required metrics id.
        """
        return {bid: self._metrics for bid in self._batteries}

    @property
    def inverter_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each inverter.

        Returns:
            Map between inverter id and set of required metrics id.
        """
        return {}

    def calculate(
        self,
        metrics_data: dict[int, ComponentMetricsData],
        working_batteries: set[int],
    ) -> Sample[Energy]:
        """Aggregate the metrics_data and calculate high level metric.

        Missing components will be ignored. Formula will be calculated for all
        working batteries that are in metrics_data.

        Args:
            metrics_data: Components metrics data, that should be used to calculate the
                result.
            working_batteries: working batteries. These batteries will be used
                to calculate the result. It should be subset of the batteries given in a
                constructor.

        Returns:
            High level metric calculated from the given metrics.
            Return None if there are no component metrics.
        """
        timestamp = _MIN_TIMESTAMP
        total_capacity = 0.0

        for battery_id in working_batteries:
            if battery_id not in metrics_data:
                continue

            metrics = metrics_data[battery_id]

            capacity = metrics.get(ComponentMetricId.CAPACITY)
            soc_upper_bound = metrics.get(ComponentMetricId.SOC_UPPER_BOUND)
            soc_lower_bound = metrics.get(ComponentMetricId.SOC_LOWER_BOUND)

            # All metrics are related so if any is missing then we skip the component.
            if capacity is None or soc_lower_bound is None or soc_upper_bound is None:
                continue
            usable_capacity = capacity * (soc_upper_bound - soc_lower_bound) / 100
            timestamp = max(timestamp, metrics.timestamp)
            total_capacity += usable_capacity

        return (
            Sample(datetime.now(tz=timezone.utc), None)
            if timestamp == _MIN_TIMESTAMP
            else Sample[Energy](timestamp, Energy.from_watt_hours(total_capacity))
        )


class TemperatureCalculator(MetricCalculator[Sample[Temperature]]):
    """Define how to calculate temperature metrics."""

    def __init__(self, batteries: Set[int]) -> None:
        """Create class instance.

        Args:
            batteries: What batteries should be used for calculation.
        """
        super().__init__(batteries)

        self._metrics = [
            ComponentMetricId.TEMPERATURE,
        ]

    @classmethod
    def name(cls) -> str:
        """Return name of the calculator.

        Returns:
            Name of the calculator
        """
        return "temperature"

    @property
    def battery_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each battery.

        Returns:
            Map between battery id and set of required metrics id.
        """
        return {bid: self._metrics for bid in self._batteries}

    @property
    def inverter_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each inverter.

        Returns:
            Map between inverter id and set of required metrics id.
        """
        return {}

    def calculate(
        self,
        metrics_data: dict[int, ComponentMetricsData],
        working_batteries: set[int],
    ) -> Sample[Temperature]:
        """Aggregate the metrics_data and calculate high level metric for temperature.

        Missing components will be ignored. Formula will be calculated for all
        working batteries that are in metrics_data.

        Args:
            metrics_data: Components metrics data, that should be used to calculate the
                result.
            working_batteries: working batteries. These batteries will be used
                to calculate the result. It should be subset of the batteries given in a
                constructor.

        Returns:
            High level metric calculated from the given metrics.
            Return None if there are no component metrics.
        """
        timestamp = _MIN_TIMESTAMP
        temperature_sum: float = 0.0
        temperature_count: int = 0
        for battery_id in working_batteries:
            if battery_id not in metrics_data:
                continue
            metrics = metrics_data[battery_id]
            temperature = metrics.get(ComponentMetricId.TEMPERATURE)
            if temperature is None:
                continue
            timestamp = max(timestamp, metrics.timestamp)
            temperature_sum += temperature
            temperature_count += 1
        if timestamp == _MIN_TIMESTAMP:
            return Sample(datetime.now(tz=timezone.utc), None)

        temperature_avg = temperature_sum / temperature_count

        return Sample[Temperature](
            timestamp=timestamp,
            value=Temperature.from_celsius(value=temperature_avg),
        )


class SoCCalculator(MetricCalculator[Sample[Percentage]]):
    """Define how to calculate SoC metrics."""

    def __init__(self, batteries: Set[int]) -> None:
        """Create class instance.

        Args:
            batteries: What batteries should be used for calculation.
        """
        super().__init__(batteries)

        self._metrics = [
            ComponentMetricId.CAPACITY,
            ComponentMetricId.SOC_LOWER_BOUND,
            ComponentMetricId.SOC_UPPER_BOUND,
            ComponentMetricId.SOC,
        ]

    @classmethod
    def name(cls) -> str:
        """Return name of the calculator.

        Returns:
            Name of the calculator
        """
        return "SoC"

    @property
    def battery_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each battery.

        Returns:
            Map between battery id and set of required metrics id.
        """
        return {bid: self._metrics for bid in self._batteries}

    @property
    def inverter_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each inverter.

        Returns:
            Map between inverter id and set of required metrics id.
        """
        return {}

    def calculate(
        self,
        metrics_data: dict[int, ComponentMetricsData],
        working_batteries: set[int],
    ) -> Sample[Percentage]:
        """Aggregate the metrics_data and calculate high level metric.

        Missing components will be ignored. Formula will be calculated for all
        working batteries that are in metrics_data.

        Args:
            metrics_data: Components metrics data, that should be used to calculate the
                result.
            working_batteries: working batteries. These batteries will be used
                to calculate the result. It should be subset of the batteries given in a
                constructor.

        Returns:
            High level metric calculated from the given metrics.
            Return None if there are no component metrics.
        """
        timestamp = _MIN_TIMESTAMP
        usable_capacity_x100: float = 0.0
        used_capacity_x100: float = 0.0
        total_capacity_x100: float = 0.0

        for battery_id in working_batteries:
            if battery_id not in metrics_data:
                continue

            metrics = metrics_data[battery_id]

            capacity = metrics.get(ComponentMetricId.CAPACITY)
            soc_upper_bound = metrics.get(ComponentMetricId.SOC_UPPER_BOUND)
            soc_lower_bound = metrics.get(ComponentMetricId.SOC_LOWER_BOUND)
            soc = metrics.get(ComponentMetricId.SOC)

            # All metrics are related so if any is missing then we skip the component.
            if (
                capacity is None
                or soc_lower_bound is None
                or soc_upper_bound is None
                or soc is None
            ):
                continue

            # The SoC bounds are in the 0-100 range, so to get the actual usable
            # capacity, we need to divide by 100.
            #
            # We only want to calculate the SoC, and the usable capacity calculation is
            # just an intermediate step, so don't have to divide by 100 here, because it
            # gets cancelled out later.
            #
            # Therefore, the variables are named with a `_x100` suffix.
            usable_capacity_x100 = capacity * (soc_upper_bound - soc_lower_bound)
            if math.isclose(soc_upper_bound, soc_lower_bound):
                if soc < soc_lower_bound:
                    soc_scaled = 0.0
                else:
                    soc_scaled = 100.0
            else:
                soc_scaled = (
                    (soc - soc_lower_bound)
                    / (soc_upper_bound - soc_lower_bound)
                    * 100.0
                )
            # we are clamping here because the SoC might be out of bounds
            soc_scaled = min(max(soc_scaled, 0.0), 100.0)
            timestamp = max(timestamp, metrics.timestamp)
            used_capacity_x100 += usable_capacity_x100 * soc_scaled
            total_capacity_x100 += usable_capacity_x100

        if timestamp == _MIN_TIMESTAMP:
            return Sample(datetime.now(tz=timezone.utc), None)

        # When the calculated is close to 0.0 or 100.0, they are set to exactly 0.0 or
        # 100.0, to make full/empty checks using the == operator less error prone.
        pct = 0.0
        # To avoid zero division error
        if not _math.is_close_to_zero(total_capacity_x100):
            pct = used_capacity_x100 / total_capacity_x100
            if math.isclose(pct, 100.0):
                pct = 100.0

        return Sample(
            timestamp=timestamp,
            value=Percentage.from_percent(pct),
        )


class PowerBoundsCalculator(MetricCalculator[SystemBounds]):
    """Define how to calculate PowerBounds metrics."""

    def __init__(
        self,
        batteries: Set[int],
    ) -> None:
        """Create class instance.

        Args:
            batteries: What batteries should be used for calculation.
        """
        mappings: dict[str, dict[int, frozenset[int]]] = _get_battery_inverter_mappings(
            batteries, inv_bats=False, bat_bats=True, inv_invs=False
        )

        self._bat_inv_map = mappings["bat_invs"]
        self._bat_bats_map = mappings["bat_bats"]

        used_batteries = set(self._bat_inv_map.keys())

        if len(self._bat_inv_map) == 0:
            _logger.warning(
                "No battery in pool has adjacent inverter. Can't calculate %s.",
                PowerBoundsCalculator.name,
            )
        elif len(batteries) != len(self._bat_inv_map):
            _logger.warning(
                "Not all batteries in pool have adjacent inverter."
                "Use batteries %s for formula %s.",
                used_batteries,
                PowerBoundsCalculator.name,
            )

        super().__init__(used_batteries)
        self._battery_metrics = [
            ComponentMetricId.POWER_INCLUSION_LOWER_BOUND,
            ComponentMetricId.POWER_EXCLUSION_LOWER_BOUND,
            ComponentMetricId.POWER_EXCLUSION_UPPER_BOUND,
            ComponentMetricId.POWER_INCLUSION_UPPER_BOUND,
        ]

        self._inverter_metrics = [
            ComponentMetricId.ACTIVE_POWER_INCLUSION_LOWER_BOUND,
            ComponentMetricId.ACTIVE_POWER_EXCLUSION_LOWER_BOUND,
            ComponentMetricId.ACTIVE_POWER_EXCLUSION_UPPER_BOUND,
            ComponentMetricId.ACTIVE_POWER_INCLUSION_UPPER_BOUND,
        ]

    @classmethod
    def name(cls) -> str:
        """Return name of the calculator.

        Returns:
            Name of the calculator
        """
        return "PowerBounds"

    @property
    def battery_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each battery.

        Returns:
            Map between battery id and set of required metrics id.
        """
        return {bid: self._battery_metrics for bid in set(self._bat_inv_map.keys())}

    @property
    def inverter_metrics(self) -> Mapping[int, list[ComponentMetricId]]:
        """Return what metrics are needed for each inverter.

        Returns:
            Map between inverter id and set of required metrics id.
        """
        return {
            inverter_id: self._inverter_metrics
            for inverters in set(self._bat_inv_map.values())
            for inverter_id in inverters
        }

    # pylint: disable=too-many-locals
    def calculate(
        self,
        metrics_data: dict[int, ComponentMetricsData],
        working_batteries: set[int],
    ) -> SystemBounds:
        """Aggregate the metrics_data and calculate high level metric.

        Missing components will be ignored. Formula will be calculated for all
        working batteries that are in metrics_data.

        Args:
            metrics_data: Components metrics data.
            working_batteries: Set of working batteries. These batteries will be used
                to calculate the result.

        Returns:
            High level metric calculated from the given metrics.
            Return None if there are no component metrics.
        """
        timestamp = _MIN_TIMESTAMP
        loop_timestamp = _MIN_TIMESTAMP
        inclusion_bounds_lower = 0.0
        inclusion_bounds_upper = 0.0
        exclusion_bounds_lower = 0.0
        exclusion_bounds_upper = 0.0

        battery_sets = {
            self._bat_bats_map[battery_id] for battery_id in working_batteries
        }

        def get_validated_bounds(
            comp_id: int, comp_metric_ids: list[ComponentMetricId]
        ) -> PowerBounds | None:
            results: list[float] = []
            # Make timestamp accessible
            nonlocal loop_timestamp
            local_timestamp = loop_timestamp

            if data := metrics_data.get(comp_id):
                for comp_metric_id in comp_metric_ids:
                    val = data.get(comp_metric_id)
                    if val is not None:
                        local_timestamp = max(loop_timestamp, data.timestamp)
                        results.append(val)

            if len(results) != len(comp_metric_ids):
                return None

            loop_timestamp = local_timestamp
            return PowerBounds(
                inclusion_lower=results[0],
                exclusion_lower=results[1],
                exclusion_upper=results[2],
                inclusion_upper=results[3],
            )

        def get_bounds_list(
            comp_ids: frozenset[int], comp_metric_ids: list[ComponentMetricId]
        ) -> list[PowerBounds]:
            return list(
                x
                for x in map(
                    lambda comp_id: get_validated_bounds(comp_id, comp_metric_ids),
                    comp_ids,
                )
                if x is not None
            )

        for battery_ids in battery_sets:
            loop_timestamp = timestamp

            inverter_ids = self._bat_inv_map[next(iter(battery_ids))]

            battery_bounds = get_bounds_list(battery_ids, self._battery_metrics)

            if len(battery_bounds) == 0:
                continue

            aggregated_bat_bounds = _aggregate_battery_power_bounds(battery_bounds)

            inverter_bounds = get_bounds_list(inverter_ids, self._inverter_metrics)

            if len(inverter_bounds) == 0:
                continue

            timestamp = max(timestamp, loop_timestamp)

            inclusion_bounds_lower += max(
                aggregated_bat_bounds.inclusion_lower,
                sum(bound.inclusion_lower for bound in inverter_bounds),
            )
            inclusion_bounds_upper += min(
                aggregated_bat_bounds.inclusion_upper,
                sum(bound.inclusion_upper for bound in inverter_bounds),
            )
            exclusion_bounds_lower += min(
                aggregated_bat_bounds.exclusion_lower,
                sum(bound.exclusion_lower for bound in inverter_bounds),
            )
            exclusion_bounds_upper += max(
                aggregated_bat_bounds.exclusion_upper,
                sum(bound.exclusion_upper for bound in inverter_bounds),
            )

        if timestamp == _MIN_TIMESTAMP:
            return SystemBounds(
                timestamp=datetime.now(tz=timezone.utc),
                inclusion_bounds=None,
                exclusion_bounds=None,
            )

        return SystemBounds(
            timestamp=timestamp,
            inclusion_bounds=timeseries.Bounds(
                Power.from_watts(inclusion_bounds_lower),
                Power.from_watts(inclusion_bounds_upper),
            ),
            exclusion_bounds=timeseries.Bounds(
                Power.from_watts(exclusion_bounds_lower),
                Power.from_watts(exclusion_bounds_upper),
            ),
        )
