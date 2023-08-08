# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Module that defines how to aggregate metrics from battery-inverter components."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping, Set
from datetime import datetime, timezone
from typing import Generic, Iterable, TypeVar

from ...microgrid import connection_manager
from ...microgrid.component import ComponentCategory, ComponentMetricId, InverterType
from ...timeseries import Sample
from .._quantities import Energy, Percentage, Power, Temperature
from ._component_metrics import ComponentMetricsData
from ._result_types import Bounds, PowerMetrics

_logger = logging.getLogger(__name__)
_MIN_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc)


def battery_inverter_mapping(batteries: Iterable[int]) -> dict[int, int]:
    """Create mapping between battery and adjacent inverter.

    Args:
        batteries: Set of batteries

    Returns:
        Mapping between battery and adjacent inverter.
    """
    graph = connection_manager.get().component_graph
    bat_inv_map: dict[int, int] = {}
    for battery_id in batteries:
        try:
            predecessors = graph.predecessors(battery_id)
        except KeyError as err:
            # If battery_id is not in the component graph, then print error and ignore
            # this id. Wrong component id might be bug in config file. We won't stop
            # everything because of bug in config file.
            _logger.error(str(err))
            continue

        inverter_id = next(
            (
                comp.component_id
                for comp in predecessors
                if comp.category == ComponentCategory.INVERTER
                and comp.type == InverterType.BATTERY
            ),
            None,
        )
        if inverter_id is None:
            _logger.info("Battery %d has no adjacent inverter.", battery_id)
        else:
            bat_inv_map[battery_id] = inverter_id
    return bat_inv_map


# Formula output types class have no common interface
# Print all possible types here.
T = TypeVar("T", Sample[Percentage], Sample[Energy], PowerMetrics, Sample[Temperature])


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
    ) -> T | None:
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
    ) -> Sample[Energy] | None:
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
            None
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
    ) -> Sample[Temperature] | None:
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
            return None

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
    ) -> Sample[Percentage] | None:
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
        usable_capacity_x100: float = 0
        used_capacity_x100: float = 0
        total_capacity_x100: float = 0

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
            soc_scaled = (
                (soc - soc_lower_bound) / (soc_upper_bound - soc_lower_bound) * 100
            )
            # we are clamping here because the SoC might be out of bounds
            soc_scaled = min(max(soc_scaled, 0), 100)
            timestamp = max(timestamp, metrics.timestamp)
            used_capacity_x100 += usable_capacity_x100 * soc_scaled
            total_capacity_x100 += usable_capacity_x100

        if timestamp == _MIN_TIMESTAMP:
            return None

        # To avoid zero division error
        if total_capacity_x100 == 0:
            return Sample(
                timestamp=timestamp,
                value=Percentage.from_percent(0.0),
            )
        return Sample(
            timestamp=timestamp,
            value=Percentage.from_percent(used_capacity_x100 / total_capacity_x100),
        )


class PowerBoundsCalculator(MetricCalculator[PowerMetrics]):
    """Define how to calculate PowerBounds metrics."""

    def __init__(
        self,
        batteries: Set[int],
    ) -> None:
        """Create class instance.

        Args:
            batteries: What batteries should be used for calculation.

        Raises:
            ValueError: If no battery has adjacent inverter.
        """
        self._bat_inv_map = battery_inverter_mapping(batteries)
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
        return {cid: self._inverter_metrics for cid in set(self._bat_inv_map.values())}

    def _fetch_inclusion_bounds(
        self,
        battery_id: int,
        inverter_id: int,
        metrics_data: dict[int, ComponentMetricsData],
    ) -> tuple[datetime, list[float], list[float]]:
        timestamp = _MIN_TIMESTAMP
        inclusion_lower_bounds: list[float] = []
        inclusion_upper_bounds: list[float] = []

        # Inclusion upper and lower bounds are not related.
        # If one is missing, then we can still use the other.
        if battery_id in metrics_data:
            data = metrics_data[battery_id]
            value = data.get(ComponentMetricId.POWER_INCLUSION_UPPER_BOUND)
            if value is not None:
                timestamp = max(timestamp, data.timestamp)
                inclusion_upper_bounds.append(value)

            value = data.get(ComponentMetricId.POWER_INCLUSION_LOWER_BOUND)
            if value is not None:
                timestamp = max(timestamp, data.timestamp)
                inclusion_lower_bounds.append(value)

        if inverter_id in metrics_data:
            data = metrics_data[inverter_id]

            value = data.get(ComponentMetricId.ACTIVE_POWER_INCLUSION_UPPER_BOUND)
            if value is not None:
                timestamp = max(data.timestamp, timestamp)
                inclusion_upper_bounds.append(value)

            value = data.get(ComponentMetricId.ACTIVE_POWER_INCLUSION_LOWER_BOUND)
            if value is not None:
                timestamp = max(data.timestamp, timestamp)
                inclusion_lower_bounds.append(value)

        return (timestamp, inclusion_lower_bounds, inclusion_upper_bounds)

    def _fetch_exclusion_bounds(
        self,
        battery_id: int,
        inverter_id: int,
        metrics_data: dict[int, ComponentMetricsData],
    ) -> tuple[datetime, list[float], list[float]]:
        timestamp = _MIN_TIMESTAMP
        exclusion_lower_bounds: list[float] = []
        exclusion_upper_bounds: list[float] = []

        # Exclusion upper and lower bounds are not related.
        # If one is missing, then we can still use the other.
        if battery_id in metrics_data:
            data = metrics_data[battery_id]
            value = data.get(ComponentMetricId.POWER_EXCLUSION_UPPER_BOUND)
            if value is not None:
                timestamp = max(timestamp, data.timestamp)
                exclusion_upper_bounds.append(value)

            value = data.get(ComponentMetricId.POWER_EXCLUSION_LOWER_BOUND)
            if value is not None:
                timestamp = max(timestamp, data.timestamp)
                exclusion_lower_bounds.append(value)

        if inverter_id in metrics_data:
            data = metrics_data[inverter_id]

            value = data.get(ComponentMetricId.ACTIVE_POWER_EXCLUSION_UPPER_BOUND)
            if value is not None:
                timestamp = max(data.timestamp, timestamp)
                exclusion_upper_bounds.append(value)

            value = data.get(ComponentMetricId.ACTIVE_POWER_EXCLUSION_LOWER_BOUND)
            if value is not None:
                timestamp = max(data.timestamp, timestamp)
                exclusion_lower_bounds.append(value)

        return (timestamp, exclusion_lower_bounds, exclusion_upper_bounds)

    def calculate(
        self,
        metrics_data: dict[int, ComponentMetricsData],
        working_batteries: set[int],
    ) -> PowerMetrics | None:
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
        inclusion_bounds_lower = 0.0
        inclusion_bounds_upper = 0.0
        exclusion_bounds_lower = 0.0
        exclusion_bounds_upper = 0.0

        for battery_id in working_batteries:
            inverter_id = self._bat_inv_map[battery_id]
            (
                _ts,
                inclusion_lower_bounds,
                inclusion_upper_bounds,
            ) = self._fetch_inclusion_bounds(battery_id, inverter_id, metrics_data)
            timestamp = max(timestamp, _ts)
            (
                _ts,
                exclusion_lower_bounds,
                exclusion_upper_bounds,
            ) = self._fetch_exclusion_bounds(battery_id, inverter_id, metrics_data)
            if len(inclusion_upper_bounds) > 0:
                inclusion_bounds_upper += min(inclusion_upper_bounds)
            if len(inclusion_lower_bounds) > 0:
                inclusion_bounds_lower += max(inclusion_lower_bounds)
            if len(exclusion_upper_bounds) > 0:
                exclusion_bounds_upper += max(exclusion_upper_bounds)
            if len(exclusion_lower_bounds) > 0:
                exclusion_bounds_lower += min(exclusion_lower_bounds)

        if timestamp == _MIN_TIMESTAMP:
            return None

        return PowerMetrics(
            timestamp=timestamp,
            inclusion_bounds=Bounds(
                Power.from_watts(inclusion_bounds_lower),
                Power.from_watts(inclusion_bounds_upper),
            ),
            exclusion_bounds=Bounds(
                Power.from_watts(exclusion_bounds_lower),
                Power.from_watts(exclusion_bounds_upper),
            ),
        )
