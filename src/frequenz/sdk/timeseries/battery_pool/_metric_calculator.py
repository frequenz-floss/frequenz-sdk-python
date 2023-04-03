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
from ._component_metrics import ComponentMetricsData
from ._result_types import Bound, CapacityMetrics, PowerMetrics, SoCMetrics

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
T = TypeVar("T", SoCMetrics, CapacityMetrics, PowerMetrics)


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


class CapacityCalculator(MetricCalculator[CapacityMetrics]):
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
    ) -> CapacityMetrics | None:
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
        result = CapacityMetrics(
            timestamp=_MIN_TIMESTAMP, total_capacity=0, bound=Bound(lower=0, upper=0)
        )

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

            result.timestamp = max(result.timestamp, metrics.timestamp)
            result.total_capacity += capacity
            result.bound.upper += capacity * soc_upper_bound
            result.bound.lower += capacity * soc_lower_bound

        return None if result.timestamp == _MIN_TIMESTAMP else result


class SoCCalculator(MetricCalculator[SoCMetrics]):
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
    ) -> SoCMetrics | None:
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
        used_capacity: float = 0
        total_capacity: float = 0
        capacity_bound = Bound(0, 0)

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

            timestamp = max(timestamp, metrics.timestamp)
            used_capacity += capacity * soc
            total_capacity += capacity
            capacity_bound.upper += capacity * soc_upper_bound
            capacity_bound.lower += capacity * soc_lower_bound

        if timestamp == _MIN_TIMESTAMP:
            return None

        # To avoid zero division error
        if total_capacity == 0:
            return SoCMetrics(
                timestamp=timestamp,
                average_soc=0,
                bound=Bound(0, 0),
            )
        return SoCMetrics(
            timestamp=timestamp,
            average_soc=used_capacity / total_capacity,
            bound=Bound(
                lower=capacity_bound.lower / total_capacity,
                upper=capacity_bound.upper / total_capacity,
            ),
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
            ComponentMetricId.POWER_LOWER_BOUND,
            ComponentMetricId.POWER_UPPER_BOUND,
        ]

        self._inverter_metrics = [
            ComponentMetricId.ACTIVE_POWER_LOWER_BOUND,
            ComponentMetricId.ACTIVE_POWER_UPPER_BOUND,
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
        # In the future we will have lower bound, too.

        result = PowerMetrics(
            timestamp=_MIN_TIMESTAMP,
            supply_bound=Bound(0, 0),
            consume_bound=Bound(0, 0),
        )

        for battery_id in working_batteries:
            supply_upper_bounds: list[float] = []
            consume_upper_bounds: list[float] = []

            if battery_id in metrics_data:
                data = metrics_data[battery_id]

                # Consume and supply bounds are not related.
                # If one is missing, then we can still use the other.
                value = data.get(ComponentMetricId.POWER_UPPER_BOUND)
                if value is not None:
                    result.timestamp = max(result.timestamp, data.timestamp)
                    consume_upper_bounds.append(value)

                value = data.get(ComponentMetricId.POWER_LOWER_BOUND)
                if value is not None:
                    result.timestamp = max(result.timestamp, data.timestamp)
                    supply_upper_bounds.append(value)

            inverter_id = self._bat_inv_map[battery_id]
            if inverter_id in metrics_data:
                data = metrics_data[inverter_id]

                value = data.get(ComponentMetricId.ACTIVE_POWER_UPPER_BOUND)
                if value is not None:
                    result.timestamp = max(data.timestamp, result.timestamp)
                    consume_upper_bounds.append(value)

                value = data.get(ComponentMetricId.ACTIVE_POWER_LOWER_BOUND)
                if value is not None:
                    result.timestamp = max(data.timestamp, result.timestamp)
                    supply_upper_bounds.append(value)

            if len(consume_upper_bounds) > 0:
                result.consume_bound.upper += min(consume_upper_bounds)
            if len(supply_upper_bounds) > 0:
                result.supply_bound.lower += max(supply_upper_bounds)

        if result.timestamp == _MIN_TIMESTAMP:
            return None

        return result
