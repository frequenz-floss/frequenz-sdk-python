# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Class that stores values of the component metrics."""


from collections.abc import Mapping
from datetime import datetime

from frequenz.client.microgrid import ComponentMetricId


class ComponentMetricsData:
    """Store values of the component metrics."""

    def __init__(
        self,
        component_id: int,
        timestamp: datetime,
        metrics: Mapping[ComponentMetricId, float],
    ) -> None:
        """Create class instance.

        Args:
            component_id: component id
            timestamp: timestamp the same for all metrics
            metrics: map between metrics and its values.
        """
        self._component_id = component_id
        self._timestamp = timestamp
        self._metrics: Mapping[ComponentMetricId, float] = metrics

    @property
    def component_id(self) -> int:
        """Get component id of the given metrics.

        Returns:
            Component id
        """
        return self._component_id

    @property
    def timestamp(self) -> datetime:
        """Get timestamp of the given metrics.

        Returns:
            Timestamp (one for all metrics).
        """
        return self._timestamp

    def get(self, metric: ComponentMetricId) -> float | None:
        """Get metric value.

        Args:
            metric: metric id

        Returns:
            Value of the metric.
        """
        return self._metrics.get(metric, None)

    def __eq__(self, other: object) -> bool:
        """Compare two objects of this class.

        Object are considered as equal if all stored values except for timestamp
        are equal.

        Args:
            other: object to compare.

        Returns:
            True if two objects are equal, false otherwise.
        """
        if not isinstance(other, ComponentMetricsData):
            return False

        return (
            self.component_id == other.component_id and self._metrics == other._metrics
        )
