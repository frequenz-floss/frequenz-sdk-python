# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Class that stores values of the component metrics."""


from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.metrics import Metric

from ...microgrid._old_component_data import TransitionalMetric


@dataclass(frozen=True, eq=False)
class ComponentMetricsData:
    """Store values of the component metrics."""

    component_id: ComponentId
    """The component ID the data is for."""

    timestamp: datetime
    """The timestamp for all the metrics."""

    metrics: Mapping[Metric | TransitionalMetric, float]
    """The values for each metric."""

    def get(self, metric: Metric | TransitionalMetric) -> float | None:
        """Get metric value.

        Args:
            metric: metric id

        Returns:
            Value of the metric.
        """
        return self.metrics.get(metric, None)

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

        return self.component_id == other.component_id and self.metrics == other.metrics
