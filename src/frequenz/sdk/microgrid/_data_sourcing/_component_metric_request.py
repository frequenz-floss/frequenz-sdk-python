# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""The ComponentMetricRequest class."""

from dataclasses import dataclass
from datetime import datetime

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.metrics import Metric

from frequenz.sdk.microgrid._old_component_data import TransitionalMetric

__all__ = ["ComponentMetricRequest", "Metric"]


@dataclass
class ComponentMetricRequest:
    """A request to start streaming a component's metric.

    Requesters use this class to specify which component's metric they want to subscribe
    to, including the component ID, metric ID, and an optional start time. The
    `namespace` is defined by the requester and influences the construction of the
    channel name via the `get_channel_name()` method.

    The `namespace` allows differentiation of data streams for the same component and
    metric. For example, requesters can use different `namespace` values to subscribe to
    raw or resampled data streams separately. This ensures that each requester receives
    the appropriate type of data without interference. Requests with the same
    `namespace`, `component_id`, and `metric` will use the same channel, preventing
    unnecessary duplication of data streams.

    The requester and provider must use the same channel name so that they can
    independently retrieve the same channel from the `ChannelRegistry`.  This is
    achieved by using the `get_channel_name` method to generate the name on both sides
    based on parameters set by the requesters.
    """

    namespace: str
    """A client-defined identifier influencing the channel name."""

    component_id: ComponentId
    """The ID of the requested component."""

    metric: Metric | TransitionalMetric
    """The ID of the requested component's metric."""

    start_time: datetime | None
    """The start time from which data is required.

    If None, only live data is streamed.
    """

    def get_channel_name(self) -> str:
        """Construct the channel name based on the request parameters.

        Returns:
            A string representing the channel name.
        """
        start = f",start={self.start_time}" if self.start_time else ""
        return (
            "component_metric_request<"
            f"namespace={self.namespace},"
            f"component_id={self.component_id},"
            f"metric={self.metric.name}"
            f"{start}"
            ">"
        )
