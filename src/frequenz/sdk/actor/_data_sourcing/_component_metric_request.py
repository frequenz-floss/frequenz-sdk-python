# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""The ComponentMetricRequest class."""

from dataclasses import dataclass
from datetime import datetime

from frequenz.client.microgrid import ComponentMetricId


@dataclass
class ComponentMetricRequest:
    """A request object to start streaming a metric for a component."""

    namespace: str
    """The namespace that this request belongs to.

    Metric requests with a shared namespace enable the reuse of channels within
    that namespace.

    If for example, an actor making a multiple requests, uses the name of the
    actor as the namespace, then requests from the actor will get reused when
    possible.
    """

    component_id: int
    """The ID of the requested component."""

    metric_id: ComponentMetricId
    """The ID of the requested component's metric."""

    start_time: datetime | None
    """The start time from which data is required.

    When None, we will stream only live data.
    """

    def get_channel_name(self) -> str:
        """Return a channel name constructed from Self.

        This channel name can be used by the sending side and receiving sides to
        identify the right channel from the ChannelRegistry.

        Returns:
            A string denoting a channel name.
        """
        return (
            f"component-stream::{self.component_id}::{self.metric_id.name}::"
            f"{self.start_time}::{self.namespace}"
        )
