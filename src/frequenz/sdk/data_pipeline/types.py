"""Common types for the Data Pipeline.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class ComponentMetricId(Enum):
    """An enum representing the various metrics available in the data pipeline."""

    ACTIVE_POWER = "active_power"

    CURRENT_PHASE_1 = "current_phase_1"
    CURRENT_PHASE_2 = "current_phase_2"
    CURRENT_PHASE_3 = "current_phase_3"

    VOLTAGE_PHASE_1 = "voltage_phase_1"
    VOLTAGE_PHASE_2 = "voltage_phase_2"
    VOLTAGE_PHASE_3 = "voltage_phase_3"

    SOC = "soc"
    SOC_LOWER_BOUND = "soc_lower_bound"
    SOC_UPPER_BOUND = "soc_upper_bound"
    CAPACITY = "capacity"

    POWER_LOWER_BOUND = "power_lower_bound"
    POWER_UPPER_BOUND = "power_upper_bound"

    ACTIVE_POWER_LOWER_BOUND = "active_power_lower_bound"
    ACTIVE_POWER_UPPER_BOUND = "active_power_upper_bound"


@dataclass
class ComponentMetricRequest:
    """A request object to start streaming a metric for a component."""

    # A namespace that this request belongs to. Metric requests with a shared
    # namespace enable the reuse of channels within that namespace.
    #
    # If for example, an actor making a multiple requests, uses the name of the
    # actor as the namespace, then requests from the actor will get reused when
    # possible.
    namespace: str

    # Id of the requested component
    component_id: int

    # `metric_name` would be attribute names on BatteryData, etc.  Example
    # value: `soc`, `current_phase_1`, etc.  For individual phase current and
    # voltage values, we need to add additional attributes to `MeterData` and
    # `EVChargerData`.
    metric_id: ComponentMetricId

    # The start time from which data is required.  When None, we will stream
    # only live data.
    start_time: Optional[datetime]

    def get_channel_name(self) -> str:
        """Return a channel name constructed from Self.

        This channel name can be used by the sending side and receiving sides to
        identify the right channel from the ChannelRegistry.

        Returns:
            A string denoting a channel name.
        """
        return f"{self.component_id}::{self.metric_id.name}::{self.start_time}::{self.namespace}"


@dataclass(frozen=True)
class Sample:
    """A measurement taken at a particular point in time.

    The `value` could be `None` if a component is malfunctioning or data is
    lacking for another reason, but a sample still needs to be sent to have a
    coherent view on a group of component metrics for a particular timestamp.
    """

    timestamp: datetime
    value: Optional[float] = None
