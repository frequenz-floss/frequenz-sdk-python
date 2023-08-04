# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Component data types for data coming from a microgrid."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import frequenz.api.microgrid.battery_pb2 as battery_pb
import frequenz.api.microgrid.inverter_pb2 as inverter_pb
import frequenz.api.microgrid.microgrid_pb2 as microgrid_pb

from ._component_states import EVChargerCableState, EVChargerComponentState


@dataclass(frozen=True)
class ComponentData(ABC):
    """A private base class for strongly typed component data classes."""

    component_id: int
    """The ID identifying this component in the microgrid."""

    timestamp: datetime
    """The timestamp of when the data was measured."""

    # The `raw` attribute is excluded from the constructor as it can only be provided
    # when instantiating `ComponentData` using the `from_proto` method, which reads
    # data from a protobuf message. The whole protobuf message is stored as the `raw`
    # attribute. When `ComponentData` is not instantiated from a protobuf message,
    # i.e. using the constructor, `raw` will be set to `None`.
    raw: Optional[microgrid_pb.ComponentData] = field(default=None, init=False)
    """Raw component data as decoded from the wire."""

    def _set_raw(self, raw: microgrid_pb.ComponentData) -> None:
        """Store raw protobuf message.

        It is preferred to keep the dataclasses immutable (frozen) and make the `raw`
            attribute read-only, which is why the approach of writing to `__dict__`
            was used, instead of mutating the `self.raw = raw` attribute directly.

        Args:
            raw: raw component data as decoded from the wire.
        """
        self.__dict__["raw"] = raw

    @classmethod
    @abstractmethod
    def from_proto(cls, raw: microgrid_pb.ComponentData) -> ComponentData:
        """Create ComponentData from a protobuf message.

        Args:
            raw: raw component data as decoded from the wire.
        """


@dataclass(frozen=True)
class MeterData(ComponentData):
    """A wrapper class for holding meter data."""

    active_power: float
    """The 3-phase active power, in Watts, represented in the passive sign convention.
            +ve current means consumption, away from the grid.
            -ve current means supply into the grid.
    """

    current_per_phase: Tuple[float, float, float]
    """AC current in Amperes (A) for phase/line 1,2 and 3 respectively.
            +ve current means consumption, away from the grid.
            -ve current means supply into the grid.
    """

    voltage_per_phase: Tuple[float, float, float]
    """The ac voltage in volts (v) between the line and the neutral wire for phase/line
        1,2 and 3 respectively.
    """

    frequency: float
    """The AC power frequency in Hertz (Hz)."""

    @classmethod
    def from_proto(cls, raw: microgrid_pb.ComponentData) -> MeterData:
        """Create MeterData from a protobuf message.

        Args:
            raw: raw component data as decoded from the wire.

        Returns:
            Instance of MeterData created from the protobuf message.
        """
        meter_data = cls(
            component_id=raw.id,
            timestamp=raw.ts.ToDatetime(tzinfo=timezone.utc),
            active_power=raw.meter.data.ac.power_active.value,
            current_per_phase=(
                raw.meter.data.ac.phase_1.current.value,
                raw.meter.data.ac.phase_2.current.value,
                raw.meter.data.ac.phase_3.current.value,
            ),
            voltage_per_phase=(
                raw.meter.data.ac.phase_1.voltage.value,
                raw.meter.data.ac.phase_2.voltage.value,
                raw.meter.data.ac.phase_3.voltage.value,
            ),
            frequency=raw.meter.data.ac.frequency.value,
        )
        meter_data._set_raw(raw=raw)
        return meter_data


@dataclass(frozen=True)
class BatteryData(ComponentData):
    """A wrapper class for holding battery data."""

    soc: float
    """Battery's overall SoC in percent (%)."""

    soc_lower_bound: float
    """The SoC below which discharge commands will be blocked by the system,
        in percent (%).
    """

    soc_upper_bound: float
    """The SoC above which charge commands will be blocked by the system,
        in percent (%).
    """

    capacity: float
    """The capacity of the battery in Wh (Watt-hour)."""

    power_lower_bound: float
    """The maximum discharge power, in watts, represented in the passive sign
        convention. this will be a negative number, or zero if no discharging is
        possible.
    """

    power_upper_bound: float
    """The maximum charge power, in Watts, represented in the passive sign convention.
        This will be a positive number, or zero if no charging is possible.
    """

    temperature: float
    """The (average) temperature reported by the battery, in Celcius (°C)."""

    _relay_state: battery_pb.RelayState.ValueType
    """State of the battery relay."""

    _component_state: battery_pb.ComponentState.ValueType
    """State of the battery."""

    _errors: List[battery_pb.Error]
    """List of errors in protobuf struct."""

    @classmethod
    def from_proto(cls, raw: microgrid_pb.ComponentData) -> BatteryData:
        """Create BatteryData from a protobuf message.

        Args:
            raw: raw component data as decoded from the wire.

        Returns:
            Instance of BatteryData created from the protobuf message.
        """
        battery_data = cls(
            component_id=raw.id,
            timestamp=raw.ts.ToDatetime(tzinfo=timezone.utc),
            soc=raw.battery.data.soc.avg,
            soc_lower_bound=raw.battery.data.soc.system_bounds.lower,
            soc_upper_bound=raw.battery.data.soc.system_bounds.upper,
            capacity=raw.battery.properties.capacity,
            power_lower_bound=raw.battery.data.dc.power.system_bounds.lower,
            power_upper_bound=raw.battery.data.dc.power.system_bounds.upper,
            temperature=raw.battery.data.temperature.avg,
            _relay_state=raw.battery.state.relay_state,
            _component_state=raw.battery.state.component_state,
            _errors=list(raw.battery.errors),
        )
        battery_data._set_raw(raw=raw)
        return battery_data


@dataclass(frozen=True)
class InverterData(ComponentData):
    """A wrapper class for holding inverter data."""

    active_power: float
    """The 3-phase active power, in Watts, represented in the passive sign convention.
            +ve current means consumption, away from the grid.
            -ve current means supply into the grid.
    """

    active_power_lower_bound: float
    """The maximum discharge power, in Watts, represented in the passive sign
        convention. This will be a negative number, or zero if no discharging is
        possible.
    """

    active_power_upper_bound: float
    """The maximum charge power, in Watts, represented in the passive sign convention.
        This will be a positive number, or zero if no charging is possible.
    """

    _component_state: inverter_pb.ComponentState.ValueType
    """State of the inverter."""

    _errors: List[inverter_pb.Error]
    """List of errors from the component."""

    @classmethod
    def from_proto(cls, raw: microgrid_pb.ComponentData) -> InverterData:
        """Create InverterData from a protobuf message.

        Args:
            raw: raw component data as decoded from the wire.

        Returns:
            Instance of InverterData created from the protobuf message.
        """
        inverter_data = cls(
            component_id=raw.id,
            timestamp=raw.ts.ToDatetime(tzinfo=timezone.utc),
            active_power=raw.inverter.data.ac.power_active.value,
            active_power_lower_bound=raw.inverter.data.ac.power_active.system_bounds.lower,
            active_power_upper_bound=raw.inverter.data.ac.power_active.system_bounds.upper,
            _component_state=raw.inverter.state.component_state,
            _errors=list(raw.inverter.errors),
        )

        inverter_data._set_raw(raw=raw)
        return inverter_data


@dataclass(frozen=True)
class EVChargerData(ComponentData):
    """A wrapper class for holding ev_charger data."""

    active_power: float
    """The 3-phase active power, in Watts, represented in the passive sign convention.
        +ve current means consumption, away from the grid.
        -ve current means supply into the grid.
    """

    current_per_phase: Tuple[float, float, float]
    """AC current in Amperes (A) for phase/line 1,2 and 3 respectively.
        +ve current means consumption, away from the grid.
        -ve current means supply into the grid.
    """

    voltage_per_phase: Tuple[float, float, float]
    """The AC voltage in Volts (V) between the line and the neutral
        wire for phase/line 1,2 and 3 respectively.
    """

    cable_state: EVChargerCableState
    """The state of the ev charger's cable."""

    component_state: EVChargerComponentState
    """The state of the ev charger."""

    @classmethod
    def from_proto(cls, raw: microgrid_pb.ComponentData) -> EVChargerData:
        """Create EVChargerData from a protobuf message.

        Args:
            raw: raw component data as decoded from the wire.

        Returns:
            Instance of EVChargerData created from the protobuf message.
        """
        ev_charger_data = cls(
            component_id=raw.id,
            timestamp=raw.ts.ToDatetime(tzinfo=timezone.utc),
            active_power=raw.ev_charger.data.ac.power_active.value,
            current_per_phase=(
                raw.ev_charger.data.ac.phase_1.current.value,
                raw.ev_charger.data.ac.phase_2.current.value,
                raw.ev_charger.data.ac.phase_3.current.value,
            ),
            voltage_per_phase=(
                raw.ev_charger.data.ac.phase_1.voltage.value,
                raw.ev_charger.data.ac.phase_2.voltage.value,
                raw.ev_charger.data.ac.phase_3.voltage.value,
            ),
            cable_state=EVChargerCableState.from_pb(raw.ev_charger.state.cable_state),
            component_state=EVChargerComponentState.from_pb(
                raw.ev_charger.state.component_state
            ),
        )
        ev_charger_data._set_raw(raw=raw)
        return ev_charger_data
