# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Component data types for data coming from a microgrid."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple

import frequenz.api.microgrid.microgrid_pb2 as microgrid_pb
import pytz

from ._component_states import EVChargerCableState


@dataclass(frozen=True)
class ComponentData(ABC):
    """A private base class for strongly typed component data classes.

    Attributes:
        component_id: the ID identifying this component in the microgrid
        timestamp: the timestamp of when the data was measured.
        raw: raw component data as decoded from the wire
    """

    component_id: int
    timestamp: datetime
    # The `raw` attribute is excluded from the constructor as it can only be provided
    # when instantiating `ComponentData` using the `from_proto` method, which reads
    # data from a protobuf message. The whole protobuf message is stored as the `raw`
    # attribute. When `ComponentData` is not instantiated from a protobuf message,
    # i.e. using the constructor, `raw` will be set to `None`.
    raw: Optional[microgrid_pb.ComponentData] = field(default=None, init=False)

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
    """A wrapper class for holding meter data.

    Attributes:
        active_power: the 3-phase active power, in Watts, represented in the passive
            sign convention.
            +ve current means consumption, away from the grid.
            -ve current means supply into the grid.
        current_per_phase: AC current in Amperes (A) for phase/line 1,2 and 3
            respectively.
            +ve current means consumption, away from the grid.
            -ve current means supply into the grid.
        voltage_per_phase: the AC voltage in Volts (V) between the line and the neutral
            wire for phase/line 1,2 and 3 respectively.
        frequency: the AC power frequency in Hertz (Hz).
    """

    active_power: float
    current_per_phase: Tuple[float, float, float]
    voltage_per_phase: Tuple[float, float, float]
    frequency: float

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
            timestamp=raw.ts.ToDatetime(tzinfo=pytz.UTC),
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
    """A wrapper class for holding battery data.

    Attributes:
        soc: battery's overall SoC in percent (%).
        soc_lower_bound: the SoC below which discharge commands will be blocked by the
            system, in percent (%).
        soc_upper_bound: the SoC above which charge commands will be blocked by the
            system, in percent (%).
        capacity: the capacity of the battery in Wh (Watt-hour)
        power_lower_bound: the maximum discharge power, in Watts, represented in the
            passive sign convention. This will be a negative number, or zero if no
            discharging is possible.
        power_upper_bound: the maximum charge power, in Watts, represented in the
            passive sign convention. This will be a positive number, or zero if no
            charging is possible.
        temperature_max: the maximum temperature of all the blocks in a battery, in
            Celcius (°C).
    """

    soc: float
    soc_lower_bound: float
    soc_upper_bound: float
    capacity: float
    power_lower_bound: float
    power_upper_bound: float
    temperature_max: float

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
            timestamp=raw.ts.ToDatetime(tzinfo=pytz.UTC),
            soc=raw.battery.data.soc.avg,
            soc_lower_bound=raw.battery.data.soc.system_bounds.lower,
            soc_upper_bound=raw.battery.data.soc.system_bounds.upper,
            capacity=raw.battery.properties.capacity,
            power_lower_bound=raw.battery.data.dc.power.system_bounds.lower,
            power_upper_bound=raw.battery.data.dc.power.system_bounds.upper,
            temperature_max=raw.battery.data.temperature.max,
        )
        battery_data._set_raw(raw=raw)
        return battery_data


@dataclass(frozen=True)
class InverterData(ComponentData):
    """A wrapper class for holding inverter data.

    Attributes:
        active_power: the 3-phase active power, in Watts, represented in the passive
            sign convention.
            +ve current means consumption, away from the grid.
            -ve current means supply into the grid.
        active_power_lower_bound: the maximum discharge power, in Watts, represented in
            the passive sign convention. This will be a negative number, or zero if no
            discharging is possible.
        active_power_upper_bound: the maximum charge power, in Watts, represented in
            the passive sign convention. This will be a positive number, or zero if no
            charging is possible.
    """

    active_power: float
    active_power_lower_bound: float
    active_power_upper_bound: float

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
            timestamp=raw.ts.ToDatetime(tzinfo=pytz.UTC),
            active_power=raw.inverter.data.ac.power_active.value,
            active_power_lower_bound=raw.inverter.data.ac.power_active.system_bounds.lower,
            active_power_upper_bound=raw.inverter.data.ac.power_active.system_bounds.upper,
        )
        inverter_data._set_raw(raw=raw)
        return inverter_data


@dataclass(frozen=True)
class EVChargerData(ComponentData):
    """A wrapper class for holding ev_charger data.

    Attributes:
        active_power_consumption: the 3-phase active power, in Watts, represented in
            the passive sign convention.
            +ve current means consumption, away from the grid.
            -ve current means supply into the grid.
        current_per_phase: AC current in Amperes (A) for phase/line 1,2 and 3
            respectively.
            +ve current means consumption, away from the grid.
            -ve current means supply into the grid.
        voltage_per_phase: the AC voltage in Volts (V) between the line and the neutral
            wire for phase/line 1,2 and 3 respectively.
        cable_state: the state of the ev charger's cable
    """

    active_power: float
    current_per_phase: Tuple[float, float, float]
    voltage_per_phase: Tuple[float, float, float]
    cable_state: EVChargerCableState

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
            timestamp=raw.ts.ToDatetime(tzinfo=pytz.UTC),
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
        )
        ev_charger_data._set_raw(raw=raw)
        return ev_charger_data
