"""Component data types for data coming from a microgrid.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from abc import ABC
from datetime import datetime
from typing import Tuple

import frequenz.api.microgrid.microgrid_pb2 as microgrid_pb
import pytz

from .component_states import EVChargerCableState


class _BaseComponentData(ABC):
    """A private base class for strongly typed component data classes."""

    _raw: microgrid_pb.ComponentData

    @property
    def component_id(self) -> int:
        """Return the component id of the message.

        Returns:
            Component id of the message.
        """
        return self._raw.id

    @property
    def timestamp(self) -> datetime:
        """Return the timestamp of the message.

        Returns:
            Timestamp of the message.
        """
        return self._raw.ts.ToDatetime(tzinfo=pytz.UTC)


class MeterData(_BaseComponentData):
    """A wrapper class for holding meter data."""

    def __init__(self, raw: microgrid_pb.ComponentData) -> None:
        """Create a `MeterData` instance.

        Args:
            raw: raw component data as decoded from the wire.
        """
        self._raw = raw

    @property
    def active_power(self) -> float:
        """Get current power with which the meter is being discharged.

        Returns:
            Current power with which the meter is being discharged.
        """
        return self._raw.meter.data.ac.power_active.value

    @property
    def current_per_phase(self) -> Tuple[float, float, float]:
        """Get apparant current measured by the meter per-phase.

        Returns:
            Current values for each phase.
        """
        phase_1 = self._raw.meter.data.ac.phase_1.current.value
        phase_2 = self._raw.meter.data.ac.phase_2.current.value
        phase_3 = self._raw.meter.data.ac.phase_3.current.value
        return (phase_1, phase_2, phase_3)

    @property
    def voltage_per_phase(self) -> Tuple[float, float, float]:
        """Get voltages measured by the meter per phase.

        Returns:
            Voltage values for each phase.
        """
        phase_1 = self._raw.meter.data.ac.phase_1.voltage.value
        phase_2 = self._raw.meter.data.ac.phase_2.voltage.value
        phase_3 = self._raw.meter.data.ac.phase_3.voltage.value
        return (phase_1, phase_2, phase_3)


class BatteryData(_BaseComponentData):
    """A wrapper class for holding battery data."""

    def __init__(self, raw: microgrid_pb.ComponentData) -> None:
        """Create a `BatteryData` instance.

        Args:
            raw: raw component data as decoded from the wire.
        """
        self._raw = raw

    @property
    def soc(self) -> float:
        """Get current SoC level.

        Returns:
            SoC level.
        """
        return self._raw.battery.data.soc.avg

    @property
    def soc_upper_bound(self) -> float:
        """Get SoC upper bound.

        Returns:
            Maximum SoC level.
        """
        return self._raw.battery.data.soc.system_bounds.upper

    @property
    def soc_lower_bound(self) -> float:
        """Get SoC lower bound.

        Returns:
            Minimum SoC level.
        """
        return self._raw.battery.data.soc.system_bounds.lower

    @property
    def capacity(self) -> float:
        """Get battery capacity.

        Returns:
            Capacity of the battery.
        """
        return self._raw.battery.properties.capacity

    @property
    def power_upper_bound(self) -> float:
        """Get maximum power with which the battery can be charged.

        Returns:
            Maximum power with which the battery can be charged.
        """
        return self._raw.battery.data.dc.power.system_bounds.upper

    @property
    def power_lower_bound(self) -> float:
        """Get minimum power with which the battery can be discharged.

        Returns:
            Minimum power with which the battery can be discharged.
        """
        return self._raw.battery.data.dc.power.system_bounds.lower


class InverterData(_BaseComponentData):
    """A wrapper class for holding inverter data."""

    def __init__(self, raw: microgrid_pb.ComponentData) -> None:
        """Create a `InverterData` instance.

        Args:
            raw: raw component data as decoded from the wire.
        """
        self._raw = raw

    @property
    def active_power(self) -> float:
        """Get current power with which the inverter is being discharged.

        Returns:
            Current power with which the inverter is being discharged.
        """
        return self._raw.inverter.data.ac.power_active.value

    @property
    def active_power_upper_bound(self) -> float:
        """Get maximum power with which the inverter can be charged.

        Returns:
            Maximum power with which the inverter can be charged.
        """
        return self._raw.inverter.data.ac.power_active.system_bounds.upper

    @property
    def active_power_lower_bound(self) -> float:
        """Get minimum power with which the inverter can be discharged.

        Returns:
            Minimum power with which the inverter can be discharged.
        """
        return self._raw.inverter.data.ac.power_active.system_bounds.lower


class EVChargerData(_BaseComponentData):
    """A wrapper class for holding ev_charger data."""

    def __init__(self, raw: microgrid_pb.ComponentData) -> None:
        """Create a `EVChargerData` instance.

        Args:
            raw: raw component data as decoded from the wire.
        """
        self._raw = raw

    @property
    def active_power_consumption(self) -> float:
        """Get current power with which the EV charger is being charged.

        Returns:
            Current power with which the EV charger is being charged.
        """
        return self._raw.ev_charger.data.ac.power_active.value

    @property
    def current_per_phase(self) -> Tuple[float, float, float]:
        """Get apparant current consumed by the ev charger, per-phase.

        Returns:
            Current values for each phase.
        """
        phase_1 = self._raw.ev_charger.data.ac.phase_1.current.value
        phase_2 = self._raw.ev_charger.data.ac.phase_2.current.value
        phase_3 = self._raw.ev_charger.data.ac.phase_3.current.value
        return (phase_1, phase_2, phase_3)

    @property
    def voltage_per_phase(self) -> Tuple[float, float, float]:
        """Get voltages measured by the ev charger, per phase.

        Returns:
            Voltage values for each phase.
        """
        phase_1 = self._raw.ev_charger.data.ac.phase_1.voltage.value
        phase_2 = self._raw.ev_charger.data.ac.phase_2.voltage.value
        phase_3 = self._raw.ev_charger.data.ac.phase_3.voltage.value
        return (phase_1, phase_2, phase_3)

    @property
    def cable_state(self) -> EVChargerCableState:
        """Get cable state of the ev charger.

        Returns:
            Cable state.
        """
        return EVChargerCableState.from_pb(self._raw.ev_charger.state.cable_state)
