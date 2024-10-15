# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Power distribution state tracking for ev chargers."""


from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from frequenz.client.microgrid import EVChargerData
from frequenz.quantities import Power


@dataclass
class EvcState:
    """A class for tracking state of an ev charger."""

    component_id: int
    """The component id of the ev charger."""

    last_data: EVChargerData
    """The last data received from the EV charger."""

    power: Power
    """The power currently used by the EV charger."""

    last_allocation: Power
    """The last allocation made for the EV."""

    last_reallocation_time: datetime
    """The last time the ev charger was allocated power.

    Used to make sure we don't allocate power to the ev charger too often.
    """

    last_charging_time: datetime
    """The last time the ev charger was charging.

    Used to de-allocate power from the ev charger if it has not been charging
    for a while.
    """

    def update_last_allocation(self, allocation: Power, alloc_time: datetime) -> None:
        """Update the last allocation and related timestamps.

        Args:
            allocation: The most allocation allocation made for the EV.
            alloc_time: The time at which the allocation was made.
        """
        self.last_allocation = allocation
        self.last_reallocation_time = alloc_time
        self.last_charging_time = alloc_time

    def update_state(
        self,
        latest_ev_data: EVChargerData,
    ) -> None:
        """Update EvcState from component data.

        Args:
            latest_ev_data: latest ev data from component data stream.
        """
        self.power = Power.from_watts(latest_ev_data.active_power)
        self.last_data = latest_ev_data

        if self.power > Power.zero():
            self.last_charging_time = latest_ev_data.timestamp


class EvcStates:
    """Tracks states of all ev chargers."""

    _states: dict[int, EvcState]

    def __init__(self) -> None:
        """Initialize this instance."""
        self._states = {}

    def get_ev_total_used_power(self) -> Power:
        """Return the total power consumed by all EV Chargers."""
        total_used = Power.zero()
        for evc in self._states.values():
            total_used += evc.power
        return total_used

    def get_total_allocated_power(self) -> Power:
        """Return the total power allocated to all EV Chargers."""
        total_allocated = Power.zero()
        for evc in self._states.values():
            total_allocated += evc.last_allocation
        return total_allocated

    def get(self, component_id: int) -> EvcState:
        """Return a reference to the EvcState object with the given component_id.

        Args:
            component_id: identifies the object to retrieve.

        Returns:
            The EvcState object with the given component_id.
        """
        return self._states[component_id]

    def add_evc(self, state: EvcState) -> None:
        """Add the given EvcState object to the list.

        Args:
            state: The EvcState object to add to the list.
        """
        self._states[state.component_id] = state

    def values(self) -> Iterable[EvcState]:
        """Return an iterator over all EvcState objects.

        Returns:
            An iterator over all EvcState objects.
        """
        return self._states.values()

    def __contains__(self, component_id: int) -> bool:
        """Check if the given component_id has an associated EvcState object.

        Args:
            component_id: The component id to test.

        Returns:
            Boolean indicating whether the given component_id is a known
            EvCharger.
        """
        return component_id in self._states
