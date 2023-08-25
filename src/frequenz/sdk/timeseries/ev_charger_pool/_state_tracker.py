# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""State tracking for EV Charger pools."""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import Optional

from frequenz.channels import Receiver
from frequenz.channels.util import Merge

from ... import microgrid
from ..._internal._asyncio import cancel_and_await
from ...microgrid.component import (
    EVChargerCableState,
    EVChargerComponentState,
    EVChargerData,
)


class EVChargerState(Enum):
    """State of individual ev charger."""

    UNSPECIFIED = "UNSPECIFIED"
    MISSING = "MISSING"

    IDLE = "IDLE"
    EV_PLUGGED = "EV_PLUGGED"
    EV_LOCKED = "EV_LOCKED"
    ERROR = "ERROR"

    @classmethod
    def from_ev_charger_data(cls, data: EVChargerData) -> EVChargerState:
        """Create an `EVChargerState` instance from component data.

        Args:
            data: ev charger data coming from microgrid.

        Returns:
            An `EVChargerState` instance.
        """
        if data.component_state == EVChargerComponentState.UNSPECIFIED:
            return EVChargerState.UNSPECIFIED
        if data.component_state in (
            EVChargerComponentState.AUTHORIZATION_REJECTED,
            EVChargerComponentState.ERROR,
        ):
            return EVChargerState.ERROR

        if data.cable_state == EVChargerCableState.UNSPECIFIED:
            return EVChargerState.UNSPECIFIED
        if data.cable_state == EVChargerCableState.EV_LOCKED:
            return EVChargerState.EV_LOCKED
        if data.cable_state == EVChargerCableState.EV_PLUGGED:
            return EVChargerState.EV_PLUGGED
        return EVChargerState.IDLE

    def is_ev_connected(self) -> bool:
        """Check whether an EV is connected to the charger.

        Returns:
            Whether an EV is connected to the charger.
        """
        return self in (EVChargerState.EV_PLUGGED, EVChargerState.EV_LOCKED)


class StateTracker:
    """A class for keeping track of the states of all EV Chargers in a pool."""

    def __init__(self, component_ids: set[int]) -> None:
        """Create a `_StateTracker` instance.

        Args:
            component_ids: EV Charger component ids to track the states of.
        """
        self._component_ids = component_ids
        self._task: asyncio.Task[None] = asyncio.create_task(self._run())
        self._merged_stream: Optional[Merge[EVChargerData]] = None

        # Initialize all components to the `MISSING` state.  This will change as data
        # starts arriving from the individual components.
        self._states: dict[int, EVChargerState] = {
            component_id: EVChargerState.MISSING for component_id in component_ids
        }

    def get(self, component_id: int) -> EVChargerState:
        """Return the current state of the EV Charger with the given component ID.

        Args:
            component_id: id of the EV Charger whose state is being fetched.

        Returns:
            An `EVChargerState` value corresponding to the given component id.
        """
        return self._states[component_id]

    def _update(
        self,
        data: EVChargerData,
    ) -> None:
        """Update the state of an EV Charger, from a new data point.

        Args:
            data: component data from the microgrid, for an EV Charger in the pool.
        """
        evc_id = data.component_id
        new_state = EVChargerState.from_ev_charger_data(data)
        self._states[evc_id] = new_state

    async def _run(self) -> None:
        api_client = microgrid.connection_manager.get().api_client
        streams: list[Receiver[EVChargerData]] = await asyncio.gather(
            *[api_client.ev_charger_data(cid) for cid in self._component_ids]
        )
        self._merged_stream = Merge(*streams)
        async for data in self._merged_stream:
            self._update(data)

    async def stop(self) -> None:
        """Stop the status tracker."""
        await cancel_and_await(self._task)
        if self._merged_stream:
            await self._merged_stream.stop()
