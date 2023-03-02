# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""State tracking for EV Charger pools."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from frequenz.channels import Broadcast, Receiver
from frequenz.channels.util import Merge

from frequenz.sdk import microgrid
from frequenz.sdk._internal.asyncio import cancel_and_await

from ...microgrid.component import (
    EVChargerCableState,
    EVChargerComponentState,
    EVChargerData,
)


class EVChargerState(Enum):
    """State of individual ev charger."""

    UNSPECIFIED = "UNSPECIFIED"
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
        if data.component_state in (
            EVChargerComponentState.AUTHORIZATION_REJECTED,
            EVChargerComponentState.ERROR,
        ):
            return EVChargerState.ERROR
        if data.cable_state == EVChargerCableState.EV_LOCKED:
            return EVChargerState.EV_LOCKED
        if data.cable_state == EVChargerCableState.EV_PLUGGED:
            return EVChargerState.EV_PLUGGED
        return EVChargerState.IDLE


@dataclass(frozen=True)
class EVChargerPoolStates:
    """States of all EV Chargers in the pool."""

    _states: dict[int, EVChargerState]
    _changed_component: Optional[int] = None

    def __iter__(self) -> Iterator[tuple[int, EVChargerState]]:
        """Iterate over states of all EV Chargers.

        Returns:
            An iterator over all EV Charger states.
        """
        return iter(self._states.items())

    def latest_change(self) -> Optional[tuple[int, EVChargerState]]:
        """Return the most recent EV Charger state change.

        The first `EVChargerPoolStates` instance created by a `StateTracker` will just
        be a representation of the states of all EV Chargers.  At that point, the most
        recent change in state of an ev charger will be unknown, so this function will
        return `None`.

        Returns:
            None, when the most recent change is unknown.  Otherwise, a tuple with
                the component ID of an EV Charger that just had a state change, and its
                new state.
        """
        if self._changed_component is None:
            return None
        return (
            self._changed_component,
            self._states.setdefault(
                self._changed_component, EVChargerState.UNSPECIFIED
            ),
        )


class StateTracker:
    """A class for keeping track of the states of all EV Chargers in a pool."""

    def __init__(self, component_ids: set[int]) -> None:
        """Create a `_StateTracker` instance.

        Args:
            component_ids: EV Charger component ids to track the states of.
        """
        self._component_ids = component_ids
        self._channel = Broadcast[EVChargerPoolStates](
            "EVCharger States", resend_latest=True
        )
        self._task: Optional[asyncio.Task[None]] = None
        self._merged_stream: Optional[Merge[EVChargerData]] = None
        self._states: dict[int, EVChargerState] = {}

    def _get(self) -> EVChargerPoolStates:
        """Get a representation of the current states of all EV Chargers.

        Returns:
            An `EVChargerPoolStates` instance.
        """
        return EVChargerPoolStates(self._states)

    def _update(
        self,
        data: EVChargerData,
    ) -> Optional[EVChargerPoolStates]:
        """Update the state of an EV Charger, from a new data point.

        Args:
            data: component data from the microgrid, for an EV Charger in the pool.

        Returns:
            A new `EVChargerPoolStates` instance representing all the EV Chargers in
                the pool, in case there has been a state change for any of the EV
                Chargers, or `None` otherwise.
        """
        evc_id = data.component_id
        new_state = EVChargerState.from_ev_charger_data(data)
        if evc_id not in self._states or self._states[evc_id] != new_state:
            self._states[evc_id] = new_state
            return EVChargerPoolStates(self._states, evc_id)
        return None

    async def _run(self) -> None:
        api_client = microgrid.get().api_client
        streams: list[Receiver[EVChargerData]] = await asyncio.gather(
            *[api_client.ev_charger_data(cid) for cid in self._component_ids]
        )

        latest_messages: list[EVChargerData] = await asyncio.gather(
            *[stream.receive() for stream in streams]
        )
        self._states = {
            msg.component_id: EVChargerState.from_ev_charger_data(msg)
            for msg in latest_messages
        }
        self._merged_stream = Merge(*streams)
        sender = self._channel.new_sender()
        sender.send(self._get())
        async for data in self._merged_stream:
            if updated_states := self._update(data):
                sender.send(updated_states)

    def new_receiver(self) -> Receiver[EVChargerPoolStates]:
        """Return a receiver that streams ev charger states.

        Returns:
            A receiver that streams the states of all EV Chargers in the pool, every
                time the states of any of them change.
        """
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())
        return self._channel.new_receiver()

    async def stop(self) -> None:
        """Stop the status tracker."""
        if self._task:
            await cancel_and_await(self._task)
        if self._merged_stream:
            await self._merged_stream.stop()
