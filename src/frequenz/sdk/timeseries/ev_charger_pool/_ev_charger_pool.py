# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of ev chargers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from frequenz.channels import Broadcast, Receiver
from frequenz.channels.util import Merge

from ... import microgrid
from ..._internal.asyncio import cancel_and_await
from ...microgrid.component import (
    ComponentCategory,
    EVChargerCableState,
    EVChargerComponentState,
    EVChargerData,
)

logger = logging.getLogger(__name__)


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
    """States of all ev chargers in the pool."""

    _states: dict[int, EVChargerState]
    _changed_component: Optional[int] = None

    def __iter__(self) -> Iterator[tuple[int, EVChargerState]]:
        """Iterate over states of all ev chargers.

        Returns:
            An iterator over all ev charger states.
        """
        return iter(self._states.items())

    def latest_change(self) -> Optional[tuple[int, EVChargerState]]:
        """Return the most recent ev charger state change.

        Returns:
            A tuple with the component ID of an ev charger that just had a state
                change, and its new state.
        """
        if self._changed_component is None:
            return None
        return (
            self._changed_component,
            self._states.setdefault(
                self._changed_component, EVChargerState.UNSPECIFIED
            ),
        )


class _StateTracker:
    """A class for keeping track of the states of all ev chargers in a pool."""

    def __init__(self, comp_states: dict[int, EVChargerState]) -> None:
        """Create a `_StateTracker` instance.

        Args:
            comp_states: initial states of all ev chargers in the pool.
        """
        self._states = comp_states

    def get(self) -> EVChargerPoolStates:
        """Get a representation of the current states of all ev chargers.

        Returns:
            An `EVChargerPoolStates` instance.
        """
        return EVChargerPoolStates(self._states)

    def update(
        self,
        data: EVChargerData,
    ) -> Optional[EVChargerPoolStates]:
        """Update the state of an ev charger, from a new data point.

        Args:
            data: component data from the microgrid, for an ev charger in the pool.

        Returns:
            A new `EVChargerPoolStates` instance representing all the ev chargers in
                the pool, in case there has been a state change for any of the ev
                chargers, or `None` otherwise.
        """
        evc_id = data.component_id
        new_state = EVChargerState.from_ev_charger_data(data)
        if evc_id not in self._states or self._states[evc_id] != new_state:
            self._states[evc_id] = new_state
            return EVChargerPoolStates(self._states, evc_id)
        return None


class EVChargerPool:
    """Interactions with EV Chargers."""

    def __init__(
        self,
        component_ids: Optional[set[int]] = None,
    ) -> None:
        """Create an `EVChargerPool` instance.

        Args:
            component_ids: An optional list of component_ids belonging to this pool.  If
                not specified, IDs of all ev chargers in the microgrid will be fetched
                from the component graph.
        """
        self._component_ids = set()
        if component_ids is not None:
            self._component_ids = component_ids
        else:
            graph = microgrid.get().component_graph
            self._component_ids = {
                evc.component_id
                for evc in graph.components(
                    component_category={ComponentCategory.EV_CHARGER}
                )
            }
        self._channel = Broadcast[EVChargerPoolStates](
            "EVCharger States", resend_latest=True
        )
        self._task: Optional[asyncio.Task[None]] = None
        self._merged_stream: Optional[Merge] = None

    async def _run(self) -> None:
        logger.debug("Starting EVChargerPool for components: %s", self._component_ids)
        api_client = microgrid.get().api_client
        streams: list[Receiver[EVChargerData]] = await asyncio.gather(
            *[api_client.ev_charger_data(cid) for cid in self._component_ids]
        )

        latest_messages: list[EVChargerData] = await asyncio.gather(
            *[stream.receive() for stream in streams]
        )
        states = {
            msg.component_id: EVChargerState.from_ev_charger_data(msg)
            for msg in latest_messages
        }
        state_tracker = _StateTracker(states)
        self._merged_stream = Merge(*streams)
        sender = self._channel.new_sender()
        await sender.send(state_tracker.get())
        async for data in self._merged_stream:
            if updated_states := state_tracker.update(data):
                await sender.send(updated_states)

    async def _stop(self) -> None:
        if self._task:
            await cancel_and_await(self._task)
        if self._merged_stream:
            await self._merged_stream.stop()

    def states(self) -> Receiver[EVChargerPoolStates]:
        """Return a receiver that streams ev charger states.

        Returns:
            A receiver that streams the states of all ev chargers in the pool, every
                time the states of any of them change.
        """
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())
        return self._channel.new_receiver()
