# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of ev chargers."""

from __future__ import annotations

import logging

from frequenz.channels import Receiver

from ... import microgrid
from ...microgrid.component import ComponentCategory
from ._state_tracker import EVChargerPoolStates, StateTracker

logger = logging.getLogger(__name__)


class EVChargerPool:
    """Interactions with EV Chargers."""

    def __init__(
        self,
        component_ids: set[int] | None = None,
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
        self._state_tracker: StateTracker | None = None

    async def _stop(self) -> None:
        if self._state_tracker:
            await self._state_tracker.stop()

    def states(self) -> Receiver[EVChargerPoolStates]:
        """Return a receiver that streams ev charger states.

        Returns:
            A receiver that streams the states of all ev chargers in the pool, every
                time the states of any of them change.
        """
        if not self._state_tracker:
            self._state_tracker = StateTracker(self._component_ids)
        return self._state_tracker.new_receiver()
