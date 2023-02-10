# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of ev chargers."""

from __future__ import annotations

import logging
import uuid

from frequenz.channels import Receiver, Sender

from ... import microgrid
from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid.component import ComponentCategory
from .._formula_engine import FormulaEnginePool, FormulaReceiver, FormulaReceiver3Phase
from .._formula_engine._formula_generators import (
    EVChargerCurrentFormula,
    EVChargerPowerFormula,
)
from ._state_tracker import EVChargerPoolStates, StateTracker

logger = logging.getLogger(__name__)


class EVChargerPool:
    """Interactions with EV Chargers."""

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        component_ids: set[int] | None = None,
    ) -> None:
        """Create an `EVChargerPool` instance.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
            component_ids: An optional list of component_ids belonging to this pool.  If
                not specified, IDs of all ev chargers in the microgrid will be fetched
                from the component graph.
        """
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender
        self._component_ids: set[int] = set()
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
        self._namespace = f"ev-charger-pool-{uuid.uuid4()}"
        self._formula_pool = FormulaEnginePool(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
        )

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

    async def total_current(self) -> FormulaReceiver3Phase:
        """Fetch the total current for the ev chargers in the pool.

        If a formula engine to calculate ev charger current is not already
        running, it will be started.  Else, we'll just get a new receiver to the
        already existing data stream.

        Returns:
            A *new* receiver that will stream ev_charger current values.
        """
        return await self._formula_pool.from_generator(
            "ev_charger_total_current", EVChargerCurrentFormula
        )

    async def total_power(self) -> FormulaReceiver:
        """Fetch the total power for the ev chargers in the pool.

        If a formula engine to calculate EV charger power is not already
        running, it will be started. Else, we'll just get a new receiver
        to the already existing data stream.

        Returns:
            A *new* receiver that will stream ev_charger power values.
        """
        return await self._formula_pool.from_generator(
            "ev_charger_total_power", EVChargerPowerFormula
        )
