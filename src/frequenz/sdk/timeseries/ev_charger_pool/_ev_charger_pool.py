# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of EV Chargers."""

from __future__ import annotations

import logging
import uuid

from frequenz.channels import Sender

from ... import microgrid
from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid.component import ComponentCategory
from .._formula_engine import FormulaEnginePool, FormulaReceiver, FormulaReceiver3Phase
from .._formula_engine._formula_generators import (
    EVChargerCurrentFormula,
    EVChargerPowerFormula,
    FormulaGeneratorConfig,
)

logger = logging.getLogger(__name__)


class EVChargerPoolError(Exception):
    """An error that occurred in any of the EVChargerPool methods."""


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
                not specified, IDs of all EV Chargers in the microgrid will be fetched
                from the component graph.
        """
        self._channel_registry: ChannelRegistry = channel_registry
        self._resampler_subscription_sender: Sender[
            ComponentMetricRequest
        ] = resampler_subscription_sender
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
        self._namespace: str = f"ev-charger-pool-{uuid.uuid4()}"
        self._formula_pool: FormulaEnginePool = FormulaEnginePool(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
        )

    async def total_current(self) -> FormulaReceiver3Phase:
        """Fetch the total current for the EV Chargers in the pool.

        If a formula engine to calculate EV Charger current is not already running, it
        will be started.  Else, it will return a new receiver to the already existing
        data stream.

        Returns:
            A *new* receiver that will stream EV Charger current values.
        """
        return await self._formula_pool.from_generator(
            "ev_charger_total_current",
            EVChargerCurrentFormula,
            FormulaGeneratorConfig(component_ids=self._component_ids),
        )

    async def total_power(self) -> FormulaReceiver:
        """Fetch the total power for the EV Chargers in the pool.

        If a formula engine to calculate EV Charger power is not already running, it
        will be started.  Else, it will return a new receiver to the already existing
        data stream.

        Returns:
            A *new* receiver that will stream EV Charger power values.

        """
        return await self._formula_pool.from_generator(
            "ev_charger_total_power",
            EVChargerPowerFormula,
            FormulaGeneratorConfig(component_ids=self._component_ids),
        )

    async def current(self, component_id: int) -> FormulaReceiver3Phase:
        """Fetch the 3-phase current for the given EV Charger id.

        Args:
            component_id: id of the EV Charger to stream current values for.

        Returns:
            A *new* receiver that will stream 3-phase current values for the given
                EV Charger.

        Raises:
            EVChargerPoolError: if the given component_id is not part of the pool.
        """
        if component_id not in self._component_ids:
            raise EVChargerPoolError(
                f"{component_id=} is not part of the EVChargerPool"
                f" (with ids={self._component_ids})"
            )
        return await self._formula_pool.from_generator(
            f"ev_charger_current_{component_id}",
            EVChargerCurrentFormula,
            FormulaGeneratorConfig(component_ids={component_id}),
        )

    async def power(self, component_id: int) -> FormulaReceiver:
        """Fetch the power for the given EV Charger id.

        Args:
            component_id: id of the EV Charger to stream power values for.

        Returns:
            A *new* receiver that will stream power values for the given EV Charger.

        Raises:
            EVChargerPoolError: if the given component_id is not part of the pool.
        """
        if component_id not in self._component_ids:
            raise EVChargerPoolError(
                f"{component_id=} is not part of the EVChargerPool"
                f" (with ids={self._component_ids})"
            )
        return await self._formula_pool.from_generator(
            f"ev_charger_current_{component_id}",
            EVChargerPowerFormula,
            FormulaGeneratorConfig(component_ids={component_id}),
        )
