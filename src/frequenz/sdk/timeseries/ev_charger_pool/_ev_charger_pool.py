# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of EV Chargers."""


import uuid
from collections import abc
from datetime import timedelta

from frequenz.channels import Broadcast, Receiver, Sender

from ..._internal._channels import ReceiverFetcher
from ...actor import ChannelRegistry, ComponentMetricRequest
from ...actor.power_distributing import ComponentPoolStatus
from ...microgrid import connection_manager
from ...microgrid.component import ComponentCategory
from .._base_types import SystemBounds
from .._quantities import Current, Power
from ..formula_engine import FormulaEngine, FormulaEngine3Phase
from ..formula_engine._formula_engine_pool import FormulaEnginePool
from ..formula_engine._formula_generators import (
    EVChargerCurrentFormula,
    EVChargerPowerFormula,
    FormulaGeneratorConfig,
)
from ._set_current_bounds import BoundsSetter, ComponentCurrentLimit
from ._system_bounds_tracker import EVCSystemBoundsTracker


class EVChargerPoolError(Exception):
    """An error that occurred in any of the EVChargerPool methods."""


class EVChargerPool:
    """An interface for interaction with pools of EV Chargers.

    !!! note
        `EVChargerPool` instances are not meant to be created directly by users. Use the
        [`microgrid.ev_charger_pool`][frequenz.sdk.microgrid.ev_charger_pool] method for
        creating `EVChargerPool` instances.

    Provides:
      - Aggregate [`power`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.power]
        and 3-phase
        [`current`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.current]
        measurements of the EV Chargers in the pool.
      - The
        [`component_data`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.component_data]
        method for fetching the 3-phase current and state of individual EV Chargers in
        the pool.
      - The
        [`set_bounds`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.set_bounds]
        method for limiting the max current of individual EV Chargers in the pool.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        status_receiver: Receiver[ComponentPoolStatus],
        component_ids: abc.Set[int] | None = None,
        repeat_interval: timedelta = timedelta(seconds=3.0),
    ) -> None:
        """Create an `EVChargerPool` instance.

        !!! note
            `EVChargerPool` instances are not meant to be created directly by users. Use
            the [`microgrid.ev_charger_pool`][frequenz.sdk.microgrid.ev_charger_pool]
            method for creating `EVChargerPool` instances.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
            status_receiver: A receiver that streams the status of the EV Chargers in
                the pool.
            component_ids: An optional list of component_ids belonging to this pool.  If
                not specified, IDs of all EV Chargers in the microgrid will be fetched
                from the component graph.
            repeat_interval: Interval after which to repeat the last set bounds to the
                microgrid API, if no new calls to `set_bounds` have been made.
        """
        self._channel_registry: ChannelRegistry = channel_registry
        self._repeat_interval: timedelta = repeat_interval
        self._resampler_subscription_sender: Sender[ComponentMetricRequest] = (
            resampler_subscription_sender
        )
        self._status_receiver: Receiver[ComponentPoolStatus] = status_receiver
        self._component_ids: abc.Set[int] = set()
        if component_ids is not None:
            self._component_ids = component_ids
        else:
            graph = connection_manager.get().component_graph
            self._component_ids = {
                evc.component_id
                for evc in graph.components(
                    component_categories={ComponentCategory.EV_CHARGER}
                )
            }
        self._namespace: str = f"ev-charger-pool-{uuid.uuid4()}"
        self._formula_pool: FormulaEnginePool = FormulaEnginePool(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
        )
        self._bounds_setter: BoundsSetter | None = None

        self._bounds_channel: Broadcast[SystemBounds] = Broadcast(
            name=f"System Bounds for EV Chargers: {component_ids}"
        )
        self._bounds_tracker: EVCSystemBoundsTracker = EVCSystemBoundsTracker(
            self.component_ids, self._status_receiver, self._bounds_channel.new_sender()
        )
        self._bounds_tracker.start()

    @property
    def component_ids(self) -> abc.Set[int]:
        """Return component IDs of all EV Chargers managed by this EVChargerPool.

        Returns:
            Set of managed component IDs.
        """
        return self._component_ids

    @property
    def current(self) -> FormulaEngine3Phase[Current]:
        """Fetch the total current for the EV Chargers in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate EV Charger current is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the total current of all EV
                Chargers.
        """
        engine = self._formula_pool.from_3_phase_current_formula_generator(
            "ev_charger_total_current",
            EVChargerCurrentFormula,
            FormulaGeneratorConfig(component_ids=self._component_ids),
        )
        assert isinstance(engine, FormulaEngine3Phase)
        return engine

    @property
    def power(self) -> FormulaEngine[Power]:
        """Fetch the total power for the EV Chargers in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate EV Charger power is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the total power of all EV
                Chargers.
        """
        engine = self._formula_pool.from_power_formula_generator(
            "ev_charger_power",
            EVChargerPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._component_ids,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    async def set_bounds(self, component_id: int, max_current: Current) -> None:
        """Send given max current bound for the given EV Charger to the microgrid API.

        Bounds are used to limit the max current drawn by an EV, although the exact
        value will be determined by the EV.

        Args:
            component_id: ID of EV Charger to set the current bounds to.
            max_current: maximum current that an EV can draw from this EV Charger.
        """
        if not self._bounds_setter:
            self._bounds_setter = BoundsSetter(self._repeat_interval)
        await self._bounds_setter.set(component_id, max_current.as_amperes())

    def new_bounds_sender(self) -> Sender[ComponentCurrentLimit]:
        """Return a `Sender` for setting EV Charger current bounds with.

        Bounds are used to limit the max current drawn by an EV, although the exact
        value will be determined by the EV.

        Returns:
            A new `Sender`.
        """
        if not self._bounds_setter:
            self._bounds_setter = BoundsSetter(self._repeat_interval)
        return self._bounds_setter.new_bounds_sender()

    async def stop(self) -> None:
        """Stop all tasks and channels owned by the EVChargerPool."""
        if self._bounds_setter:
            await self._bounds_setter.stop()
        await self._formula_pool.stop()

    @property
    def _system_power_bounds(self) -> ReceiverFetcher[SystemBounds]:
        """Return a receiver for the system power bounds."""
        return self._bounds_channel
