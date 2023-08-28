# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of EV Chargers."""

from __future__ import annotations

import asyncio
import logging
import uuid
from asyncio import Task
from collections import abc
from dataclasses import dataclass
from datetime import timedelta

from frequenz.channels import Broadcast, ChannelClosedError, Receiver, Sender

from ..._internal._asyncio import cancel_and_await
from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid import connection_manager
from ...microgrid.component import ComponentCategory, ComponentMetricId
from .. import Sample, Sample3Phase
from .._formula_engine import FormulaEngine, FormulaEngine3Phase, FormulaEnginePool
from .._formula_engine._formula_generators import (
    EVChargerCurrentFormula,
    EVChargerPowerFormula,
    FormulaGeneratorConfig,
    FormulaType,
)
from .._quantities import Current, Power, Quantity
from ._set_current_bounds import BoundsSetter, ComponentCurrentLimit
from ._state_tracker import EVChargerState, StateTracker

_logger = logging.getLogger(__name__)


class EVChargerPoolError(Exception):
    """An error that occurred in any of the EVChargerPool methods."""


@dataclass(frozen=True)
class EVChargerData:
    """Data for an EV Charger, including the 3-phase current and the component state."""

    component_id: int
    current: Sample3Phase[Current]
    state: EVChargerState


class EVChargerPool:
    """Interactions with EV Chargers."""

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        component_ids: set[int] | None = None,
        repeat_interval: timedelta = timedelta(seconds=3.0),
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
            repeat_interval: Interval after which to repeat the last set bounds to the
                microgrid API, if no new calls to `set_bounds` have been made.
        """
        self._channel_registry: ChannelRegistry = channel_registry
        self._repeat_interval = repeat_interval
        self._resampler_subscription_sender: Sender[
            ComponentMetricRequest
        ] = resampler_subscription_sender
        self._component_ids: set[int] = set()
        if component_ids is not None:
            self._component_ids = component_ids
        else:
            graph = connection_manager.get().component_graph
            self._component_ids = {
                evc.component_id
                for evc in graph.components(
                    component_category={ComponentCategory.EV_CHARGER}
                )
            }
        self._state_tracker: StateTracker | None = None
        self._status_streams: dict[
            int, tuple[Task[None], Broadcast[EVChargerData]]
        ] = {}
        self._namespace: str = f"ev-charger-pool-{uuid.uuid4()}"
        self._formula_pool: FormulaEnginePool = FormulaEnginePool(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
        )
        self._bounds_setter: BoundsSetter | None = None

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
                formula_type=FormulaType.PASSIVE_SIGN_CONVENTION,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def production_power(self) -> FormulaEngine[Power]:
        """Fetch the total power produced by the EV Chargers in the pool.

        This formula produces positive values when producing power and 0 otherwise.

        If a formula engine to calculate EV Charger power is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the production power of all
                EV Chargers.
        """
        engine = self._formula_pool.from_power_formula_generator(
            "ev_charger_production_power",
            EVChargerPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._component_ids,
                formula_type=FormulaType.PRODUCTION,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def consumption_power(self) -> FormulaEngine[Power]:
        """Fetch the total power consumed by the EV Chargers in the pool.

        This formula produces positive values when consuming power and 0 otherwise.

        If a formula engine to calculate EV Charger power is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the consumption power of all
                EV Chargers.
        """
        engine = self._formula_pool.from_power_formula_generator(
            "ev_charger_consumption_power",
            EVChargerPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._component_ids,
                formula_type=FormulaType.CONSUMPTION,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    def component_data(self, component_id: int) -> Receiver[EVChargerData]:
        """Stream 3-phase current values and state of an EV Charger.

        Args:
            component_id: id of the EV Charger for which data is requested.

        Returns:
            A receiver that streams objects containing 3-phase current and state of
                an EV Charger.
        """
        if recv := self._status_streams.get(component_id, None):
            task, output_chan = recv
            if not task.done():
                return output_chan.new_receiver()
            _logger.warning("Restarting component_status for id: %s", component_id)
        else:
            output_chan = Broadcast[EVChargerData](
                f"evpool-component_status-{component_id}"
            )

        task = asyncio.create_task(
            self._stream_component_data(component_id, output_chan.new_sender())
        )

        self._status_streams[component_id] = (task, output_chan)

        return output_chan.new_receiver()

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
        if self._state_tracker:
            await self._state_tracker.stop()
        await self._formula_pool.stop()
        for stream in self._status_streams.values():
            task, chan = stream
            await chan.close()
            await cancel_and_await(task)

    async def _get_current_streams(
        self, component_id: int
    ) -> tuple[
        Receiver[Sample[Quantity]],
        Receiver[Sample[Quantity]],
        Receiver[Sample[Quantity]],
    ]:
        """Fetch current streams from the resampler for each phase.

        Args:
            component_id: id of EV Charger for which current streams are being fetched.

        Returns:
            A tuple of 3 receivers stream resampled current values for the given
                component id, one for each phase.
        """

        async def resampler_subscribe(
            metric_id: ComponentMetricId,
        ) -> Receiver[Sample[Quantity]]:
            request = ComponentMetricRequest(
                namespace="ev-pool",
                component_id=component_id,
                metric_id=metric_id,
                start_time=None,
            )
            await self._resampler_subscription_sender.send(request)
            return self._channel_registry.new_receiver(request.get_channel_name())

        return (
            await resampler_subscribe(ComponentMetricId.CURRENT_PHASE_1),
            await resampler_subscribe(ComponentMetricId.CURRENT_PHASE_2),
            await resampler_subscribe(ComponentMetricId.CURRENT_PHASE_3),
        )

    async def _stream_component_data(
        self,
        component_id: int,
        sender: Sender[EVChargerData],
    ) -> None:
        """Stream 3-phase current values and state of an EV Charger.

        Args:
            component_id: id of the EV Charger for which data is requested.
            sender: A sender to stream EV Charger data to.

        Raises:
            ChannelClosedError: If the channels from the resampler are closed.
        """
        if not self._state_tracker:
            self._state_tracker = StateTracker(self._component_ids)

        (phase_1_rx, phase_2_rx, phase_3_rx) = await self._get_current_streams(
            component_id
        )
        while True:
            try:
                (phase_1, phase_2, phase_3) = (
                    await phase_1_rx.receive(),
                    await phase_2_rx.receive(),
                    await phase_3_rx.receive(),
                )
            except ChannelClosedError:
                _logger.exception("Streams closed for component_id=%s.", component_id)
                raise

            sample = Sample3Phase(
                timestamp=phase_1.timestamp,
                value_p1=None
                if phase_1.value is None
                else Current.from_amperes(phase_1.value.base_value),
                value_p2=None
                if phase_2.value is None
                else Current.from_amperes(phase_2.value.base_value),
                value_p3=None
                if phase_3.value is None
                else Current.from_amperes(phase_3.value.base_value),
            )

            if (
                phase_1.value is None
                and phase_2.value is None
                and phase_3.value is None
            ):
                state = EVChargerState.MISSING
            else:
                state = self._state_tracker.get(component_id)

            await sender.send(
                EVChargerData(
                    component_id=component_id,
                    current=sample,
                    state=state,
                )
            )
