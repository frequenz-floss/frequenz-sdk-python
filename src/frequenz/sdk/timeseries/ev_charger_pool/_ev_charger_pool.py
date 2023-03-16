# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of EV Chargers."""

from __future__ import annotations

import asyncio
import logging
import uuid
from asyncio import Task
from dataclasses import dataclass

from frequenz.channels import Broadcast, ChannelClosedError, Receiver, Sender

from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid import connection_manager
from ...microgrid.component import ComponentCategory, ComponentMetricId
from .. import Sample, Sample3Phase
from .._formula_engine import FormulaEnginePool, FormulaReceiver, FormulaReceiver3Phase
from .._formula_engine._formula_generators import (
    EVChargerCurrentFormula,
    EVChargerPowerFormula,
    FormulaGeneratorConfig,
)
from ._state_tracker import EVChargerState, StateTracker

logger = logging.getLogger(__name__)


class EVChargerPoolError(Exception):
    """An error that occurred in any of the EVChargerPool methods."""


@dataclass(frozen=True)
class EVChargerData:
    """Data for an EV Charger, including the 3-phase current and the component state."""

    component_id: int
    current: Sample3Phase
    state: EVChargerState


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

    async def component_data(self, component_id: int) -> Receiver[EVChargerData]:
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
            logger.warning("Restarting component_status for id: %s", component_id)
        else:
            output_chan = Broadcast[EVChargerData](
                f"evpool-component_status-{component_id}"
            )

        task = asyncio.create_task(
            self._stream_component_data(component_id, output_chan.new_sender())
        )

        self._status_streams[component_id] = (task, output_chan)

        return output_chan.new_receiver()

    async def _get_current_streams(
        self, component_id: int
    ) -> tuple[Receiver[Sample], Receiver[Sample], Receiver[Sample]]:
        """Fetch current streams from the resampler for each phase.

        Args:
            component_id: id of EV Charger for which current streams are being fetched.

        Returns:
            A tuple of 3 receivers stream resampled current values for the given
                component id, one for each phase.
        """

        async def resampler_subscribe(metric_id: ComponentMetricId) -> Receiver[Sample]:
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
                logger.exception("Streams closed for component_id=%s.", component_id)
                raise

            sample = Sample3Phase(
                timestamp=phase_1.timestamp,
                value_p1=phase_1.value,
                value_p2=phase_2.value,
                value_p3=phase_3.value,
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
