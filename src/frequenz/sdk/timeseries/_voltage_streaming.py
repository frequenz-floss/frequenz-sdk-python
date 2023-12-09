# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Fetch and stream the phase-to-neutral voltage from a source component.

Each phase of the phase-to-neutral voltage is fetched from the source
component individually and then streamed as a 3-phase sample.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from frequenz.channels import Receiver, Sender

from ..actor import ChannelRegistry
from ..microgrid import connection_manager
from ..microgrid.component import Component, ComponentCategory, ComponentMetricId
from ..timeseries._base_types import Sample3Phase
from ..timeseries._quantities import Voltage

if TYPE_CHECKING:
    # Imported here to avoid a circular import.
    from ..actor import ComponentMetricRequest

_logger = logging.getLogger(__name__)


class VoltageStreaming:
    """Fetch and stream the phase-to-neutral voltage from a source component.

    Example:
        ```python
        from datetime import timedelta

        from frequenz.sdk import microgrid
        from frequenz.sdk.timeseries import ResamplerConfig

        await microgrid.initialize(
            "127.0.0.1",
            50051,
            ResamplerConfig(resampling_period=timedelta(seconds=1))
        )

        # Get a receiver for the phase-to-neutral voltage.
        voltage_recv = microgrid.voltage().new_receiver()

        async for voltage_sample in voltage_recv:
            print(voltage_sample)
        ```
    """

    def __init__(
        self,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        channel_registry: ChannelRegistry,
        source_component: Component | None = None,
    ):
        """Initialize the phase-to-neutral voltage streaming.

        Args:
            resampler_subscription_sender: The sender for sending metric
                requests to the resampling actor.
            channel_registry: The channel registry for the phase-to-neutral
                voltage streaming.
            source_component: The source component to receive the
                phase-to-neutral voltage. If None, it fetches the source
                component from the connection manager.

        """
        self._resampler_subscription_sender = resampler_subscription_sender
        """The sender for sending metric requests to the resampling actor."""

        self._channel_registry = channel_registry
        """The channel registry for the phase-to-neutral voltage streaming."""

        if not source_component:
            component_graph = connection_manager.get().component_graph
            source_component = component_graph.find_first_descendant_component(
                root_category=ComponentCategory.GRID,
                descendant_categories=[
                    ComponentCategory.METER,
                    ComponentCategory.EV_CHARGER,
                ],
            )

        self._source_component = source_component
        """The source component to receive the phase-to-neutral voltage."""

        self._task: None | asyncio.Task[None] = None
        """The task for fetching and stream the phase-to-neutral voltage."""

        self._namespace = "microgrid-voltage"
        """The namespace for the phase-to-neutral voltage streaming."""

        self._channel_key = f"{self._namespace}-all-phases"
        """The channel key for the phase-to-neutral voltage streaming."""

    @property
    def source(self) -> Component:
        """Get the component to fetch the phase-to-neutral voltage from.

        Returns:
            The component to fetch the phase-to-neutral voltage from.
        """
        return self._source_component

    def new_receiver(self) -> Receiver[Sample3Phase[Voltage]]:
        """Create a receiver for the phase-to-neutral voltage.

        Each phase of the phase-to-neutral voltage is fetched from the source
        component individually and then streamed as a 3-phase sample.

        Returns:
            A receiver that will receive the phase-to-neutral voltage as a
            3-phase sample.
        """
        receiver = self._channel_registry.new_receiver(self._channel_key)

        if not self._task:
            self._task = asyncio.create_task(self._send_request())
        else:
            _logger.info("Voltage request already sent: %s", self._source_component)

        return receiver

    async def _send_request(self) -> None:
        """Send the request to fetch each voltage phase and stream 3-phase voltage."""
        # Imported here to avoid a circular import.
        from ..actor import (  # pylint: disable=import-outside-toplevel
            ComponentMetricRequest,
        )

        def _create_request(phase: ComponentMetricId) -> ComponentMetricRequest:
            return ComponentMetricRequest(
                self._namespace,
                self._source_component.component_id,
                phase,
                None,
            )

        phase_1_req = _create_request(ComponentMetricId.VOLTAGE_PHASE_1)
        phase_2_req = _create_request(ComponentMetricId.VOLTAGE_PHASE_2)
        phase_3_req = _create_request(ComponentMetricId.VOLTAGE_PHASE_3)

        await self._resampler_subscription_sender.send(phase_1_req)
        await self._resampler_subscription_sender.send(phase_2_req)
        await self._resampler_subscription_sender.send(phase_3_req)

        phase_1_rx = self._channel_registry.new_receiver(phase_1_req.get_channel_name())
        phase_2_rx = self._channel_registry.new_receiver(phase_2_req.get_channel_name())
        phase_3_rx = self._channel_registry.new_receiver(phase_3_req.get_channel_name())

        sender = self._channel_registry.new_sender(self._channel_key)

        _logger.debug(
            "Sent request for fetching voltage from: %s", self._source_component
        )

        while True:
            try:
                phase_1 = await phase_1_rx.receive()
                phase_2 = await phase_2_rx.receive()
                phase_3 = await phase_3_rx.receive()

                if phase_1 is None or phase_2 is None or phase_3 is None:
                    _logger.warning(
                        "Received None from voltage request: %s (%s, %s, %s)",
                        self._source_component,
                        phase_1,
                        phase_2,
                        phase_3,
                    )
                    continue

                msg = Sample3Phase(
                    phase_1.timestamp,
                    Voltage.from_volts(phase_1.value.base_value),
                    Voltage.from_volts(phase_2.value.base_value),
                    Voltage.from_volts(phase_3.value.base_value),
                )
            except asyncio.CancelledError:
                _logger.exception(
                    "Phase-to-neutral 3-phase voltage streaming task cancelled: %s",
                    self._source_component,
                )
                break
            else:
                await sender.send(msg)
