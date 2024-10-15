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
from frequenz.client.microgrid import Component, ComponentCategory, ComponentMetricId
from frequenz.quantities import Quantity, Voltage

from .._internal._channels import ChannelRegistry
from ..timeseries._base_types import Sample, Sample3Phase

if TYPE_CHECKING:
    # Imported here to avoid a circular import.
    from ..microgrid._data_sourcing import ComponentMetricRequest

_logger = logging.getLogger(__name__)


class VoltageStreamer:
    """Fetch and stream the phase-to-neutral voltage from a source component.

    Example:
        ```python
        from datetime import timedelta

        from frequenz.sdk import microgrid
        from frequenz.sdk.timeseries import ResamplerConfig

        await microgrid.initialize(
            "grpc://127.0.0.1:50051",
            ResamplerConfig(resampling_period=timedelta(seconds=1))
        )

        # Get a receiver for the phase-to-neutral voltage.
        voltage_recv = microgrid.voltage_per_phase().new_receiver()

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

        from ..microgrid import (  # pylint: disable=import-outside-toplevel
            connection_manager,
        )

        if not source_component:
            component_graph = connection_manager.get().component_graph
            source_component = component_graph.find_first_descendant_component(
                root_category=ComponentCategory.GRID,
                descendant_categories=[
                    ComponentCategory.METER,
                    ComponentCategory.INVERTER,
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
        receiver = self._channel_registry.get_or_create(
            Sample3Phase[Voltage], self._channel_key
        ).new_receiver()

        if not self._task:
            self._task = asyncio.create_task(self._send_request())
        else:
            _logger.info("Voltage request already sent: %s", self._source_component)

        return receiver

    async def _send_request(self) -> None:
        """Send the request to fetch each voltage phase and stream 3-phase voltage."""
        # Imported here to avoid a circular import.
        from ..microgrid._data_sourcing import (  # pylint: disable=import-outside-toplevel
            ComponentMetricRequest,
        )

        metric_ids = (
            ComponentMetricId.VOLTAGE_PHASE_1,
            ComponentMetricId.VOLTAGE_PHASE_2,
            ComponentMetricId.VOLTAGE_PHASE_3,
        )
        phases_rx: list[Receiver[Sample[Quantity]]] = []
        for metric_id in metric_ids:
            req = ComponentMetricRequest(
                self._namespace, self._source_component.component_id, metric_id, None
            )

            await self._resampler_subscription_sender.send(req)

            phases_rx.append(
                self._channel_registry.get_or_create(
                    Sample[Quantity], req.get_channel_name()
                ).new_receiver()
            )

        sender = self._channel_registry.get_or_create(
            Sample3Phase[Voltage], self._channel_key
        ).new_sender()

        _logger.debug(
            "Sent request for fetching voltage from: %s", self._source_component
        )

        while True:
            try:
                phases = [await r.receive() for r in phases_rx]

                if not all(map(lambda p: p is not None, phases)):
                    _logger.warning(
                        "Received None from voltage request: %s %s",
                        self._source_component,
                        phases,
                    )
                    continue

                msg = Sample3Phase(
                    phases[0].timestamp,
                    *map(
                        lambda p: (
                            Voltage.from_volts(p.value.base_value) if p.value else None
                        ),
                        phases,
                    ),
                )
            except asyncio.CancelledError:
                _logger.exception(
                    "Phase-to-neutral 3-phase voltage streaming task cancelled: %s",
                    self._source_component,
                )
                break
            else:
                await sender.send(msg)
