# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Fetches the Grid Frequency."""

from __future__ import annotations

import asyncio
import logging

from frequenz.channels import Broadcast, Receiver, Sender, make_oneshot
from frequenz.channels.experimental import Pipe
from frequenz.client.microgrid.component import Component, EvCharger, Inverter, Meter
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Frequency, Quantity

from .._internal._channels import ChannelRegistry
from .._internal._graph_traversal import find_first_descendant_component
from ..microgrid import connection_manager
from ..microgrid._data_sourcing import ComponentMetricRequest
from ..timeseries._base_types import Sample

_logger = logging.getLogger(__name__)


class GridFrequency:
    """Grid Frequency."""

    def __init__(
        self,
        data_sourcing_request_sender: Sender[ComponentMetricRequest],
        channel_registry: ChannelRegistry,
        source: Component | None = None,
    ):
        """Initialize the grid frequency formula generator.

        Args:
            data_sourcing_request_sender: The sender to use for requests.
            channel_registry: The channel registry to use for the grid frequency.
            source: The source component to use to receive the grid frequency.
        """
        if not source:
            component_graph = connection_manager.get().component_graph
            source = find_first_descendant_component(
                component_graph,
                descendants=[Meter, Inverter, EvCharger],
            )

        self._request_sender: Sender[ComponentMetricRequest] = (
            data_sourcing_request_sender
        )
        self._channel_registry: ChannelRegistry = channel_registry
        self._source_component: Component = source

        # Microgrid API source will send the stream through a oneshot channel
        telem_stream_sender, self._telem_stream_receiver = make_oneshot(
            Receiver[Sample[Quantity]]  # type: ignore[type-abstract]
        )

        self._component_metric_request = ComponentMetricRequest(
            "grid-frequency",
            self._source_component.id,
            Metric.AC_FREQUENCY,
            None,
            telem_stream_sender,
        )

        # This channel merely forwards the telemetry stream. It is needed
        # because we must return a receiver synchronously in new_receiver.
        # The "real" channel for telemetry must be created in and owned by
        # MicrogridApiSource, otherwise streams would not be reused.
        self._forwarding_channel: Broadcast[Sample[Quantity]] | None = None

        # Sadly needed for testing
        self._task: None | asyncio.Task[None] = None

    @property
    def source(self) -> Component:
        """The component that is used to fetch the grid frequency.

        Returns:
            The component that is used for grid frequency.
        """
        return self._source_component

    def new_receiver(self) -> Receiver[Sample[Frequency]]:
        """Create a receiver for grid frequency.

        Returns:
            A receiver that will receive grid frequency samples.
        """
        if self._forwarding_channel is None:
            self._forwarding_channel = Broadcast(name="Forward frequency samples")
            self._task = asyncio.create_task(
                self._send_request(self._forwarding_channel.new_sender())
            )

        return self._forwarding_channel.new_receiver().map(
            lambda sample: (
                Sample[Frequency](sample.timestamp, None)
                if sample.value is None or sample.value.isnan()
                else Sample(
                    sample.timestamp, Frequency.from_hertz(sample.value.base_value)
                )
            )
        )

    async def _send_request(self, forwarding_sender: Sender[Sample[Quantity]]) -> None:
        """Send the request for grid frequency."""
        await self._request_sender.send(self._component_metric_request)
        _logger.debug("Sent request for grid frequency: %s", self._source_component)

        # Receive the telemetry stream and forward it via pipe
        telem_receiver: Receiver[Sample[Quantity]] = (
            await self._telem_stream_receiver.receive()
        )
        await Pipe(telem_receiver, forwarding_sender).start()
