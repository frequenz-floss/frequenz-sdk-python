# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Fetches the Grid Frequency."""

from __future__ import annotations

import asyncio
import logging

from frequenz.channels import Receiver, Sender
from frequenz.client.microgrid import Component, ComponentCategory, ComponentMetricId
from frequenz.quantities import Frequency, Quantity

from .._internal._channels import ChannelRegistry
from ..microgrid import connection_manager
from ..microgrid._data_sourcing import ComponentMetricRequest
from ..timeseries._base_types import Sample

_logger = logging.getLogger(__name__)


def create_request(component_id: int) -> ComponentMetricRequest:
    """Create a request for grid frequency.

    Args:
        component_id: The component id to use for the request.

    Returns:
        A component metric request for grid frequency.
    """
    return ComponentMetricRequest(
        "grid-frequency", component_id, ComponentMetricId.FREQUENCY, None
    )


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
            source = component_graph.find_first_descendant_component(
                root_category=ComponentCategory.GRID,
                descendant_categories=(
                    ComponentCategory.METER,
                    ComponentCategory.INVERTER,
                    ComponentCategory.EV_CHARGER,
                ),
            )

        self._request_sender: Sender[ComponentMetricRequest] = (
            data_sourcing_request_sender
        )
        self._channel_registry: ChannelRegistry = channel_registry
        self._source_component: Component = source
        self._component_metric_request: ComponentMetricRequest = create_request(
            self._source_component.component_id
        )

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
        receiver = self._channel_registry.get_or_create(
            Sample[Quantity], self._component_metric_request.get_channel_name()
        ).new_receiver()

        if not self._task:
            self._task = asyncio.create_task(self._send_request())
        else:
            _logger.info(
                "Grid frequency request already sent: %s", self._source_component
            )

        return receiver.map(
            lambda sample: (
                Sample[Frequency](sample.timestamp, None)
                if sample.value is None or sample.value.isnan()
                else Sample(
                    sample.timestamp, Frequency.from_hertz(sample.value.base_value)
                )
            )
        )

    async def _send_request(self) -> None:
        """Send the request for grid frequency."""
        await self._request_sender.send(self._component_metric_request)
        _logger.debug("Sent request for grid frequency: %s", self._source_component)
