# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Fetches the Grid Frequency."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from frequenz.channels import Receiver, Sender

from ..actor import ChannelRegistry
from ..microgrid import connection_manager
from ..microgrid.component import Component, ComponentCategory, ComponentMetricId
from ..timeseries._base_types import Sample
from ..timeseries._quantities import Frequency, Quantity

if TYPE_CHECKING:
    # Imported here to avoid a circular import.
    from ..actor import ComponentMetricRequest

_logger = logging.getLogger(__name__)


def create_request(component_id: int) -> ComponentMetricRequest:
    """Create a request for grid frequency.

    Args:
        component_id: The component id to use for the request.

    Returns:
        A component metric request for grid frequency.
    """
    # Imported here to avoid a circular import.
    # pylint: disable=import-outside-toplevel
    from ..actor import ComponentMetricRequest

    return ComponentMetricRequest(
        "grid-frequency", component_id, ComponentMetricId.FREQUENCY, None
    )


class GridFrequency:
    """Grid Frequency."""

    def __init__(
        self,
        data_sourcing_request_sender: Sender[ComponentMetricRequest],
        channel_registry: ChannelRegistry[Sample[Quantity]],
        source: Component | None = None,
    ):
        """Initialize the grid frequency formula generator.

        Args:
            data_sourcing_request_sender: The sender to use for requests.
            channel_registry: The channel registry to use for the grid frequency.
            source: The source component to use to receive the grid frequency.
        """
        self._request_sender: Sender[
            ComponentMetricRequest
        ] = data_sourcing_request_sender
        self._channel_registry: ChannelRegistry[Sample[Quantity]] = channel_registry
        self._source_component: Component = source or self.find_frequency_source()
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
        receiver = self._channel_registry.new_receiver(
            self._component_metric_request.get_channel_name()
        )

        if not self._task:
            self._task = asyncio.create_task(self._send_request())
        else:
            _logger.info(
                "Grid frequency request already sent: %s", self._source_component
            )

        return receiver.map(
            lambda sample: Sample(
                sample.timestamp, Frequency.from_hertz(sample.value.base_value)
            )
        )

    async def _send_request(self) -> None:
        """Send the request for grid frequency."""
        await self._request_sender.send(self._component_metric_request)
        _logger.debug("Sent request for grid frequency: %s", self._source_component)

    @staticmethod
    def find_frequency_source() -> Component:
        """Find the source component that will be used for grid frequency.

        Will use the first meter it can find to gather the frequency.
        If no meter is available, the first inverter will be used and finally the first EV charger.

        Returns:
            The component that will be used for grid frequency.

        Raises:
            ValueError: when the component graph doesn't have a `GRID` component.
        """
        component_graph = connection_manager.get().component_graph
        grid_component = next(
            (
                comp
                for comp in component_graph.components()
                if comp.category == ComponentCategory.GRID
            ),
            None,
        )

        if grid_component is None:
            raise ValueError(
                "Unable to find a GRID component from the component graph."
            )

        # Sort by component id to ensure consistent results
        grid_successors = sorted(
            component_graph.successors(grid_component.component_id),
            key=lambda comp: comp.component_id,
        )

        def find_component(component_category: ComponentCategory) -> Component | None:
            return next(
                (
                    comp
                    for comp in grid_successors
                    if comp.category == component_category
                ),
                None,
            )

        # Find the first component that is either a meter, inverter or EV charger
        # with category priority in that order.
        component = next(
            filter(
                None,
                map(
                    find_component,
                    [
                        ComponentCategory.METER,
                        ComponentCategory.INVERTER,
                        ComponentCategory.EV_CHARGER,
                    ],
                ),
            ),
            None,
        )

        if component is None:
            raise ValueError(
                "Unable to find a METER, INVERTER or EV_CHARGER component from the component graph."
            )

        return component
