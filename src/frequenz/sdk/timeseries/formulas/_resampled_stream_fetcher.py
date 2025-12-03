# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Fetches telemetry streams for components."""

from frequenz.channels import Receiver, Sender
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Quantity

from ..._internal._channels import ChannelRegistry
from ...microgrid._data_sourcing import ComponentMetricRequest, Metric
from ...microgrid._old_component_data import TransitionalMetric
from .. import Sample


class ResampledStreamFetcher:
    """Fetches telemetry streams for components."""

    def __init__(
        self,
        namespace: str,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        metric: Metric | TransitionalMetric,
    ):
        """Initialize this instance.

        Args:
            namespace: The unique namespace to allow reuse of streams in the data
                pipeline.
            channel_registry: The channel registry instance shared with the resampling
                and the data sourcing actors.
            resampler_subscription_sender: A sender to send metric requests to the
                resampling actor.
            metric: The metric to fetch for all components in this formula.
        """
        self._namespace: str = namespace
        self._channel_registry: ChannelRegistry = channel_registry
        self._resampler_subscription_sender: Sender[ComponentMetricRequest] = (
            resampler_subscription_sender
        )
        self._metric: Metric | TransitionalMetric = metric

        self._pending_requests: list[ComponentMetricRequest] = []

    def fetch_stream(
        self,
        component_id: ComponentId,
    ) -> Receiver[Sample[Quantity]]:
        """Get a receiver with the resampled data for the given component id.

        Args:
            component_id: The component id for which to get a resampled data stream.

        Returns:
            A receiver to stream resampled data for the given component id.
        """
        request = ComponentMetricRequest(
            self._namespace,
            component_id,
            self._metric,
            None,
        )
        self._pending_requests.append(request)
        return self._channel_registry.get_or_create(
            Sample[Quantity], request.get_channel_name()
        ).new_receiver()

    async def subscribe(self) -> None:
        """Subscribe to all resampled component metric streams."""
        for request in self._pending_requests:
            await self._resampler_subscription_sender.send(request)
        self._pending_requests.clear()
