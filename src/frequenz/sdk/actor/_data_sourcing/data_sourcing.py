# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""The DataSourcing Actor."""

from frequenz.channels import Receiver

from .._channel_registry import ChannelRegistry
from .._decorator import actor
from .microgrid_api_source import ComponentMetricRequest, MicrogridApiSource


@actor
class DataSourcingActor:
    """An actor that provides data streams of metrics as time series."""

    def __init__(
        self,
        request_receiver: Receiver[ComponentMetricRequest],
        registry: ChannelRegistry,
    ) -> None:
        """Create a `DataSourcingActor` instance.

        Args:
            request_receiver: A channel receiver to accept metric requests from.
            registry: A channel registry.  To be replaced by a singleton
                instance.
        """
        self._request_receiver = request_receiver
        self._microgrid_api_source = MicrogridApiSource(registry)

    async def run(self) -> None:
        """Run the actor."""
        async for request in self._request_receiver:
            await self._microgrid_api_source.add_metric(request)
