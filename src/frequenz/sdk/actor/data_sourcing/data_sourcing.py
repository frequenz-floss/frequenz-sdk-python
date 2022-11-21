"""The DataSourcing Actor.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from frequenz.channels import Receiver

from ...data_pipeline import ComponentMetricRequest
from .. import ChannelRegistry, actor
from .microgrid_api_source import MicrogridApiSource


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
