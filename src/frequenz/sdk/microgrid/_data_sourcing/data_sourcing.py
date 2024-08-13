# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""The DataSourcing Actor."""

from frequenz.channels import Receiver

from ..._internal._channels import ChannelRegistry
from ...actor import Actor
from ._component_metric_request import ComponentMetricRequest
from .microgrid_api_source import MicrogridApiSource


class DataSourcingActor(Actor):
    """An actor that provides data streams of metrics as time series."""

    def __init__(
        self,
        request_receiver: Receiver[ComponentMetricRequest],
        registry: ChannelRegistry,
        *,
        name: str | None = None,
    ) -> None:
        """Create a `DataSourcingActor` instance.

        Args:
            request_receiver: A channel receiver to accept metric requests from.
            registry: A channel registry.  To be replaced by a singleton
                instance.
            name: The name of the actor. If `None`, `str(id(self))` will be used. This
                is used mostly for debugging purposes.
        """
        super().__init__(name=name)
        self._request_receiver = request_receiver
        self._microgrid_api_source = MicrogridApiSource(registry)

    async def _run(self) -> None:
        """Run the actor."""
        async for request in self._request_receiver:
            await self._microgrid_api_source.add_metric(request)
