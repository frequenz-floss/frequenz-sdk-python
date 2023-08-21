# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""An actor to resample microgrid component metrics."""

from __future__ import annotations

import asyncio
import dataclasses
import logging

from frequenz.channels import Receiver, Sender

from .._internal._asyncio import cancel_and_await
from ..timeseries import Sample
from ..timeseries._quantities import Quantity
from ..timeseries._resampling import Resampler, ResamplerConfig, ResamplingError
from ._actor import Actor
from ._channel_registry import ChannelRegistry
from ._data_sourcing import ComponentMetricRequest

_logger = logging.getLogger(__name__)


class ComponentMetricsResamplingActor(Actor):
    """An actor to resample microgrid component metrics."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        channel_registry: ChannelRegistry,
        data_sourcing_request_sender: Sender[ComponentMetricRequest],
        resampling_request_receiver: Receiver[ComponentMetricRequest],
        config: ResamplerConfig,
        name: str | None = None,
    ) -> None:
        """Initialize an instance.

        Args:
            channel_registry: The channel registry used to get senders and
                receivers for data sourcing subscriptions.
            data_sourcing_request_sender: The sender used to send requests to
                the [`DataSourcingActor`][frequenz.sdk.actor.DataSourcingActor]
                to subscribe to component metrics.
            resampling_request_receiver: The receiver to use to receive new
                resampmling subscription requests.
            config: The configuration for the resampler.
            name: The name of the actor. If `None`, `str(id(self))` will be used. This
                is used mostly for debugging purposes.
        """
        super().__init__(name=name)
        self._channel_registry: ChannelRegistry = channel_registry
        self._data_sourcing_request_sender: Sender[
            ComponentMetricRequest
        ] = data_sourcing_request_sender
        self._resampling_request_receiver: Receiver[
            ComponentMetricRequest
        ] = resampling_request_receiver
        self._resampler: Resampler = Resampler(config)
        self._active_req_channels: set[str] = set()

    async def _subscribe(self, request: ComponentMetricRequest) -> None:
        """Request data for a component metric.

        Args:
            request: The request for component metric data.
        """
        request_channel_name = request.get_channel_name()

        # If we are already handling this request, there is nothing to do.
        if request_channel_name in self._active_req_channels:
            return

        self._active_req_channels.add(request_channel_name)

        data_source_request = dataclasses.replace(
            request, namespace=request.namespace + ":Source"
        )
        data_source_channel_name = data_source_request.get_channel_name()
        await self._data_sourcing_request_sender.send(data_source_request)
        receiver = self._channel_registry.new_receiver(data_source_channel_name)

        # This is a temporary hack until the Sender implementation uses
        # exceptions to report errors.
        sender = self._channel_registry.new_sender(request_channel_name)

        async def sink_adapter(sample: Sample[Quantity]) -> None:
            await sender.send(sample)

        self._resampler.add_timeseries(request_channel_name, receiver, sink_adapter)

    async def _process_resampling_requests(self) -> None:
        """Process resampling data requests."""
        async for request in self._resampling_request_receiver:
            await self._subscribe(request)

    async def _run(self) -> None:
        """Resample known component metrics and process resampling requests.

        If there is a resampling error while resampling some component metric,
        then that metric will be discarded and not resampled any more. Any
        other error will be propagated (most likely ending in the actor being
        restarted).

        This method creates 2 main tasks:

        - One task to process incoming subscription requests to resample new metrics.
        - One task to run the resampler.

        Raises:
            RuntimeError: If there is some unexpected error while resampling or
                handling requests.

        [//]: # (# noqa: DAR401 error)
        """
        tasks_to_cancel: set[asyncio.Task[None]] = set()
        try:
            subscriptions_task = asyncio.create_task(
                self._process_resampling_requests()
            )
            tasks_to_cancel.add(subscriptions_task)

            while True:
                resampling_task = asyncio.create_task(self._resampler.resample())
                tasks_to_cancel.add(resampling_task)
                done, _ = await asyncio.wait(
                    [resampling_task, subscriptions_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if subscriptions_task in done:
                    tasks_to_cancel.remove(subscriptions_task)
                    raise RuntimeError(
                        "There was a problem with the subscriptions channel."
                    )

                if resampling_task in done:
                    tasks_to_cancel.remove(resampling_task)
                    # The resampler shouldn't end without an exception
                    error = resampling_task.exception()
                    assert (
                        error is not None
                    ), "The resample() function shouldn't exit normally."

                    # We don't know what to do with something other than
                    # ResamplingError, so propagate the exception if that is the
                    # case.
                    if not isinstance(error, ResamplingError):
                        raise error
                    for source, source_error in error.exceptions.items():
                        _logger.error(
                            "Error resampling source %s, removing source...", source
                        )
                        removed = self._resampler.remove_timeseries(source)
                        if not removed:
                            _logger.warning(
                                "Got an exception from an unknown source: "
                                "source=%r, exception=%r",
                                source,
                                source_error,
                            )
                    # The resampling_task will be re-created if we reached this point
        finally:
            await asyncio.gather(*[cancel_and_await(t) for t in tasks_to_cancel])

            # XXX: Here we should probably do a:  pylint: disable=fixme
            # await self._resampler.stop()
            # But since the actor will be restarted, the internal state would
            # be broken if we stop the resampler.
            #
            # We have an even bigger problem with this naive restarting
            # approach, as restarting this actor without really resetting its
            # state would be mostly the same as not really leaving the run()
            # method and just swallow any exception, which doesn't look super
            # smart.
