# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""An actor to resample microgrid component metrics."""


import asyncio
import logging

from frequenz.channels import (
    Broadcast,
    BroadcastReceiver,
    OneshotChannel,
    Receiver,
    Sender,
)
from frequenz.quantities import Quantity

from .._internal._asyncio import cancel_and_await
from ..actor._actor import Actor
from ..timeseries import Sample
from ..timeseries._resampling._config import ResamplerConfig
from ..timeseries._resampling._exceptions import ResamplingError
from ..timeseries._resampling._resampler import Resampler
from ._data_sourcing import ComponentMetricRequest

_logger = logging.getLogger(__name__)


class ComponentMetricsResamplingActor(Actor):
    """An actor to resample microgrid component metrics."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        data_sourcing_request_sender: Sender[ComponentMetricRequest],
        resampling_request_receiver: Receiver[ComponentMetricRequest],
        config: ResamplerConfig,
        name: str | None = None,
    ) -> None:
        """Initialize an instance.

        Args:
            data_sourcing_request_sender: The sender used to send requests to
                the [`DataSourcingActor`][frequenz.sdk.actor.DataSourcingActor]
                to subscribe to component metrics.
            resampling_request_receiver: The receiver to use to receive new
                resampling subscription requests.
            config: The configuration for the resampler.
            name: The name of the actor. If `None`, `str(id(self))` will be used. This
                is used mostly for debugging purposes.
        """
        super().__init__(name=name)
        self._data_sourcing_request_sender: Sender[ComponentMetricRequest] = (
            data_sourcing_request_sender
        )
        self._resampling_request_receiver: Receiver[ComponentMetricRequest] = (
            resampling_request_receiver
        )
        self._resampler: Resampler = Resampler(config)
        self._data_sink_channels: dict[str, Broadcast[Sample[Quantity]]] = {}

    async def _subscribe_to_data_source(
        self, request: ComponentMetricRequest
    ) -> BroadcastReceiver[Sample[Quantity]]:
        """Subscribe to the data source using a new request.

        Args:
            request: The original request from the resampler.

        Returns:
            The metric data receiver.
        """
        sender, receiver = OneshotChannel[BroadcastReceiver[Sample[Quantity]]]()
        data_source_request = ComponentMetricRequest(
            namespace=request.namespace + ":Source",
            component_id=request.component_id,
            metric=request.metric,
            start_time=request.start_time,
            telem_stream_sender=sender,
        )
        await self._data_sourcing_request_sender.send(data_source_request)
        return await receiver.receive()

    async def _subscribe(self, request: ComponentMetricRequest) -> None:
        """Request data for a component metric.

        Args:
            request: The request for component metric data.
        """
        request_channel_name = request.get_channel_name()

        # If we are already handling this request, answer the request by sending a
        # new receiver from the existing channel.
        if data_sink_channel := self._data_sink_channels.get(request_channel_name):
            await request.telem_stream_sender.send(data_sink_channel.new_receiver())
            return

        # Set up data source and sink channels
        data_source = await self._subscribe_to_data_source(request)

        data_sink_channel = Broadcast(name=request_channel_name, resend_latest=True)
        await request.telem_stream_sender.send(data_sink_channel.new_receiver())
        self._data_sink_channels[request_channel_name] = data_sink_channel

        self._resampler.add_timeseries(
            name=request_channel_name,
            source=data_source,
            sink=data_sink_channel.new_sender().send,
        )

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
        """
        tasks_to_cancel: set[asyncio.Task[None]] = set()
        subscriptions_task: asyncio.Task[None] | None = None
        resampling_task: asyncio.Task[None] | None = None

        try:
            while True:
                if subscriptions_task is None or subscriptions_task.done():
                    subscriptions_task = asyncio.create_task(
                        self._process_resampling_requests()
                    )
                    tasks_to_cancel.add(subscriptions_task)

                if resampling_task is None or resampling_task.done():
                    resampling_task = asyncio.create_task(self._resampler.resample())
                    tasks_to_cancel.add(resampling_task)

                done, _ = await asyncio.wait(
                    [resampling_task, subscriptions_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if subscriptions_task in done:
                    tasks_to_cancel.remove(subscriptions_task)
                    self._log_subscriptions_task_error(subscriptions_task)

                if resampling_task in done:
                    tasks_to_cancel.remove(resampling_task)
                    self._log_resampling_task_error(resampling_task)

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

    def _log_subscriptions_task_error(
        self, subscriptions_task: asyncio.Task[None]
    ) -> None:
        """Log an error from a stopped subscriptions task.

        Args:
            subscriptions_task: The subscriptions task.
        """
        try:
            subscriptions_task.result()
        # pylint: disable-next=broad-except
        except (Exception, asyncio.CancelledError):
            _logger.exception(
                "The subscriptions task ended with an exception, restarting..."
            )
        else:
            _logger.error("The subscriptions task ended unexpectedly, restarting...")

    def _log_resampling_task_error(self, resampling_task: asyncio.Task[None]) -> None:
        """Log an error from a stopped resampling task.

        Args:
            resampling_task: The resampling task.
        """
        # The resampler shouldn't be cancelled or end without an exception
        try:
            resampling_task.result()
        except ResamplingError as error:
            for source, source_error in error.exceptions.items():
                _logger.error(
                    "Error resampling source %s, removing source",
                    source,
                    exc_info=source_error,
                )
                removed = self._resampler.remove_timeseries(source)
                if not removed:
                    _logger.error(
                        "Got an exception from an unknown source: "
                        "source=%r, exception=%r",
                        source,
                        source_error,
                    )
        # pylint: disable-next=broad-except
        except (Exception, asyncio.CancelledError):
            # We don't know what to do with something other than
            # ResamplingError, so we log it, restart, and hope for the best.
            _logger.exception(
                "The resample() function got an unexpected error, restarting..."
            )
        else:
            # The resample function should not end normally, so we log it,
            # restart, and hope for the best.
            _logger.error(
                "The resample() function ended without an exception, restarting..."
            )
