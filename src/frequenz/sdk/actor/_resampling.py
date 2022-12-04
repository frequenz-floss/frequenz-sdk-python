# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""ComponentMetricsResamplingActor used to subscribe for resampled component metrics."""

import asyncio
import dataclasses
import logging
from typing import Sequence, Set

from frequenz.channels import Receiver, Sender

from frequenz.sdk.util.asyncio import cancel_and_await

from ..timeseries import Sample
from ..timeseries.resampling import Resampler, ResamplingError, ResamplingFunction
from ._channel_registry import ChannelRegistry
from ._data_sourcing import ComponentMetricRequest
from ._decorator import actor

logger = logging.Logger(__name__)


# pylint: disable=unused-argument
def average(samples: Sequence[Sample], resampling_period_s: float) -> float:
    """Calculate average of the provided values.

    Args:
        samples: sequences of samples to apply the average to. It must be
            non-empty.
        resampling_period_s: value describing how often resampling should be
            performed, in seconds

    Returns:
        average of all the sample values
    """
    assert len(samples) > 0, "Average cannot be given an empty list of samples"
    values = list(sample.value for sample in samples if sample.value is not None)
    return sum(values) / len(values)


@actor
class ComponentMetricsResamplingActor:
    """ComponentMetricsResamplingActor used to ingest component data and resample it."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        channel_registry: ChannelRegistry,
        subscription_sender: Sender[ComponentMetricRequest],
        subscription_receiver: Receiver[ComponentMetricRequest],
        resampling_period_s: float = 0.2,
        max_data_age_in_periods: float = 3.0,
        resampling_function: ResamplingFunction = average,
    ) -> None:
        """Initialize the ComponentMetricsResamplingActor.

        Args:
            channel_registry: global channel registry used for receiving component
                data from DataSource and for sending resampled samples downstream
            subscription_sender: channel for sending component metric requests to the
                DataSourcing actor
            subscription_receiver: channel for receiving component metric requests
            resampling_period_s: value describing how often resampling should be
                performed, in seconds
            max_data_age_in_periods: max age that samples shouldn't exceed in order
                to be used in the resampling function
            resampling_function: function to be applied to a sequence of samples within
                a resampling period to produce a single output sample

        Example:
            ```python
            async def run() -> None:
                await microgrid_api.initialize(HOST, PORT)

                channel_registry = ChannelRegistry(name="Microgrid Channel Registry")

                data_source_request_channel = Broadcast[ComponentMetricRequest](
                    "Data Source Request Channel"
                )
                data_source_request_sender = data_source_request_channel.new_sender()
                data_source_request_receiver = data_source_request_channel.new_receiver()

                resampling_actor_request_channel = Broadcast[ComponentMetricRequest](
                    "Resampling Actor Request Channel"
                )
                resampling_actor_request_sender = resampling_actor_request_channel.new_sender()
                resampling_actor_request_receiver = resampling_actor_request_channel.new_receiver()

                _data_sourcing_actor = DataSourcingActor(
                    request_receiver=data_source_request_receiver, registry=channel_registry
                )

                _resampling_actor = ComponentMetricsResamplingActor(
                    channel_registry=channel_registry,
                    subscription_sender=data_source_request_sender,
                    subscription_receiver=resampling_actor_request_receiver,
                    resampling_period_s=1.0,
                )

                components = await microgrid_api.get().microgrid_api_client.components()
                battery_ids = [
                    comp.component_id
                    for comp in components
                    if comp.category == ComponentCategory.BATTERY
                ]

                subscription_requests = [
                    ComponentMetricRequest(
                        namespace="Resampling",
                        component_id=component_id,
                        metric_id=ComponentMetricId.SOC,
                        start_time=None,
                    )
                    for component_id in battery_ids
                ]

                await asyncio.gather(
                    *[
                        resampling_actor_request_sender.send(request)
                        for request in subscription_requests
                    ]
                )

                sample_receiver = MergeNamed(
                    **{
                        channel_name: channel_registry.new_receiver(channel_name)
                        for channel_name in map(
                            lambda req: req.get_channel_name(), subscription_requests
                        )
                    }
                )

                async for channel_name, msg in sample_receiver:
                    print(msg)

            asyncio.run(run())
            ```
        """
        self._channel_registry = channel_registry
        self._subscription_sender = subscription_sender
        self._subscription_receiver = subscription_receiver
        self._resampling_period_s = resampling_period_s
        self._max_data_age_in_periods: float = max_data_age_in_periods
        self._resampling_function: ResamplingFunction = resampling_function
        self._resampler = Resampler(
            resampling_period_s=resampling_period_s,
            max_data_age_in_periods=max_data_age_in_periods,
            resampling_function=resampling_function,
        )

    async def _subscribe(self, request: ComponentMetricRequest) -> None:
        """Subscribe for data for a specific time series.

        Args:
            request: subscription request for a specific component metric
        """
        data_source_request = dataclasses.replace(
            request, namespace=request.namespace + ":Source"
        )
        data_source_channel_name = data_source_request.get_channel_name()
        await self._subscription_sender.send(data_source_request)
        receiver = self._channel_registry.new_receiver(data_source_channel_name)

        # This is a temporary hack until the Sender implementation uses
        # exceptions to report errors.
        sender = self._channel_registry.new_sender(request.get_channel_name())

        async def sink_adapter(sample: Sample) -> None:
            if not await sender.send(sample):
                raise Exception(f"Error while sending with sender {sender}", sender)

        self._resampler.add_timeseries(receiver, sink_adapter)

    async def _process_resampling_requests(self) -> None:
        """Process resampling data requests."""
        async for request in self._subscription_receiver:
            await self._subscribe(request)

    async def run(self) -> None:
        """Resample known component metrics and process resampling requests.

        If there is a resampling error while resampling some component metric,
        then that metric will be discarded and not resampled any more. Any
        other error will be propagated (most likely ending in the actor being
        restarted).

        Raises:
            RuntimeError: If there is some unexpected error while resampling or
                handling requests.

        # noqa: DAR401 error
        """
        tasks_to_cancel: Set[asyncio.Task] = set()
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
                        logger.error(
                            "Error resampling source %s, removing source...", source
                        )
                        removed = self._resampler.remove_timeseries(source)
                        if not removed:
                            logger.warning(
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
