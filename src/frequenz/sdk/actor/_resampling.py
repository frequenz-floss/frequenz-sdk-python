# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""ComponentMetricsResamplingActor used to subscribe for resampled component metrics."""

import asyncio
import dataclasses
import logging
import math
from typing import Dict, Sequence

from frequenz.channels import Receiver, Sender
from frequenz.channels.util import MergeNamed, Select, Timer

from ..timeseries import GroupResampler, ResamplingFunction, Sample
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

        self._resampler = GroupResampler(
            resampling_period_s=resampling_period_s,
            max_data_age_in_periods=max_data_age_in_periods,
            initial_resampling_function=resampling_function,
        )

        self._input_receivers: Dict[str, Receiver[Sample]] = {}
        self._output_senders: Dict[str, Sender[Sample]] = {}
        self._resampling_timer = Timer(interval=self._resampling_period_s)

    async def _subscribe(self, request: ComponentMetricRequest) -> None:
        """Subscribe for data for a specific time series.

        Args:
            request: subscription request for a specific component metric
        """
        channel_name = request.get_channel_name()

        data_source_request = dataclasses.replace(request, **dict(namespace="Source"))
        data_source_channel_name = data_source_request.get_channel_name()
        if channel_name not in self._input_receivers:
            await self._subscription_sender.send(data_source_request)
            receiver: Receiver[Sample] = self._channel_registry.new_receiver(
                data_source_channel_name
            )
            self._input_receivers[data_source_channel_name] = receiver
            self._resampler.add_time_series(time_series_id=data_source_channel_name)

        if channel_name not in self._output_senders:
            sender: Sender[Sample] = self._channel_registry.new_sender(channel_name)
            # This means that the `sender` will be sending samples to the channel with
            # name `channel_name` based on samples collected from the channel named
            # `data_source_channel_name`
            self._output_senders[data_source_channel_name] = sender

    def _is_sample_valid(self, sample: Sample) -> bool:
        """Check if the provided sample is valid.

        Args:
            sample: sample to be validated

        Returns:
            True if the sample is valid, False otherwise
        """
        if sample.value is None or math.isnan(sample.value):
            return False
        return True

    async def run(self) -> None:
        """Run the actor.

        Raises:
            ConnectionError: When the provider of the subscription channel closes the
                connection
        """
        while True:
            select = Select(
                resampling_timer=self._resampling_timer,
                subscription_receiver=self._subscription_receiver,
                component_data_receiver=MergeNamed(**self._input_receivers),
            )
            while await select.ready():
                if msg := select.resampling_timer:
                    assert msg.inner is not None, "The timer should never be 'closed'"
                    timestamp = msg.inner
                    awaitables = [
                        self._output_senders[channel_name].send(sample)
                        for channel_name, sample in self._resampler.resample(timestamp)
                    ]
                    await asyncio.gather(*awaitables)
                if msg := select.component_data_receiver:
                    if msg.inner is None:
                        # When this happens, then DataSourcingActor has closed the channel
                        # for sending data for a specific `ComponentMetricRequest`,
                        # which may need to be handled properly here, e.g. unsubscribe
                        continue
                    channel_name, sample = msg.inner
                    if self._is_sample_valid(sample=sample):
                        self._resampler.add_sample(
                            time_series_id=channel_name,
                            sample=sample,
                        )
                if msg := select.subscription_receiver:
                    if msg.inner is None:
                        raise ConnectionError(
                            "Subscription channel connection has been closed!"
                        )
                    await self._subscribe(request=msg.inner)
                    # Breaking out from the loop is required to regenerate
                    # component_data_receivers to be able to fulfil this
                    # subscription (later can be optimized by checking if
                    # an output channel already existed in the `subscribe()` method)
                    break
