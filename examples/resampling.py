# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Frequenz Python SDK resampling example."""

import asyncio
from datetime import datetime, timezone

from frequenz.channels import Broadcast
from frequenz.channels.util import Merge

from frequenz.sdk import microgrid
from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    ComponentMetricsResamplingActor,
    DataSourcingActor,
)
from frequenz.sdk.microgrid.component import ComponentCategory, ComponentMetricId
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._resampling import Resampler, Sink, Source

HOST = "microgrid.sandbox.api.frequenz.io"
PORT = 61060


async def _calculate_average(source: Source, sink: Sink) -> None:
    avg: float = 0.0
    count: int = 0
    async for sample in source:
        print(f"Received sample to average at {sample.timestamp}: {sample.value}")
        count += 1
        if sample.value is None:
            continue
        avg = avg * (count - 1) / count + sample.value / count
        await sink(Sample(datetime.now(timezone.utc), avg))


async def _print_sample(sample: Sample) -> None:
    print(f"\nResampled average at {sample.timestamp}: {sample.value}\n")


async def run() -> None:  # pylint: disable=too-many-locals
    """Run main functions that initializes and creates everything."""
    await microgrid.initialize(HOST, PORT)

    channel_registry = ChannelRegistry(name="data-registry")

    # Create a channels for sending/receiving subscription requests
    data_source_request_channel = Broadcast[ComponentMetricRequest]("data-source")
    data_source_request_sender = data_source_request_channel.new_sender()
    data_source_request_receiver = data_source_request_channel.new_receiver()

    resampling_request_channel = Broadcast[ComponentMetricRequest]("resample")
    resampling_request_sender = resampling_request_channel.new_sender()
    resampling_request_receiver = resampling_request_channel.new_receiver()

    # Instantiate a data sourcing actor
    _data_sourcing_actor = DataSourcingActor(
        request_receiver=data_source_request_receiver, registry=channel_registry
    )

    # Instantiate a resampling actor
    _resampling_actor = ComponentMetricsResamplingActor(
        channel_registry=channel_registry,
        data_sourcing_request_sender=data_source_request_sender,
        resampling_request_receiver=resampling_request_receiver,
        resampling_period_s=1,
    )

    components = await microgrid.get().api_client.components()
    battery_ids = [
        comp.component_id
        for comp in components
        if comp.category == ComponentCategory.BATTERY
    ]

    print(f"Found {len(battery_ids)} batteries: {battery_ids}")

    # Create subscription requests for each battery's SoC
    subscription_requests = [
        ComponentMetricRequest(
            namespace="resampling",
            component_id=component_id,
            metric_id=ComponentMetricId.SOC,
            start_time=None,
        )
        for component_id in battery_ids
    ]

    # Send the subscription requests
    await asyncio.gather(
        *[resampling_request_sender.send(request) for request in subscription_requests]
    )

    # Merge sample receivers for each subscription into one receiver
    merged_receiver = Merge(
        *[
            channel_registry.new_receiver(req.get_channel_name())
            for req in subscription_requests
        ]
    )

    # Create a channel to calculate an average for all the data
    average_chan = Broadcast[Sample]("average")

    second_stage_resampler = Resampler(resampling_period_s=3.0)
    second_stage_resampler.add_timeseries(average_chan.new_receiver(), _print_sample)

    average_sender = average_chan.new_sender()
    # Needed until channels Senders raises exceptions on errors
    async def sink_adapter(sample: Sample) -> None:
        assert await average_sender.send(sample)

    print("Starting...")

    try:
        # This will run until it is interrupted (with Ctrl-C for example)
        await asyncio.gather(
            _calculate_average(merged_receiver, sink_adapter),
            second_stage_resampler.resample(),
        )
    finally:
        await second_stage_resampler.stop()


try:
    asyncio.run(run())
except KeyboardInterrupt:
    print("Bye!")
