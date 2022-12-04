# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Frequenz Python SDK resampling example."""

import asyncio

from frequenz.channels import Broadcast
from frequenz.channels.util import MergeNamed

from frequenz.sdk import microgrid
from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    ComponentMetricsResamplingActor,
    DataSourcingActor,
)
from frequenz.sdk.microgrid.component import ComponentCategory, ComponentMetricId

HOST = "microgrid.sandbox.api.frequenz.io"
PORT = 61060


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
        resampling_period_s=1.0,
    )

    components = await microgrid.get().api_client.components()
    battery_ids = [
        comp.component_id
        for comp in components
        if comp.category == ComponentCategory.BATTERY
    ]

    # Create subscription requests for each time series id
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
    merged_receiver = MergeNamed(
        **{
            req.get_channel_name(): channel_registry.new_receiver(
                req.get_channel_name()
            )
            for req in subscription_requests
        }
    )

    async for channel_name, msg in merged_receiver:
        print(f"{channel_name}: {msg}")


asyncio.run(run())
