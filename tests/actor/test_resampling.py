"""Frequenz Python SDK resampling example.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import time
from typing import Iterator

import grpc
from frequenz.api.microgrid import microgrid_pb2
from frequenz.api.microgrid.battery_pb2 import Battery
from frequenz.api.microgrid.battery_pb2 import Data as BatteryData
from frequenz.api.microgrid.common_pb2 import MetricAggregation
from frequenz.api.microgrid.microgrid_pb2 import ComponentData, ComponentIdParam
from frequenz.channels import Broadcast
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from pytest_mock import MockerFixture

from frequenz.sdk.actor import ChannelRegistry
from frequenz.sdk.actor.data_sourcing import DataSourcingActor
from frequenz.sdk.actor.resampling import ComponentMetricsResamplingActor
from frequenz.sdk.data_pipeline import ComponentMetricId, ComponentMetricRequest
from frequenz.sdk.microgrid import microgrid_api
from tests.test_microgrid import mock_api


async def test_component_metrics_resampling_actor(mocker: MockerFixture) -> None:
    """Run main functions that initializes and creates everything."""

    servicer = mock_api.MockMicrogridServicer()

    # pylint: disable=unused-argument
    def get_component_data(
        request: ComponentIdParam, context: grpc.ServicerContext
    ) -> Iterator[ComponentData]:
        """Return an iterator for mock ComponentData."""
        # pylint: disable=stop-iteration-return

        def next_msg(value: float) -> ComponentData:
            timestamp = Timestamp()
            timestamp.GetCurrentTime()
            return ComponentData(
                id=request.id,
                ts=timestamp,
                battery=Battery(
                    data=BatteryData(
                        soc=MetricAggregation(avg=value),
                    )
                ),
            )

        for value in [3, 6, 9]:
            yield next_msg(value=value)
            time.sleep(0.1)

    mocker.patch.object(servicer, "GetComponentData", get_component_data)

    server = mock_api.MockGrpcServer(servicer, port=57899)
    await server.start()

    servicer.add_component(1, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_GRID)
    servicer.add_component(
        3, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_JUNCTION
    )
    servicer.add_component(4, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_METER)
    servicer.add_component(7, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_METER)
    servicer.add_component(
        8, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_INVERTER
    )
    servicer.add_component(
        9, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_BATTERY
    )

    servicer.add_connection(1, 3)
    servicer.add_connection(3, 4)
    servicer.add_connection(3, 7)
    servicer.add_connection(7, 8)
    servicer.add_connection(8, 9)

    await microgrid_api.initialize("[::1]", 57899)

    channel_registry = ChannelRegistry(name="Microgrid Channel Registry")

    data_source_request_channel = Broadcast[ComponentMetricRequest](
        "Data Source Request Channel"
    )
    data_source_request_sender = data_source_request_channel.get_sender()
    data_source_request_receiver = data_source_request_channel.get_receiver()

    resampling_actor_request_channel = Broadcast[ComponentMetricRequest](
        "Resampling Actor Request Channel"
    )
    resampling_actor_request_sender = resampling_actor_request_channel.get_sender()
    resampling_actor_request_receiver = resampling_actor_request_channel.get_receiver()

    DataSourcingActor(
        request_receiver=data_source_request_receiver, registry=channel_registry
    )

    ComponentMetricsResamplingActor(
        channel_registry=channel_registry,
        subscription_sender=data_source_request_sender,
        subscription_receiver=resampling_actor_request_receiver,
        resampling_period_s=0.1,
    )

    subscription_request = ComponentMetricRequest(
        namespace="Resampling",
        component_id=9,
        metric_id=ComponentMetricId.SOC,
        start_time=None,
    )

    await resampling_actor_request_sender.send(subscription_request)

    index = 0
    expected_sample_values = [
        3.0,
        4.5,
        6.0,
        7.5,
        9.0,
        None,
        None,
        None,
    ]

    async for sample in channel_registry.get_receiver(
        subscription_request.get_channel_name()
    ):
        assert sample.value == expected_sample_values[index]
        index += 1
        if index >= len(expected_sample_values):
            break

    await server.stop(0.1)
    microgrid_api._MICROGRID_API = None  # pylint: disable=protected-access
