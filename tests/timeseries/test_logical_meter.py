# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""

import time
from typing import Iterator, Tuple

from frequenz.api.microgrid import microgrid_pb2
from frequenz.api.microgrid.common_pb2 import AC, Metric
from frequenz.api.microgrid.meter_pb2 import Data as MeterData
from frequenz.api.microgrid.meter_pb2 import Meter
from frequenz.api.microgrid.microgrid_pb2 import ComponentData, ComponentIdParam
from frequenz.channels import Broadcast, Sender
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from grpc.aio import grpc
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    ComponentMetricsResamplingActor,
    DataSourcingActor,
)
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import LogicalMeter
from frequenz.sdk.timeseries._logical_meter._formula_builder import FormulaBuilder
from tests.microgrid import mock_api


class TestLogicalMeter:
    """Tests for the logical meter."""

    async def setup(
        self, mocker: MockerFixture
    ) -> Tuple[Sender[ComponentMetricRequest], ChannelRegistry]:
        """Initialize a mock microgrid api for a test.

        Because we can't create a __init__ or multiple instances of the Test class, we
        use the `setup` method as a constructor, and call it once before each test.
        """
        microgrid._microgrid._MICROGRID = None  # pylint: disable=protected-access
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
                    meter=Meter(
                        data=MeterData(ac=AC(power_active=Metric(value=value)))
                    ),
                )

            for value in range(1, 10):
                yield next_msg(value=value + request.id)
                time.sleep(0.1)

        mocker.patch.object(servicer, "GetComponentData", get_component_data)

        servicer.add_component(
            1, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_GRID
        )
        servicer.add_component(
            3, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_JUNCTION
        )
        servicer.add_component(
            4, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_METER
        )
        servicer.add_component(
            7, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_METER
        )
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

        # pylint: disable=attribute-defined-outside-init
        self.server = mock_api.MockGrpcServer(servicer, port=57891)
        # pylint: enable=attribute-defined-outside-init
        await self.server.start()

        await microgrid.initialize("[::1]", 57891)

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
        resampling_actor_request_receiver = (
            resampling_actor_request_channel.new_receiver()
        )

        DataSourcingActor(
            request_receiver=data_source_request_receiver, registry=channel_registry
        )

        ComponentMetricsResamplingActor(
            channel_registry=channel_registry,
            subscription_sender=data_source_request_sender,
            subscription_receiver=resampling_actor_request_receiver,
            resampling_period_s=0.1,
        )

        return (resampling_actor_request_sender, channel_registry)

    async def cleanup(self) -> None:
        """Clean up after a test."""
        await self.server.stop(0.1)
        microgrid._microgrid._MICROGRID = None  # pylint: disable=protected-access

    async def test_1(self, mocker: MockerFixture) -> None:
        """Test the LogicalMeter with the grid power formula: "#4 + #7"."""
        request_sender, channel_registry = await self.setup(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
        )

        grid_power_recv = await logical_meter.start_formula(
            "#4 + #7", ComponentMetricId.ACTIVE_POWER
        )

        # Create a `FormulaBuilder` instance, just in order to reuse its
        # `_get_resampled_receiver` function implementation.

        # pylint: disable=protected-access
        builder = FormulaBuilder(
            logical_meter._namespace,
            channel_registry,
            request_sender,
            ComponentMetricId.ACTIVE_POWER,
        )
        comp_4 = await builder._get_resampled_receiver(4)
        comp_7 = await builder._get_resampled_receiver(7)
        # pylint: enable=protected-access

        results = []
        comp_4_data = []
        comp_7_data = []
        for _ in range(10):
            val = await comp_4.receive()
            assert val is not None and val.value is not None and val.value > 0.0
            comp_4_data.append(val.value)

            val = await comp_7.receive()
            assert val is not None and val.value is not None and val.value > 0.0
            comp_7_data.append(val.value)

            val = await grid_power_recv.receive()
            assert val is not None
            results.append(val.value)
        await self.cleanup()

        assert results == [
            val_4 + val_7 for val_4, val_7 in zip(comp_4_data, comp_7_data)
        ]
