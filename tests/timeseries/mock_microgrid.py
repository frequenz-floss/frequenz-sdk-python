# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A configurable mock microgrid for testing logical meter formulas."""

from __future__ import annotations

import asyncio
import time
import typing
from typing import Iterator, Tuple

from frequenz.api.microgrid import microgrid_pb2
from frequenz.api.microgrid.battery_pb2 import Battery
from frequenz.api.microgrid.battery_pb2 import Data as BatteryData
from frequenz.api.microgrid.common_pb2 import AC, Metric, MetricAggregation
from frequenz.api.microgrid.inverter_pb2 import Data as InverterData
from frequenz.api.microgrid.inverter_pb2 import Inverter
from frequenz.api.microgrid.inverter_pb2 import Type as InverterType
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
    ResamplerConfig,
)
from tests.microgrid import mock_api


class MockMicrogrid:
    """Setup a MockApi instance with multiple component layouts for tests."""

    grid_id = 1
    main_meter_id = 4
    meter_id_suffix = 7
    inverter_id_suffix = 8
    battery_id_suffix = 9

    def __init__(
        self,
        server: mock_api.MockGrpcServer,
        servicer: mock_api.MockMicrogridServicer,
        grid_side_meter: bool,
    ):
        """Create a new instance.

        NOTE: Use the `MockMicrogrid.new()` method to create an instance instead.
        """
        self._server = server
        self._servicer = servicer
        self._id_increment = 0
        self._grid_side_meter = grid_side_meter

        self._connect_to = self.grid_id
        if self._grid_side_meter:
            self._connect_to = self.main_meter_id

        self.battery_inverter_ids: list[int] = []
        self.pv_inverter_ids: list[int] = []
        self.battery_ids: list[int] = []
        self.meter_ids: list[int] = [4]

        self._actors: list[typing.Any] = []

    async def start(self) -> Tuple[Sender[ComponentMetricRequest], ChannelRegistry]:
        """Start the MockServer, and the data source and resampling actors.

        Returns:
            A sender to send requests to the Resampling actor, and a corresponding
                channel registry.
        """
        await self._server.start()
        await asyncio.sleep(0.1)
        ret = await self._init_client_and_actors()
        await asyncio.sleep(0.1)
        return ret

    @classmethod
    async def new(
        cls, mocker: MockerFixture, *, grid_side_meter: bool = True
    ) -> MockMicrogrid:
        """Create a new MockMicrogrid instance."""
        server, servicer = await MockMicrogrid._setup_service(mocker)
        return MockMicrogrid(server, servicer, grid_side_meter)

    @classmethod
    async def _setup_service(
        cls, mocker: MockerFixture
    ) -> Tuple[mock_api.MockGrpcServer, mock_api.MockMicrogridServicer]:
        """Initialize a mock microgrid api for a test."""
        microgrid._microgrid._MICROGRID = None  # pylint: disable=protected-access
        servicer = mock_api.MockMicrogridServicer()

        # pylint: disable=unused-argument
        def get_component_data(
            request: ComponentIdParam, context: grpc.ServicerContext
        ) -> Iterator[ComponentData]:
            """Return an iterator for mock ComponentData."""
            # pylint: disable=stop-iteration-return

            def meter_msg(value: float) -> ComponentData:
                timestamp = Timestamp()
                timestamp.GetCurrentTime()
                return ComponentData(
                    id=request.id,
                    ts=timestamp,
                    meter=Meter(
                        data=MeterData(ac=AC(power_active=Metric(value=value)))
                    ),
                )

            def inverter_msg(value: float) -> ComponentData:
                timestamp = Timestamp()
                timestamp.GetCurrentTime()
                return ComponentData(
                    id=request.id,
                    ts=timestamp,
                    inverter=Inverter(
                        data=InverterData(ac=AC(power_active=Metric(value=value)))
                    ),
                )

            def battery_msg(value: float) -> ComponentData:
                timestamp = Timestamp()
                timestamp.GetCurrentTime()
                return ComponentData(
                    id=request.id,
                    ts=timestamp,
                    battery=Battery(data=BatteryData(soc=MetricAggregation(avg=value))),
                )

            if request.id % 10 == cls.inverter_id_suffix:
                next_msg = inverter_msg
            elif (
                request.id % 10 == cls.meter_id_suffix
                or request.id == cls.main_meter_id
            ):
                next_msg = meter_msg
            elif request.id % 10 == cls.battery_id_suffix:
                next_msg = battery_msg
            else:
                raise RuntimeError(
                    f"Component id {request.id} unsupported by MockMicrogrid"
                )
            for value in range(1, 10):
                # for inverters with component_id > 100, send only half the messages.
                if request.id % 10 == cls.inverter_id_suffix:
                    if request.id < 100 or value <= 5:
                        yield next_msg(value=value + int(request.id / 10))
                else:
                    yield next_msg(value=value + int(request.id / 10))
                time.sleep(0.1)

        mocker.patch.object(servicer, "GetComponentData", get_component_data)

        server = mock_api.MockGrpcServer(servicer, port=57891)

        servicer.add_component(
            1, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_GRID
        )
        servicer.add_component(
            4, microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_METER
        )
        servicer.add_connection(1, 4)

        return (server, servicer)

    def add_batteries(self, count: int) -> None:
        """Add batteries with connected inverters and meters to the microgrid.

        Args:
            count: number of battery sets to add.
        """
        for _ in range(count):
            meter_id = self._id_increment * 10 + 7
            inv_id = self._id_increment * 10 + 8
            bat_id = self._id_increment * 10 + 9
            self._id_increment += 1

            self.meter_ids.append(meter_id)
            self.battery_inverter_ids.append(inv_id)
            self.battery_ids.append(bat_id)

            self._servicer.add_component(
                meter_id,
                microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_METER,
            )
            self._servicer.add_component(
                inv_id,
                microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_INVERTER,
                InverterType.TYPE_BATTERY,
            )
            self._servicer.add_component(
                bat_id,
                microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
            )
            self._servicer.add_connection(self._connect_to, meter_id)
            self._servicer.add_connection(meter_id, inv_id)
            self._servicer.add_connection(inv_id, bat_id)

    def add_solar_inverters(self, count: int) -> None:
        """Add pv inverters and connected pv meters to the microgrid.

        Args:
            count: number of inverters to add to the microgrid.
        """
        for _ in range(count):
            meter_id = self._id_increment * 10 + 7
            inv_id = self._id_increment * 10 + 8
            self._id_increment += 1

            self.meter_ids.append(meter_id)
            self.pv_inverter_ids.append(inv_id)

            self._servicer.add_component(
                meter_id,
                microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_METER,
            )
            self._servicer.add_component(
                inv_id,
                microgrid_pb2.ComponentCategory.COMPONENT_CATEGORY_INVERTER,
                InverterType.TYPE_SOLAR,
            )
            self._servicer.add_connection(self._connect_to, meter_id)
            self._servicer.add_connection(meter_id, inv_id)

    async def _init_client_and_actors(
        self,
    ) -> Tuple[Sender[ComponentMetricRequest], ChannelRegistry]:
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

        self._actors.append(
            DataSourcingActor(
                request_receiver=data_source_request_receiver, registry=channel_registry
            )
        )

        self._actors.append(
            ComponentMetricsResamplingActor(
                channel_registry=channel_registry,
                data_sourcing_request_sender=data_source_request_sender,
                resampling_request_receiver=resampling_actor_request_receiver,
                config=ResamplerConfig(resampling_period_s=0.1),
            )
        )

        return (resampling_actor_request_sender, channel_registry)

    async def cleanup(self) -> None:
        """Clean up after a test."""
        for actor in self._actors:
            await actor._stop()  # pylint: disable=protected-access
        await self._server.graceful_shutdown()
        microgrid._microgrid._MICROGRID = None  # pylint: disable=protected-access
