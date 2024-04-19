# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the DataSourcingActor."""

from frequenz.api.common import components_pb2 as components_pb
from frequenz.channels import Broadcast
from frequenz.client.microgrid import ComponentMetricId

from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    DataSourcingActor,
)
from frequenz.sdk.microgrid import connection_manager
from frequenz.sdk.timeseries import Quantity, Sample
from tests.microgrid import mock_api

# pylint: disable=no-member


class TestDataSourcingActor:
    """Tests for the DataSourcingActor."""

    async def test_data_sourcing_actor(self) -> None:
        """Tests for the DataSourcingActor."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)
        await server.start()

        servicer.add_component(
            1, components_pb.ComponentCategory.COMPONENT_CATEGORY_GRID
        )
        servicer.add_component(
            4, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER
        )
        servicer.add_component(
            7, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER
        )
        servicer.add_component(
            8, components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
        )
        servicer.add_component(
            9, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
        )

        servicer.add_connection(1, 4)
        servicer.add_connection(1, 7)
        servicer.add_connection(7, 8)
        servicer.add_connection(8, 9)

        await connection_manager.initialize("[::1]", 57899)

        req_chan = Broadcast[ComponentMetricRequest](name="data_sourcing_requests")
        req_sender = req_chan.new_sender()

        registry = ChannelRegistry(name="test-registry")

        async with DataSourcingActor(req_chan.new_receiver(), registry):
            active_power_request = ComponentMetricRequest(
                "test-namespace", 4, ComponentMetricId.ACTIVE_POWER, None
            )
            active_power_recv = registry.get_or_create(
                Sample[Quantity], active_power_request.get_channel_name()
            ).new_receiver()
            await req_sender.send(active_power_request)

            reactive_power_request = ComponentMetricRequest(
                "test-namespace", 4, ComponentMetricId.REACTIVE_POWER, None
            )
            _ = registry.get_or_create(
                Sample[Quantity], reactive_power_request.get_channel_name()
            ).new_receiver()
            await req_sender.send(reactive_power_request)

            soc_request = ComponentMetricRequest(
                "test-namespace", 9, ComponentMetricId.SOC, None
            )
            soc_recv = registry.get_or_create(
                Sample[Quantity], soc_request.get_channel_name()
            ).new_receiver()
            await req_sender.send(soc_request)

            soc2_request = ComponentMetricRequest(
                "test-namespace", 9, ComponentMetricId.SOC, None
            )
            soc2_recv = registry.get_or_create(
                Sample[Quantity], soc2_request.get_channel_name()
            ).new_receiver()
            await req_sender.send(soc2_request)

            for _ in range(3):
                sample = await soc_recv.receive()
                assert sample.value is not None
                assert 9.0 == sample.value.base_value

                sample = await soc2_recv.receive()
                assert sample.value is not None
                assert 9.0 == sample.value.base_value

                sample = await active_power_recv.receive()
                assert sample.value is not None
                assert 100.0 == sample.value.base_value

            assert await server.graceful_shutdown()
            connection_manager._CONNECTION_MANAGER = (  # pylint: disable=protected-access
                None
            )
