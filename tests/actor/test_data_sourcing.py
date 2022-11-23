# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Tests for the DataSourcingActor.
"""

from frequenz.api.microgrid import microgrid_pb2
from frequenz.channels import Broadcast

from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    DataSourcingActor,
)
from frequenz.sdk.microgrid import ComponentMetricId, microgrid_api
from tests.test_microgrid import mock_api


class TestDataSourcingActor:
    """Tests for the DataSourcingActor."""

    async def test_data_sourcing_actor(self) -> None:
        """Tests for the DataSourcingActor."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)
        await server.start()

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

        await microgrid_api.initialize("[::1]", 57899)

        req_chan = Broadcast[ComponentMetricRequest]("data_sourcing_requests")
        req_sender = req_chan.new_sender()

        registry = ChannelRegistry(name="test-registry")

        DataSourcingActor(req_chan.new_receiver(), registry)
        active_power_request = ComponentMetricRequest(
            "test-namespace", 4, ComponentMetricId.ACTIVE_POWER, None
        )
        active_power_recv = registry.new_receiver(
            active_power_request.get_channel_name()
        )
        await req_sender.send(active_power_request)

        soc_request = ComponentMetricRequest(
            "test-namespace", 9, ComponentMetricId.SOC, None
        )
        soc_recv = registry.new_receiver(soc_request.get_channel_name())
        await req_sender.send(soc_request)

        soc2_request = ComponentMetricRequest(
            "test-namespace", 9, ComponentMetricId.SOC, None
        )
        soc2_recv = registry.new_receiver(soc2_request.get_channel_name())
        await req_sender.send(soc2_request)

        for _ in range(3):
            sample = await soc_recv.receive()
            assert sample is not None
            assert 9.0 == sample.value

            sample = await soc2_recv.receive()
            assert sample is not None
            assert 9.0 == sample.value

            sample = await active_power_recv.receive()
            assert sample is not None
            assert 100.0 == sample.value

        await server.stop(0.1)
        microgrid_api._MICROGRID_API = None  # pylint: disable=protected-access
