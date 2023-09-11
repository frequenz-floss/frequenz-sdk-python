# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the microgrid client thin wrapper."""

import asyncio

import grpc
import pytest
from frequenz.api.common import components_pb2 as components_pb
from frequenz.api.common import metrics_pb2 as metrics_pb
from frequenz.api.microgrid import microgrid_pb2 as microgrid_pb
from google.protobuf.empty_pb2 import Empty  # pylint: disable=no-name-in-module

from frequenz.sdk.microgrid import client
from frequenz.sdk.microgrid.client import Connection, LinearBackoff
from frequenz.sdk.microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    EVChargerData,
    GridMetadata,
    InverterData,
    InverterType,
    MeterData,
)
from frequenz.sdk.microgrid.fuse import Fuse
from frequenz.sdk.timeseries import Current

from . import mock_api

# pylint: disable=missing-function-docstring,use-implicit-booleaness-not-comparison
# pylint: disable=missing-class-docstring,no-member


class TestMicrogridGrpcClient:
    """Tests for the microgrid client thin wrapper."""

    @staticmethod
    def create_client(port: int) -> client.MicrogridApiClient:
        """Create a client for the mock API server."""
        return client.MicrogridGrpcClient(
            grpc.aio.insecure_channel(f"[::]:{port}"),
            f"[::]:{port}",
            retry_spec=LinearBackoff(interval=0.0, jitter=0.05),
        )

    async def test_components(self) -> None:
        """Test the components() method."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            assert set(await microgrid.components()) == set()

            servicer.add_component(
                0, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )
            assert set(await microgrid.components()) == {
                Component(0, ComponentCategory.METER)
            }

            servicer.add_component(
                0, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )
            assert set(await microgrid.components()) == {
                Component(0, ComponentCategory.METER),
                Component(0, ComponentCategory.BATTERY),
            }

            servicer.add_component(
                0, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )
            assert set(await microgrid.components()) == {
                Component(0, ComponentCategory.METER),
                Component(0, ComponentCategory.BATTERY),
                Component(0, ComponentCategory.METER),
            }

            # sensors are not counted as components by the API client
            servicer.add_component(
                1, components_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR
            )
            assert set(await microgrid.components()) == {
                Component(0, ComponentCategory.METER),
                Component(0, ComponentCategory.BATTERY),
                Component(0, ComponentCategory.METER),
            }

            servicer.set_components(
                [
                    (9, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER),
                    (99, components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER),
                    (666, components_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR),
                    (999, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY),
                ]
            )
            assert set(await microgrid.components()) == {
                Component(9, ComponentCategory.METER),
                Component(99, ComponentCategory.INVERTER, InverterType.NONE),
                Component(999, ComponentCategory.BATTERY),
            }

            servicer.set_components(
                [
                    (99, components_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR),
                    (
                        100,
                        components_pb.ComponentCategory.COMPONENT_CATEGORY_UNSPECIFIED,
                    ),
                    (104, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER),
                    (105, components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER),
                    (106, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY),
                    (
                        107,
                        components_pb.ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER,
                    ),
                    (999, components_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR),
                ]
            )

            servicer.add_component(
                101,
                components_pb.ComponentCategory.COMPONENT_CATEGORY_GRID,
                Current.from_amperes(123.0),
            )

            grid_max_current = Current.from_amperes(123.0)
            grid_fuse = Fuse(grid_max_current)

            assert set(await microgrid.components()) == {
                Component(100, ComponentCategory.NONE),
                Component(
                    101,
                    ComponentCategory.GRID,
                    None,
                    GridMetadata(fuse=grid_fuse),
                ),
                Component(104, ComponentCategory.METER),
                Component(105, ComponentCategory.INVERTER, InverterType.NONE),
                Component(106, ComponentCategory.BATTERY),
                Component(107, ComponentCategory.EV_CHARGER),
            }

            servicer.set_components(
                [
                    (9, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER),
                    (666, components_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR),
                    (999, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY),
                ]
            )
            servicer.add_component(
                99,
                components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER,
                None,
                components_pb.InverterType.INVERTER_TYPE_BATTERY,
            )

            assert set(await microgrid.components()) == {
                Component(9, ComponentCategory.METER),
                Component(99, ComponentCategory.INVERTER, InverterType.BATTERY),
                Component(999, ComponentCategory.BATTERY),
            }

        finally:
            assert await server.graceful_shutdown()

    async def test_connections(self) -> None:
        """Test the connections() method."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57898)
        await server.start()

        try:
            microgrid = self.create_client(57898)

            assert set(await microgrid.connections()) == set()

            servicer.add_connection(0, 0)
            assert set(await microgrid.connections()) == {Connection(0, 0)}

            servicer.add_connection(7, 9)
            servicer.add_component(
                7,
                component_category=components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
            )
            servicer.add_component(
                9,
                component_category=components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER,
            )
            assert set(await microgrid.connections()) == {
                Connection(0, 0),
                Connection(7, 9),
            }

            servicer.add_connection(0, 0)
            assert set(await microgrid.connections()) == {
                Connection(0, 0),
                Connection(7, 9),
                Connection(0, 0),
            }

            servicer.set_connections([(999, 9), (99, 19), (909, 101), (99, 91)])
            for component_id in [999, 99, 19, 909, 101, 91]:
                servicer.add_component(
                    component_id,
                    components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
                )

            assert set(await microgrid.connections()) == {
                Connection(999, 9),
                Connection(99, 19),
                Connection(909, 101),
                Connection(99, 91),
            }

            for component_id in [1, 2, 3, 4, 5, 6, 7, 8]:
                servicer.add_component(
                    component_id,
                    components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
                )

            servicer.set_connections(
                [
                    (1, 2),
                    (2, 3),
                    (2, 4),
                    (2, 5),
                    (4, 3),
                    (4, 5),
                    (4, 6),
                    (5, 4),
                    (5, 7),
                    (5, 8),
                ]
            )
            assert set(await microgrid.connections()) == {
                Connection(1, 2),
                Connection(2, 3),
                Connection(2, 4),
                Connection(2, 5),
                Connection(4, 3),
                Connection(4, 5),
                Connection(4, 6),
                Connection(5, 4),
                Connection(5, 7),
                Connection(5, 8),
            }

            # passing empty sets is the same as passing `None`,
            # filter is ignored
            assert set(await microgrid.connections(starts=set(), ends=set())) == {
                Connection(1, 2),
                Connection(2, 3),
                Connection(2, 4),
                Connection(2, 5),
                Connection(4, 3),
                Connection(4, 5),
                Connection(4, 6),
                Connection(5, 4),
                Connection(5, 7),
                Connection(5, 8),
            }

            # include filter for connection start
            assert set(await microgrid.connections(starts={1})) == {Connection(1, 2)}

            assert set(await microgrid.connections(starts={2})) == {
                Connection(2, 3),
                Connection(2, 4),
                Connection(2, 5),
            }
            assert set(await microgrid.connections(starts={3})) == set()

            assert set(await microgrid.connections(starts={4, 5})) == {
                Connection(4, 3),
                Connection(4, 5),
                Connection(4, 6),
                Connection(5, 4),
                Connection(5, 7),
                Connection(5, 8),
            }

            # include filter for connection end
            assert set(await microgrid.connections(ends={1})) == set()

            assert set(await microgrid.connections(ends={3})) == {
                Connection(2, 3),
                Connection(4, 3),
            }

            assert set(await microgrid.connections(ends={2, 4, 5})) == {
                Connection(1, 2),
                Connection(2, 4),
                Connection(2, 5),
                Connection(4, 5),
                Connection(5, 4),
            }

            # different filters combine with AND logic
            assert set(
                await microgrid.connections(starts={1, 2, 4}, ends={4, 5, 6})
            ) == {
                Connection(2, 4),
                Connection(2, 5),
                Connection(4, 5),
                Connection(4, 6),
            }

            assert set(await microgrid.connections(starts={3, 5}, ends={7, 8})) == {
                Connection(5, 7),
                Connection(5, 8),
            }

            assert set(await microgrid.connections(starts={1, 5}, ends={2, 7})) == {
                Connection(1, 2),
                Connection(5, 7),
            }

        finally:
            assert await server.graceful_shutdown()

    async def test_bad_connections(self) -> None:
        """Validate that the client does not apply connection filters itself."""

        class BadServicer(mock_api.MockMicrogridServicer):
            # pylint: disable=unused-argument,invalid-name
            def ListConnections(
                self,
                request: microgrid_pb.ConnectionFilter,
                context: grpc.ServicerContext,
            ) -> microgrid_pb.ConnectionList:
                """Ignores supplied `ConnectionFilter`."""
                return microgrid_pb.ConnectionList(connections=self._connections)

            def ListAllComponents(
                self, request: Empty, context: grpc.ServicerContext
            ) -> microgrid_pb.ComponentList:
                return microgrid_pb.ComponentList(components=self._components)

        servicer = BadServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57897)
        await server.start()

        try:
            microgrid = self.create_client(57897)

            assert list(await microgrid.connections()) == []
            for component_id in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
                servicer.add_component(
                    component_id,
                    components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
                )
            servicer.set_connections(
                [
                    (1, 2),
                    (1, 9),
                    (2, 3),
                    (3, 4),
                    (4, 5),
                    (5, 6),
                    (6, 7),
                    (7, 6),
                    (7, 9),
                ]
            )

            unfiltered = {
                Connection(1, 2),
                Connection(1, 9),
                Connection(2, 3),
                Connection(3, 4),
                Connection(4, 5),
                Connection(5, 6),
                Connection(6, 7),
                Connection(7, 6),
                Connection(7, 9),
            }

            # because the application of filters is left to the server side,
            # it doesn't matter what filters we set in the client if the
            # server doesn't do its part
            assert set(await microgrid.connections()) == unfiltered
            assert set(await microgrid.connections(starts={1})) == unfiltered
            assert set(await microgrid.connections(ends={9})) == unfiltered
            assert (
                set(await microgrid.connections(starts={1, 7}, ends={3, 9}))
                == unfiltered
            )

        finally:
            assert await server.graceful_shutdown()

    async def test_meter_data(self) -> None:
        """Test the meter_data() method."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )
            servicer.add_component(
                38, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )

            with pytest.raises(ValueError):
                # should raise a ValueError for missing component_id
                await microgrid.meter_data(20)

            with pytest.raises(ValueError):
                # should raise a ValueError for wrong component category
                await microgrid.meter_data(38)
            peekable = (await microgrid.meter_data(83)).into_peekable()
            await asyncio.sleep(0.2)

        finally:
            assert await server.graceful_shutdown()

        latest = peekable.peek()
        assert isinstance(latest, MeterData)
        assert latest.component_id == 83

    async def test_battery_data(self) -> None:
        """Test the battery_data() method."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )
            servicer.add_component(
                38, components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
            )

            with pytest.raises(ValueError):
                # should raise a ValueError for missing component_id
                await microgrid.meter_data(20)

            with pytest.raises(ValueError):
                # should raise a ValueError for wrong component category
                await microgrid.meter_data(38)
            peekable = (await microgrid.battery_data(83)).into_peekable()
            await asyncio.sleep(0.2)

        finally:
            assert await server.graceful_shutdown()

        latest = peekable.peek()
        assert isinstance(latest, BatteryData)
        assert latest.component_id == 83

    async def test_inverter_data(self) -> None:
        """Test the inverter_data() method."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
            )
            servicer.add_component(
                38, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )

            with pytest.raises(ValueError):
                # should raise a ValueError for missing component_id
                await microgrid.meter_data(20)

            with pytest.raises(ValueError):
                # should raise a ValueError for wrong component category
                await microgrid.meter_data(38)
            peekable = (await microgrid.inverter_data(83)).into_peekable()
            await asyncio.sleep(0.2)

        finally:
            assert await server.graceful_shutdown()

        latest = peekable.peek()
        assert isinstance(latest, InverterData)
        assert latest.component_id == 83

    async def test_ev_charger_data(self) -> None:
        """Test the ev_charger_data() method."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, components_pb.ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER
            )
            servicer.add_component(
                38, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )

            with pytest.raises(ValueError):
                # should raise a ValueError for missing component_id
                await microgrid.meter_data(20)

            with pytest.raises(ValueError):
                # should raise a ValueError for wrong component category
                await microgrid.meter_data(38)
            peekable = (await microgrid.ev_charger_data(83)).into_peekable()
            await asyncio.sleep(0.2)

        finally:
            assert await server.graceful_shutdown()

        latest = peekable.peek()
        assert isinstance(latest, EVChargerData)
        assert latest.component_id == 83

    async def test_charge(self) -> None:
        """Check if charge is able to charge component."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57899)

        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )

            await microgrid.set_power(component_id=83, power_w=12)

            assert servicer.latest_power is not None
            assert servicer.latest_power.component_id == 83
            assert servicer.latest_power.power == 12

        finally:
            assert await server.graceful_shutdown()

    async def test_discharge(self) -> None:
        """Check if discharge is able to discharge component."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57899)

        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                73, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )

            await microgrid.set_power(component_id=73, power_w=-15)

            assert servicer.latest_power is not None
            assert servicer.latest_power.component_id == 73
            assert servicer.latest_power.power == -15
        finally:
            assert await server.graceful_shutdown()

    async def test_set_bounds(self) -> None:
        """Check if set_bounds is able to set bounds for component."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, host="127.0.0.1", port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                38, components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
            )

            num_calls = 4

            target_metric = microgrid_pb.SetBoundsParam.TargetMetric
            expected_bounds = [
                microgrid_pb.SetBoundsParam(
                    component_id=comp_id,
                    target_metric=target_metric.TARGET_METRIC_POWER_ACTIVE,
                    bounds=metrics_pb.Bounds(lower=-10, upper=2),
                )
                for comp_id in range(num_calls)
            ]
            for cid in range(num_calls):
                await microgrid.set_bounds(cid, -10.0, 2.0)
                await asyncio.sleep(0.1)

        finally:
            assert await server.graceful_shutdown()

        assert len(expected_bounds) == len(servicer.get_bounds())

        def sort_key(
            bound: microgrid_pb.SetBoundsParam,
        ) -> microgrid_pb.SetBoundsParam.TargetMetric.ValueType:
            return bound.target_metric

        assert sorted(servicer.get_bounds(), key=sort_key) == sorted(
            expected_bounds, key=sort_key
        )
