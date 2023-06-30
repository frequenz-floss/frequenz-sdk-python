# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Tests for the microgrid client thin wrapper.
"""

import asyncio

import grpc
import pytest
from frequenz.api.microgrid import common_pb2 as common_pb
from frequenz.api.microgrid import microgrid_pb2 as microgrid_pb
from google.protobuf.empty_pb2 import Empty  # pylint: disable=no-name-in-module

from frequenz.sdk.microgrid import client
from frequenz.sdk.microgrid.client import Connection, LinearBackoff
from frequenz.sdk.microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    EVChargerData,
    InverterData,
    InverterType,
    MeterData,
)

from . import mock_api

# pylint: disable=missing-function-docstring,use-implicit-booleaness-not-comparison
# pylint: disable=missing-class-docstring,no-member


class TestMicrogridGrpcClient:
    @staticmethod
    def create_client(port: int) -> client.MicrogridApiClient:
        return client.MicrogridGrpcClient(
            grpc.aio.insecure_channel(f"[::]:{port}"),
            f"[::]:{port}",
            retry_spec=LinearBackoff(interval=0.0, jitter=0.05),
        )

    async def test_components(self) -> None:
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            assert set(await microgrid.components()) == set()

            servicer.add_component(
                0, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )
            assert set(await microgrid.components()) == {
                Component(0, ComponentCategory.METER)
            }

            servicer.add_component(
                0, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )
            assert set(await microgrid.components()) == {
                Component(0, ComponentCategory.METER),
                Component(0, ComponentCategory.BATTERY),
            }

            servicer.add_component(
                0, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )
            assert set(await microgrid.components()) == {
                Component(0, ComponentCategory.METER),
                Component(0, ComponentCategory.BATTERY),
                Component(0, ComponentCategory.METER),
            }

            # sensors are not counted as components by the API client
            servicer.add_component(
                1, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR
            )
            assert set(await microgrid.components()) == {
                Component(0, ComponentCategory.METER),
                Component(0, ComponentCategory.BATTERY),
                Component(0, ComponentCategory.METER),
            }

            servicer.set_components(
                [
                    (9, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER),
                    (99, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER),
                    (666, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR),
                    (999, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY),
                ]
            )
            assert set(await microgrid.components()) == {
                Component(9, ComponentCategory.METER),
                Component(99, ComponentCategory.INVERTER, InverterType.NONE),
                Component(999, ComponentCategory.BATTERY),
            }

            servicer.set_components(
                [
                    (99, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR),
                    (
                        100,
                        microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_UNSPECIFIED,
                    ),
                    (101, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_GRID),
                    (104, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER),
                    (105, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER),
                    (106, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY),
                    (107, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER),
                    (999, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR),
                ]
            )
            assert set(await microgrid.components()) == {
                Component(100, ComponentCategory.NONE),
                Component(101, ComponentCategory.GRID),
                Component(104, ComponentCategory.METER),
                Component(105, ComponentCategory.INVERTER, InverterType.NONE),
                Component(106, ComponentCategory.BATTERY),
                Component(107, ComponentCategory.EV_CHARGER),
            }

        finally:
            assert await server.graceful_shutdown()

    async def test_connections(self) -> None:
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57898)
        await server.start()

        try:
            microgrid = self.create_client(57898)

            assert set(await microgrid.connections()) == set()

            servicer.add_connection(0, 0)
            assert set(await microgrid.connections()) == {Connection(0, 0)}

            servicer.add_connection(7, 9)
            servicer.add_component(
                7,
                component_category=microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
            )
            servicer.add_component(
                9,
                component_category=microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER,
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
                    microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
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
                    microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
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
            assert set(await microgrid.connections(starts=set([]), ends=set([]))) == {
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
        server = mock_api.MockGrpcServer(servicer, port=57897)
        await server.start()

        try:
            microgrid = self.create_client(57897)

            assert list(await microgrid.connections()) == []
            for component_id in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
                servicer.add_component(
                    component_id,
                    microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY,
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
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )
            servicer.add_component(
                38, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )

            with pytest.raises(ValueError):
                ## should raise a ValueError for missing component_id
                await microgrid.meter_data(20)

            with pytest.raises(ValueError):
                ## should raise a ValueError for wrong component category
                await microgrid.meter_data(38)
            peekable = (await microgrid.meter_data(83)).into_peekable()
            await asyncio.sleep(0.2)

        finally:
            assert await server.graceful_shutdown()

        latest = peekable.peek()
        assert isinstance(latest, MeterData)
        assert latest.component_id == 83

    async def test_battery_data(self) -> None:
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )
            servicer.add_component(
                38, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
            )

            with pytest.raises(ValueError):
                ## should raise a ValueError for missing component_id
                await microgrid.meter_data(20)

            with pytest.raises(ValueError):
                ## should raise a ValueError for wrong component category
                await microgrid.meter_data(38)
            peekable = (await microgrid.battery_data(83)).into_peekable()
            await asyncio.sleep(0.2)

        finally:
            assert await server.graceful_shutdown()

        latest = peekable.peek()
        assert isinstance(latest, BatteryData)
        assert latest.component_id == 83

    async def test_inverter_data(self) -> None:
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
            )
            servicer.add_component(
                38, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )

            with pytest.raises(ValueError):
                ## should raise a ValueError for missing component_id
                await microgrid.meter_data(20)

            with pytest.raises(ValueError):
                ## should raise a ValueError for wrong component category
                await microgrid.meter_data(38)
            peekable = (await microgrid.inverter_data(83)).into_peekable()
            await asyncio.sleep(0.2)

        finally:
            assert await server.graceful_shutdown()

        latest = peekable.peek()
        assert isinstance(latest, InverterData)
        assert latest.component_id == 83

    async def test_ev_charger_data(self) -> None:
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER
            )
            servicer.add_component(
                38, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )

            with pytest.raises(ValueError):
                ## should raise a ValueError for missing component_id
                await microgrid.meter_data(20)

            with pytest.raises(ValueError):
                ## should raise a ValueError for wrong component category
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
        server = mock_api.MockGrpcServer(servicer, port=57899)

        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                83, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )

            await microgrid.set_power(component_id=83, power_w=12)

            assert servicer.latest_charge is not None
            assert servicer.latest_charge.component_id == 83
            assert servicer.latest_charge.power_w == 12

        finally:
            assert await server.graceful_shutdown()

    async def test_discharge(self) -> None:
        """Check if discharge is able to discharge component."""
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)

        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                73, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )

            await microgrid.set_power(component_id=73, power_w=-15)

            assert servicer.latest_discharge is not None
            assert servicer.latest_discharge.component_id == 73
            assert servicer.latest_discharge.power_w == 15
        finally:
            assert await server.graceful_shutdown()

    async def test_set_bounds(self) -> None:
        servicer = mock_api.MockMicrogridServicer()
        server = mock_api.MockGrpcServer(servicer, port=57899)
        await server.start()

        try:
            microgrid = self.create_client(57899)

            servicer.add_component(
                38, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
            )

            num_calls = 4

            target_metric = microgrid_pb.SetBoundsParam.TargetMetric
            expected_bounds = [
                microgrid_pb.SetBoundsParam(
                    component_id=comp_id,
                    target_metric=target_metric.TARGET_METRIC_POWER_ACTIVE,
                    bounds=common_pb.Bounds(lower=-10, upper=2),
                )
                for comp_id in range(num_calls)
            ]
            for cid in range(num_calls):
                await microgrid.set_bounds(cid, -10.0, 2.0)
                await asyncio.sleep(0.1)

        finally:
            assert await server.graceful_shutdown()

        assert len(expected_bounds) == len(servicer.get_bounds())

        #  pylint:disable=unnecessary-lambda-assignment
        sort_key = lambda bound: bound.target_metric
        assert sorted(servicer.get_bounds(), key=sort_key) == sorted(
            expected_bounds, key=sort_key
        )
