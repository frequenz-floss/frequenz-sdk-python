# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests of MicrogridApi."""

import asyncio
from asyncio.tasks import ALL_COMPLETED
from datetime import datetime, timezone
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest
from frequenz.client.common.microgrid import EnterpriseId, MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid import (
    DeliveryArea,
    EnergyMarketCodeType,
    Location,
    MicrogridInfo,
    MicrogridStatus,
)
from frequenz.client.microgrid.component import (
    BatteryInverter,
    Component,
    ComponentConnection,
    GridConnectionPoint,
    LiIonBattery,
    Meter,
)

from frequenz.sdk.microgrid import connection_manager

_MICROGRID_ID = MicrogridId(1)


class TestMicrogridApi:
    """Test for MicropgridApi."""

    # ignore mypy: Untyped decorator makes function "components" untyped
    @pytest.fixture
    def components(self) -> list[list[Component]]:
        """Get components in the graph.

        Override this method to create a graph with different components.

        Returns:
            list of components in graph

        """
        components = [
            [
                GridConnectionPoint(
                    id=ComponentId(1),
                    microgrid_id=_MICROGRID_ID,
                    rated_fuse_current=10_000,
                ),
                Meter(id=ComponentId(4), microgrid_id=_MICROGRID_ID),
                Meter(id=ComponentId(5), microgrid_id=_MICROGRID_ID),
                Meter(id=ComponentId(7), microgrid_id=_MICROGRID_ID),
                BatteryInverter(id=ComponentId(8), microgrid_id=_MICROGRID_ID),
                LiIonBattery(id=ComponentId(9), microgrid_id=_MICROGRID_ID),
                Meter(id=ComponentId(10), microgrid_id=_MICROGRID_ID),
                BatteryInverter(id=ComponentId(11), microgrid_id=_MICROGRID_ID),
                LiIonBattery(id=ComponentId(12), microgrid_id=_MICROGRID_ID),
            ],
            [
                GridConnectionPoint(
                    id=ComponentId(1),
                    microgrid_id=_MICROGRID_ID,
                    rated_fuse_current=10_000,
                ),
                Meter(id=ComponentId(4), microgrid_id=_MICROGRID_ID),
                Meter(id=ComponentId(7), microgrid_id=_MICROGRID_ID),
                BatteryInverter(id=ComponentId(8), microgrid_id=_MICROGRID_ID),
                LiIonBattery(id=ComponentId(9), microgrid_id=_MICROGRID_ID),
            ],
        ]
        return components

    # ignore mypy: Untyped decorator makes function "components" untyped
    @pytest.fixture
    def connections(self) -> list[list[ComponentConnection]]:
        """Get connections between components in the graph.

        Override this method to create a graph with different connections.

        Returns:
            list of connections between components in graph

        """
        connections = [
            [
                ComponentConnection(source=ComponentId(1), destination=ComponentId(4)),
                ComponentConnection(source=ComponentId(1), destination=ComponentId(5)),
                ComponentConnection(source=ComponentId(1), destination=ComponentId(7)),
                ComponentConnection(source=ComponentId(7), destination=ComponentId(8)),
                ComponentConnection(source=ComponentId(8), destination=ComponentId(9)),
                ComponentConnection(source=ComponentId(1), destination=ComponentId(10)),
                ComponentConnection(
                    source=ComponentId(10), destination=ComponentId(11)
                ),
                ComponentConnection(
                    source=ComponentId(11), destination=ComponentId(12)
                ),
            ],
            [
                ComponentConnection(source=ComponentId(1), destination=ComponentId(4)),
                ComponentConnection(source=ComponentId(1), destination=ComponentId(7)),
                ComponentConnection(source=ComponentId(7), destination=ComponentId(8)),
                ComponentConnection(source=ComponentId(8), destination=ComponentId(9)),
            ],
        ]
        return connections

    @pytest.fixture
    def microgrid(self) -> MicrogridInfo:
        """Fetch the microgrid information.

        Returns:
            the information about the microgrid
        """
        return MicrogridInfo(
            id=_MICROGRID_ID,
            enterprise_id=EnterpriseId(1),
            name="test",
            delivery_area=DeliveryArea(
                code="test", code_type=EnergyMarketCodeType.EUROPE_EIC
            ),
            status=MicrogridStatus.ACTIVE,
            create_timestamp=datetime.now(tz=timezone.utc),
            location=Location(
                latitude=52.520008,
                longitude=13.404954,
                country_code="DE",
            ),
        )

    @mock.patch("grpc.aio.insecure_channel")
    async def test_connection_manager(
        self,
        _insecure_channel_mock: MagicMock,
        components: list[list[Component]],
        connections: list[list[ComponentConnection]],
        microgrid: MicrogridInfo,
    ) -> None:
        """Test microgrid api.

        Args:
            _insecure_channel_mock: insecure channel mock from `mock.patch`
            components: components
            connections: connections
            microgrid: the information about the microgrid
        """
        microgrid_client = MagicMock()
        microgrid_client.list_components = AsyncMock(side_effect=components)
        microgrid_client.list_connections = AsyncMock(side_effect=connections)
        microgrid_client.get_microgrid_info = AsyncMock(return_value=microgrid)

        with mock.patch(
            "frequenz.sdk.microgrid.connection_manager.MicrogridApiClient",
            return_value=microgrid_client,
        ):
            # Get instance without initializing git first.
            with pytest.raises(RuntimeError):
                connection_manager.get()

            tasks = [
                asyncio.create_task(
                    connection_manager.initialize("grpc://127.0.0.1:10001")
                ),
                asyncio.create_task(
                    connection_manager.initialize("grpc://127.0.0.1:10001")
                ),
            ]
            initialize_task = asyncio.wait(tasks, return_when=ALL_COMPLETED)

            # Check if we can get connection_manager after not full initialization
            with pytest.raises(RuntimeError):
                connection_manager.get()

            done, pending = await initialize_task
            assert len(pending) == 0
            assert len(done) == 2
            assertion_counter = 0
            for result in done:
                try:
                    result.result()
                except AssertionError:
                    assertion_counter += 1
            assert assertion_counter == 1

            # Initialization is over we should now get api
            api = connection_manager.get()
            assert api.api_client is microgrid_client

            graph = api.component_graph
            assert set(graph.components()) == set(components[0])
            assert set(graph.connections()) == set(connections[0])

            assert api.microgrid_id == microgrid.id
            assert api.location == microgrid.location

            # It should not be possible to initialize method once again
            with pytest.raises(AssertionError):
                await connection_manager.initialize("grpc://127.0.0.1:10001")

            api2 = connection_manager.get()

            assert api is api2
            graph = api2.component_graph
            assert set(graph.components()) == set(components[0])
            assert set(graph.connections()) == set(connections[0])

            assert api.microgrid_id == microgrid.id
            assert api.location == microgrid.location

    @mock.patch("grpc.aio.insecure_channel")
    async def test_connection_manager_another_method(
        self,
        _insecure_channel_mock: MagicMock,
        components: list[list[Component]],
        connections: list[list[ComponentConnection]],
        microgrid: MicrogridInfo,
    ) -> None:
        """Test if the api was not deallocated.

        Args:
            _insecure_channel_mock: insecure channel mock
            components: components
            connections: connections
            microgrid: the information about the microgrid
        """
        microgrid_client = MagicMock()
        microgrid_client.components = AsyncMock(return_value=[])
        microgrid_client.connections = AsyncMock(return_value=[])
        microgrid_client.get_metadata = AsyncMock(return_value=None)

        api = connection_manager.get()
        graph = api.component_graph
        assert set(graph.components()) == set(components[0])
        assert set(graph.connections()) == set(connections[0])

        assert api.microgrid_id == microgrid.id
        assert api.location == microgrid.location
