# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests of MicrogridApi."""

import asyncio
from asyncio.tasks import ALL_COMPLETED
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest
from frequenz.client.microgrid import (
    Component,
    ComponentCategory,
    Connection,
    Location,
    Metadata,
)

from frequenz.sdk.microgrid import connection_manager


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
                Component(1, ComponentCategory.GRID),
                Component(4, ComponentCategory.METER),
                Component(5, ComponentCategory.METER),
                Component(7, ComponentCategory.METER),
                Component(8, ComponentCategory.INVERTER),
                Component(9, ComponentCategory.BATTERY),
                Component(10, ComponentCategory.METER),
                Component(11, ComponentCategory.INVERTER),
                Component(12, ComponentCategory.BATTERY),
            ],
            [
                Component(1, ComponentCategory.GRID),
                Component(4, ComponentCategory.METER),
                Component(7, ComponentCategory.METER),
                Component(8, ComponentCategory.INVERTER),
                Component(9, ComponentCategory.BATTERY),
            ],
        ]
        return components

    # ignore mypy: Untyped decorator makes function "components" untyped
    @pytest.fixture
    def connections(self) -> list[list[Connection]]:
        """Get connections between components in the graph.

        Override this method to create a graph with different connections.

        Returns:
            list of connections between components in graph

        """
        connections = [
            [
                Connection(1, 4),
                Connection(1, 5),
                Connection(1, 7),
                Connection(7, 8),
                Connection(8, 9),
                Connection(1, 10),
                Connection(10, 11),
                Connection(11, 12),
            ],
            [
                Connection(1, 4),
                Connection(1, 7),
                Connection(7, 8),
                Connection(8, 9),
            ],
        ]
        return connections

    @pytest.fixture
    def metadata(self) -> Metadata:
        """Fetch the microgrid metadata.

        Returns:
            the microgrid metadata.
        """
        mock_timezone_finder = MagicMock()
        mock_timezone_finder.timezone_at.return_value = "Europe/Berlin"
        meta._timezone_finder = mock_timezone_finder  # pylint: disable=protected-access

        return Metadata(
            microgrid_id=8,
            location=meta.Location(latitude=52.520008, longitude=13.404954),
        )

    @mock.patch("grpc.aio.insecure_channel")
    async def test_connection_manager(
        self,
        _insecure_channel_mock: MagicMock,
        components: list[list[Component]],
        connections: list[list[Connection]],
        metadata: Metadata,
    ) -> None:
        """Test microgrid api.

        Args:
            _insecure_channel_mock: insecure channel mock from `mock.patch`
            components: components
            connections: connections
            metadata: the metadata of the microgrid
        """
        microgrid_client = MagicMock()
        microgrid_client.components = AsyncMock(side_effect=components)
        microgrid_client.connections = AsyncMock(side_effect=connections)
        microgrid_client.metadata = AsyncMock(return_value=metadata)

        with mock.patch(
            "frequenz.sdk.microgrid.connection_manager.ApiClient",
            return_value=microgrid_client,
        ):
            # Get instance without initializing git first.
            with pytest.raises(RuntimeError):
                connection_manager.get()

            tasks = [
                asyncio.create_task(connection_manager.initialize("127.0.0.1", 10001)),
                asyncio.create_task(connection_manager.initialize("127.0.0.1", 10001)),
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

            assert api.microgrid_id == metadata.microgrid_id
            assert api.location == metadata.location
            assert api.location and api.location.timezone
            assert api.location.timezone.key == "Europe/Berlin"

            # It should not be possible to initialize method once again
            with pytest.raises(AssertionError):
                await connection_manager.initialize("127.0.0.1", 10001)

            api2 = connection_manager.get()

            assert api is api2
            graph = api2.component_graph
            assert set(graph.components()) == set(components[0])
            assert set(graph.connections()) == set(connections[0])

            assert api.microgrid_id == metadata.microgrid_id
            assert api.location == metadata.location

    @mock.patch("grpc.aio.insecure_channel")
    async def test_connection_manager_another_method(
        self,
        _insecure_channel_mock: MagicMock,
        components: list[list[Component]],
        connections: list[list[Connection]],
        metadata: Metadata,
    ) -> None:
        """Test if the api was not deallocated.

        Args:
            _insecure_channel_mock: insecure channel mock
            components: components
            connections: connections
            metadata: the metadata of the microgrid
        """
        microgrid_client = MagicMock()
        microgrid_client.components = AsyncMock(return_value=[])
        microgrid_client.connections = AsyncMock(return_value=[])
        microgrid_client.get_metadata = AsyncMock(return_value=None)

        api = connection_manager.get()
        graph = api.component_graph
        assert set(graph.components()) == set(components[0])
        assert set(graph.connections()) == set(connections[0])

        assert api.microgrid_id == metadata.microgrid_id
        assert api.location == metadata.location
        assert api.location and api.location.timezone
        assert api.location.timezone.key == "Europe/Berlin"
