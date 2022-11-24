# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests of MicrogridApi"""
import asyncio
from asyncio.tasks import ALL_COMPLETED
from typing import List
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest

from frequenz.sdk.microgrid import _microgrid
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory


class TestMicrogridApi:
    """Test for MicropgridApi."""

    # ignore mypy: Untyped decorator makes function "components" untyped
    @pytest.fixture
    def components(self) -> List[List[Component]]:
        """Get components in the graph.

        Override this method to create a graph with different components.

        Returns:
            list of components in graph

        """
        components = [
            [
                Component(1, ComponentCategory.GRID),
                Component(3, ComponentCategory.JUNCTION),
                Component(4, ComponentCategory.METER),
                Component(5, ComponentCategory.METER),
                Component(6, ComponentCategory.PV_ARRAY),
                Component(7, ComponentCategory.METER),
                Component(8, ComponentCategory.INVERTER),
                Component(9, ComponentCategory.BATTERY),
                Component(10, ComponentCategory.METER),
                Component(11, ComponentCategory.INVERTER),
                Component(12, ComponentCategory.BATTERY),
            ],
            [
                Component(1, ComponentCategory.GRID),
                Component(3, ComponentCategory.JUNCTION),
                Component(4, ComponentCategory.METER),
                Component(7, ComponentCategory.METER),
                Component(8, ComponentCategory.INVERTER),
                Component(9, ComponentCategory.BATTERY),
            ],
        ]
        return components

    # ignore mypy: Untyped decorator makes function "components" untyped
    @pytest.fixture
    def connections(self) -> List[List[Connection]]:
        """Get connections between components in the graph.

        Override this method to create a graph with different connections.

        Returns:
            list of connections between components in graph

        """
        connections = [
            [
                Connection(1, 3),
                Connection(3, 4),
                Connection(3, 5),
                Connection(5, 6),
                Connection(3, 7),
                Connection(7, 8),
                Connection(8, 9),
                Connection(3, 10),
                Connection(10, 11),
                Connection(11, 12),
            ],
            [
                Connection(1, 3),
                Connection(3, 4),
                Connection(3, 7),
                Connection(7, 8),
                Connection(8, 9),
            ],
        ]
        return connections

    @mock.patch("grpc.aio.insecure_channel")
    async def test_microgrid_api(
        self,
        _: MagicMock,
        components: List[List[Component]],
        connections: List[List[Connection]],
    ) -> None:
        """Test microgrid api.

        Args:
            _: insecure channel mock from `mock.patch`
            components: components
            connections: connections
        """
        microgrid_client = MagicMock()
        microgrid_client.components = AsyncMock(side_effect=components)
        microgrid_client.connections = AsyncMock(side_effect=connections)

        with mock.patch(
            "frequenz.sdk.microgrid._microgrid.MicrogridGrpcClient",
            return_value=microgrid_client,
        ):
            # Get instance without initializing git first.
            with pytest.raises(RuntimeError):
                _microgrid.get()

            tasks = [
                _microgrid.initialize("127.0.0.1", 10001),
                _microgrid.initialize("127.0.0.1", 10001),
            ]
            initialize_task = asyncio.wait(tasks, return_when=ALL_COMPLETED)

            # Check if we can get _microgrid after not full initialization
            with pytest.raises(RuntimeError):
                _microgrid.get()

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
            api = _microgrid.get()
            assert api.api_client is microgrid_client

            graph = api.component_graph
            assert set(graph.components()) == set(components[0])
            assert set(graph.connections()) == set(connections[0])

            # It should not be possible to initialize method once again
            with pytest.raises(AssertionError):
                await _microgrid.initialize("127.0.0.1", 10001)

            api2 = _microgrid.get()

            assert api is api2
            graph = api2.component_graph
            assert set(graph.components()) == set(components[0])
            assert set(graph.connections()) == set(connections[0])

    @mock.patch("grpc.aio.insecure_channel")
    async def test_microgrid_api_from_another_method(
        self,
        _: MagicMock,
        components: List[List[Component]],
        connections: List[List[Connection]],
    ) -> None:
        """Test if the api was not deallocated.

        Args:
            _: insecure channel mock
            components: components
            connections: connections
        """

        microgrid_client = MagicMock()
        microgrid_client.components = AsyncMock(return_value=[])
        microgrid_client.connections = AsyncMock(return_value=[])

        api = _microgrid.get()
        graph = api.component_graph
        assert set(graph.components()) == set(components[0])
        assert set(graph.connections()) == set(connections[0])
