# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `Grid` module."""

from pytest_mock import MockerFixture

import frequenz.sdk.microgrid.component_graph as gr
from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory, GridMetadata
from frequenz.sdk.timeseries import Current, Fuse

from ..timeseries.mock_microgrid import MockMicrogrid


async def test_grid_1(mocker: MockerFixture) -> None:
    """Test the grid connection module."""
    # The tests here need to be in this exact sequence, because the grid connection
    # is a singleton. Once it gets created, it stays in memory for the duration of
    # the tests, unless we explicitly delete it.

    # validate that islands with no grid connection are accepted.
    components = {
        Component(1, ComponentCategory.NONE),
        Component(2, ComponentCategory.METER),
    }
    connections = {
        Connection(1, 2),
    }
    # pylint: disable=protected-access
    graph = gr._MicrogridComponentGraph(components=components, connections=connections)

    mockgrid = MockMicrogrid(graph=graph)
    await mockgrid.start(mocker)
    grid = microgrid.grid()

    assert grid is None
    await mockgrid.cleanup()


def _create_fuse() -> Fuse:
    """Create a fuse with a fixed current.

    Returns:
        Fuse: The fuse.
    """
    fuse_current = Current.from_amperes(123.0)
    fuse = Fuse(fuse_current)
    return fuse


async def test_grid_2(mocker: MockerFixture) -> None:
    """Validate that microgrids with one grid connection are accepted."""
    components = {
        Component(1, ComponentCategory.GRID, None, GridMetadata(_create_fuse())),
        Component(2, ComponentCategory.METER),
    }
    connections = {
        Connection(1, 2),
    }

    # pylint: disable=protected-access
    graph = gr._MicrogridComponentGraph(components=components, connections=connections)

    mockgrid = MockMicrogrid(graph=graph)
    await mockgrid.start(mocker)

    grid = microgrid.grid()
    assert grid is not None

    expected_fuse_current = Current.from_amperes(123.0)
    expected_fuse = Fuse(expected_fuse_current)

    assert grid.fuse == expected_fuse
