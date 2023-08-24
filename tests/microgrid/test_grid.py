# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""
Tests for the `Grid` module.
"""

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import Component, ComponentCategory, GridMetadata
from frequenz.sdk.microgrid.grid import Grid


async def test_grid() -> None:
    """Test the grid connection module."""

    # The tests here need to be in this exact sequence, because the grid connection
    # is a singleton. Once it gets created, it stays in memory for the duration of
    # the tests, unless we explicitly delete it.

    # validate that islands with no grid connection are accepted.
    components = [
        Component(2, ComponentCategory.METER),
    ]

    microgrid.grid.initialize(components)

    grid = microgrid.grid.get()
    assert grid is None

    # validate that the microgrid initialization fails when there are multiple
    # grid connection points.
    components = [
        Component(1, ComponentCategory.GRID, None, GridMetadata(123.0)),
        Component(2, ComponentCategory.GRID, None, GridMetadata(345.0)),
        Component(3, ComponentCategory.METER),
    ]

    try:
        microgrid.grid.initialize(components)
        assert False, "Expected microgrid.grid.initialize to raise a RuntimeError."
    except RuntimeError:
        pass

    grid = microgrid.grid.get()
    assert grid is None

    # validate that microgrids with one grid connection are accepted.
    components = [
        Component(1, ComponentCategory.GRID, None, GridMetadata(123.0)),
        Component(2, ComponentCategory.METER),
    ]

    microgrid.grid.initialize(components)

    grid = microgrid.grid.get()
    assert grid == Grid(max_current=123.0)

    max_current = grid.max_current
    assert max_current == 123.0
