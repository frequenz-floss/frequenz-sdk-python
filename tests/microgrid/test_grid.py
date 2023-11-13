# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `Grid` module."""

from frequenz.sdk.microgrid.component import Component, ComponentCategory, GridMetadata
from frequenz.sdk.timeseries import Current, Fuse
from frequenz.sdk.timeseries import grid as _grid


async def test_grid_1() -> None:
    """Test the grid connection module."""
    # The tests here need to be in this exact sequence, because the grid connection
    # is a singleton. Once it gets created, it stays in memory for the duration of
    # the tests, unless we explicitly delete it.

    # validate that islands with no grid connection are accepted.
    components = [
        Component(2, ComponentCategory.METER),
    ]

    _grid.initialize(components)
    grid = _grid.get()
    assert grid is None


def _create_fuse() -> Fuse:
    """Create a fuse with a fixed current.

    Returns:
        Fuse: The fuse.
    """
    fuse_current = Current.from_amperes(123.0)
    fuse = Fuse(fuse_current)
    return fuse


async def test_grid_2() -> None:
    """Test the grid connection module, slightly more complex.

    Validate that the microgrid initialization fails
    when there are multiple grid connection points.
    """
    fuse = _create_fuse()

    components = [
        Component(1, ComponentCategory.GRID, None, GridMetadata(fuse)),
        Component(2, ComponentCategory.GRID, None, GridMetadata(fuse)),
        Component(3, ComponentCategory.METER),
    ]

    try:
        _grid.initialize(components)
        assert False, "Expected microgrid.grid() to raise a RuntimeError."
    except RuntimeError:
        pass


async def test_grid_3() -> None:
    """Validate that microgrids with one grid connection are accepted."""
    components = [
        Component(1, ComponentCategory.GRID, None, GridMetadata(_create_fuse())),
        Component(2, ComponentCategory.METER),
    ]

    _grid.initialize(components)
    grid = _grid.get()
    assert grid is not None

    expected_fuse_current = Current.from_amperes(123.0)
    expected_fuse = Fuse(expected_fuse_current)

    assert grid == _grid.Grid(fuse=expected_fuse)

    fuse_current = grid.fuse.max_current
    assert fuse_current == expected_fuse_current
