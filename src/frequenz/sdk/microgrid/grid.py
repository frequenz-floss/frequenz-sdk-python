# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Grid connection point.

This module provides the `Grid` type, which represents a grid connection point
in a microgrid.
"""

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from .component import Component
from .component._component import ComponentCategory
from .fuse import Fuse


@dataclass(frozen=True)
class Grid:
    """A grid connection point."""

    fuse: Fuse
    """The fuse protecting the grid connection point."""


_GRID: Grid | None = None


def initialize(components: Iterable[Component]) -> None:
    """Initialize the grid connection.

    Args:
        components: The components in the microgrid.

    Raises:
        RuntimeError: If there is more than 1 grid connection point in the
            microgrid, or if the grid connection point is not initialized,
            or if the grid connection point does not have a fuse.
    """
    global _GRID  # pylint: disable=global-statement

    grid_connections = list(
        component
        for component in components
        if component.category == ComponentCategory.GRID
    )

    grid_connections_count = len(grid_connections)

    if grid_connections_count == 0:
        logging.info(
            "No grid connection found for this microgrid. This is normal for an islanded microgrid."
        )
    elif grid_connections_count > 1:
        raise RuntimeError(
            f"Expected at most one grid connection, got {grid_connections_count}"
        )
    else:
        if grid_connections[0].metadata is None:
            raise RuntimeError("Grid metadata is None")

        fuse = grid_connections[0].metadata.fuse

        if fuse is None:
            raise RuntimeError("Grid fuse is None")

        _GRID = Grid(fuse)


def get() -> Grid | None:
    """Get the grid connection.

    Note that a microgrid configured as an island will not have a grid
    connection point. For such microgrids, this function will return `None`.

    Returns:
        The grid connection.
    """
    return _GRID
