# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Grid connection point.

This module provides the `Grid` type, which represents a grid connection point
in a microgrid.
"""

import logging
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from frequenz.channels import Sender

from ..microgrid.component import Component
from ..microgrid.component._component import ComponentCategory
from . import Fuse
from .formula_engine._formula_engine_pool import FormulaEnginePool

if TYPE_CHECKING:
    # Break circular import
    from ..actor import ChannelRegistry, ComponentMetricRequest

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Grid:
    """A grid connection point."""

    fuse: Fuse
    """The fuse protecting the grid connection point."""

    _formula_pool: FormulaEnginePool
    """The formula engine pool to generate grid metrics."""


_GRID: Grid | None = None


def initialize(
    components: Iterable[Component],
    channel_registry: ChannelRegistry,
    resampler_subscription_sender: Sender[ComponentMetricRequest],
) -> None:
    """Initialize the grid connection.

    Args:
        components: The components in the microgrid.
        channel_registry: The channel registry instance shared with the
            resampling actor.
        resampler_subscription_sender: The sender for sending metric requests
            to the resampling actor.

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
        _logger.info(
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

        namespace = f"grid-{uuid.uuid4()}"
        formula_pool = FormulaEnginePool(
            namespace,
            channel_registry,
            resampler_subscription_sender,
        )

        _GRID = Grid(fuse, formula_pool)


def get() -> Grid | None:
    """Get the grid connection.

    Note that a microgrid configured as an island will not have a grid
    connection point. For such microgrids, this function will return `None`.

    Returns:
        The grid connection.
    """
    return _GRID
