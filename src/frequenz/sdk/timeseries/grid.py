# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Grid connection point.

This module provides the `Grid` type, which represents a grid connection point
in a microgrid.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from frequenz.channels import Sender

from ..microgrid import connection_manager
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
    channel_registry: ChannelRegistry,
    resampler_subscription_sender: Sender[ComponentMetricRequest],
) -> None:
    """Initialize the grid connection.

    Args:
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
        connection_manager.get().component_graph.components(
            component_category={ComponentCategory.GRID},
        )
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

        fuse: Fuse | None = None

        # The current implementation of the Component Graph fails to
        # effectively convert components from a dictionary representation to
        # the expected Component object.
        # Specifically for the component metadata, it hands back a dictionary
        # instead of the expected ComponentMetadata type.
        metadata = grid_connections[0].metadata
        if isinstance(metadata, dict):
            fuse_dict = metadata.get("fuse", None)
            fuse = Fuse(**fuse_dict) if fuse_dict else None

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
