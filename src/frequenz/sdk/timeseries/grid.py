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
from frequenz.client.microgrid._component import ComponentCategory

from ..microgrid import connection_manager
from ._fuse import Fuse
from ._quantities import Current, Power
from .formula_engine import FormulaEngine, FormulaEngine3Phase
from .formula_engine._formula_engine_pool import FormulaEnginePool
from .formula_engine._formula_generators import (
    GridCurrentFormula,
    GridPower3PhaseFormula,
    GridPowerFormula,
)

if TYPE_CHECKING:
    # Break circular import
    from ..actor import ChannelRegistry, ComponentMetricRequest

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Grid:
    """A grid connection point.

    !!! note
        The `Grid` instance is not meant to be created directly by users.
        Use the [`microgrid.grid`][frequenz.sdk.microgrid.grid] method for
        creating or getting the `Grid` instance.

    Example:
        ```python
        from datetime import timedelta

        from frequenz.sdk import microgrid
        from frequenz.sdk.timeseries import ResamplerConfig

        await microgrid.initialize(
            "grpc://127.0.0.1:50051",
            ResamplerConfig(resampling_period=timedelta(seconds=1))
        )

        grid = microgrid.grid()

        # Get a receiver for a builtin formula
        grid_power_recv = grid.power.new_receiver()
        async for grid_power_sample in grid_power_recv:
            print(grid_power_sample)
        ```
    """

    fuse: Fuse | None
    """The fuse protecting the grid connection point.

    The rated current of the fuse is set to zero in case of an islanded
    microgrid.
    And the fuse is set to `None` when the grid connection component metadata
    lacks information about the fuse.
    """

    _formula_pool: FormulaEnginePool
    """The formula engine pool to generate grid metrics."""

    @property
    def power(self) -> FormulaEngine[Power]:
        """Fetch the grid power for the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate grid power is not already running, it will be
        started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream grid power.
        """
        engine = self._formula_pool.from_power_formula_generator(
            "grid_power",
            GridPowerFormula,
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def _power_3_phase(self) -> FormulaEngine3Phase[Power]:
        """Fetch the grid 3-phase power for the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        A receiver from the formula engine can be created using the
        `new_receiver`method.

        Returns:
            A FormulaEngine that will calculate and stream grid 3-phase power.
        """
        engine = self._formula_pool.from_power_3_phase_formula_generator(
            "grid_power_3_phase", GridPower3PhaseFormula
        )
        assert isinstance(engine, FormulaEngine3Phase)
        return engine

    @property
    def current(self) -> FormulaEngine3Phase[Current]:
        """Fetch the grid current for the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate grid current is not already running, it will be
        started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream grid current.
        """
        engine = self._formula_pool.from_3_phase_current_formula_generator(
            "grid_current",
            GridCurrentFormula,
        )
        assert isinstance(engine, FormulaEngine3Phase)
        return engine

    async def stop(self) -> None:
        """Stop all formula engines."""
        await self._formula_pool.stop()


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
            microgrid, or if the grid connection point is not initialized.
    """
    global _GRID  # pylint: disable=global-statement

    grid_connections = list(
        connection_manager.get().component_graph.components(
            component_categories={ComponentCategory.GRID},
        )
    )

    grid_connections_count = len(grid_connections)

    fuse: Fuse | None = None

    if grid_connections_count == 0:
        fuse = Fuse(max_current=Current.zero())
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

        # The current implementation of the Component Graph fails to
        # effectively convert components from a dictionary representation to
        # the expected Component object.
        # Specifically for the component metadata, it hands back a dictionary
        # instead of the expected ComponentMetadata type.
        metadata = grid_connections[0].metadata
        if isinstance(metadata, dict):
            if fuse_dict := metadata.get("fuse", None):
                fuse = Fuse(
                    max_current=Current.from_amperes(fuse_dict.get("max_current", 0.0))
                )

        if fuse is None:
            _logger.warning("The grid connection point does not have a fuse")

    namespace = f"grid-{uuid.uuid4()}"
    formula_pool = FormulaEnginePool(
        namespace,
        channel_registry,
        resampler_subscription_sender,
    )

    _GRID = Grid(fuse, formula_pool)


def get() -> Grid:
    """Get the grid connection.

    Note that the rated current of the fuse is set to zero in case of an
    islanded microgrid.
    And the fuse is set to `None` when the grid connection component metadata
    lacks information about the fuse.

    Returns:
        The grid connection.
    """
    assert _GRID, "Grid is not initialized"
    return _GRID
