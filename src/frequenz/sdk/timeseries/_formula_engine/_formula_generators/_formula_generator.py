# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Base class for formula generators that use the component graphs."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from collections import abc
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Generic

from frequenz.channels import Sender

from ....actor import ChannelRegistry, ComponentMetricRequest
from ....microgrid import component, connection_manager
from ....microgrid.component import ComponentMetricId
from ..._quantities import QuantityT
from .._formula_engine import FormulaEngine, FormulaEngine3Phase
from .._resampled_formula_builder import ResampledFormulaBuilder


class FormulaGenerationError(Exception):
    """An error encountered during formula generation from the component graph."""


class ComponentNotFound(FormulaGenerationError):
    """Indicates that a component required for generating a formula is not found."""


NON_EXISTING_COMPONENT_ID = sys.maxsize


class FormulaType(Enum):
    """Enum representing type of formula outputs."""

    PASSIVE_SIGN_CONVENTION = 1
    """Formula output will be signed values, following the passive sign convention, with
    consumption from the grid being positive and production to the grid being negative.
    """

    PRODUCTION = 2
    """Formula output will be unsigned values representing production to the grid.  When
    power is being consumed from the grid instead, this formula will output zero.
    """

    CONSUMPTION = 3
    """Formula output will be unsigned values representing consumption from the grid.
    When power is being produced to the grid instead, this formula will output zero.
    """


@dataclass(frozen=True)
class FormulaGeneratorConfig:
    """Config for formula generators."""

    component_ids: abc.Set[int] | None = None
    formula_type: FormulaType = FormulaType.PASSIVE_SIGN_CONVENTION


class FormulaGenerator(ABC, Generic[QuantityT]):
    """A class for generating formulas from the component graph."""

    def __init__(
        self,
        namespace: str,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        config: FormulaGeneratorConfig,
    ) -> None:
        """Create a `FormulaGenerator` instance.

        Args:
            namespace: A namespace to use with the data-pipeline.
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
            config: configs for the formula generator.
        """
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender
        self._namespace = namespace
        self._config = config

    def _get_builder(
        self,
        name: str,
        component_metric_id: ComponentMetricId,
        create_method: Callable[[float], QuantityT],
    ) -> ResampledFormulaBuilder[QuantityT]:
        builder = ResampledFormulaBuilder(
            self._namespace,
            name,
            self._channel_registry,
            self._resampler_subscription_sender,
            component_metric_id,
            create_method,
        )
        return builder

    def _get_grid_component(self) -> component.Component:
        """
        Get the grid component in the component graph.

        Returns:
            The first grid component found in the graph.

        Raises:
            ComponentNotFound: If the grid component is not found in the component graph.
        """
        component_graph = connection_manager.get().component_graph
        grid_component = next(
            iter(
                component_graph.components(
                    component_category={component.ComponentCategory.GRID}
                )
            ),
            None,
        )
        if grid_component is None:
            raise ComponentNotFound("Grid component not found in the component graph.")

        return grid_component

    def _get_grid_component_successors(self) -> set[component.Component]:
        """Get the set of grid component successors in the component graph.

        Returns:
            A set of grid component successors.

        Raises:
            ComponentNotFound: If no successor components are found in the component graph.
        """
        grid_component = self._get_grid_component()
        component_graph = connection_manager.get().component_graph
        grid_successors = component_graph.successors(grid_component.component_id)

        if not grid_successors:
            raise ComponentNotFound("No components found in the component graph.")

        return grid_successors

    @abstractmethod
    def generate(
        self,
    ) -> FormulaEngine[QuantityT] | FormulaEngine3Phase[QuantityT]:
        """Generate a formula engine, based on the component graph."""
