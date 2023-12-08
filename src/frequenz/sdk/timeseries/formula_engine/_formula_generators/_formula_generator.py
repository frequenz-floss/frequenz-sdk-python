# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Base class for formula generators that use the component graphs."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from collections import abc
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic

from frequenz.channels import Sender

from ....microgrid import component, connection_manager
from ....microgrid.component import ComponentMetricId
from ..._base_types import Sample
from ..._quantities import QuantityT
from .._formula_engine import FormulaEngine, FormulaEngine3Phase
from .._resampled_formula_builder import ResampledFormulaBuilder

if TYPE_CHECKING:
    # Break circular import
    from ....actor import ChannelRegistry, ComponentMetricRequest


class FormulaGenerationError(Exception):
    """An error encountered during formula generation from the component graph."""


class ComponentNotFound(FormulaGenerationError):
    """Indicates that a component required for generating a formula is not found."""


NON_EXISTING_COMPONENT_ID = sys.maxsize
"""The component ID for non-existent components in the components graph.

The non-existing component ID is commonly used in scenarios where a formula
engine requires a component ID but there are no available components in the
graph to associate with it. Thus, the non-existing component ID is subscribed
instead so that the formula engine can send `None` or `0` values at the same
frequency as the other streams.
"""


@dataclass(frozen=True)
class FormulaGeneratorConfig:
    """Config for formula generators."""

    component_ids: abc.Set[int] | None = None
    """The component IDs to use for generating the formula."""


class FormulaGenerator(ABC, Generic[QuantityT]):
    """A class for generating formulas from the component graph."""

    def __init__(
        self,
        namespace: str,
        channel_registry: ChannelRegistry[Sample[QuantityT]],
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
        self._channel_registry: ChannelRegistry[Sample[QuantityT]] = channel_registry
        self._resampler_subscription_sender: Sender[
            ComponentMetricRequest
        ] = resampler_subscription_sender
        self._namespace: str = namespace
        self._config: FormulaGeneratorConfig = config

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
                    component_categories={component.ComponentCategory.GRID}
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
