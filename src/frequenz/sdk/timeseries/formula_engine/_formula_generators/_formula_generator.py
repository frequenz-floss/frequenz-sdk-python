# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Base class for formula generators that use the component graphs."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from collections import abc
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic

from frequenz.channels import Sender
from frequenz.client.microgrid import Component, ComponentCategory, ComponentMetricId

from ...._internal._channels import ChannelRegistry
from ....microgrid import connection_manager
from ....microgrid._data_sourcing import ComponentMetricRequest
from ..._base_types import QuantityT
from .._formula_engine import FormulaEngine, FormulaEngine3Phase
from .._resampled_formula_builder import ResampledFormulaBuilder


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

    allow_fallback: bool = True


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
        self._channel_registry: ChannelRegistry = channel_registry
        self._resampler_subscription_sender: Sender[ComponentMetricRequest] = (
            resampler_subscription_sender
        )
        self._namespace: str = namespace
        self._config: FormulaGeneratorConfig = config

    @property
    def namespace(self) -> str:
        """Get the namespace for the formula generator."""
        return self._namespace

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

    def _get_grid_component(self) -> Component:
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
                    component_categories={ComponentCategory.GRID}
                )
            ),
            None,
        )
        if grid_component is None:
            raise ComponentNotFound("Grid component not found in the component graph.")

        return grid_component

    def _get_grid_component_successors(self) -> set[Component]:
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

    def _get_metric_fallback_components(
        self, components: set[Component]
    ) -> dict[Component, set[Component]]:
        """Get primary and fallback components within a given set of components.

        When a meter is positioned before one or more components of the same type (e.g., inverters),
        it is considered the primary component, and the components that follow are treated
        as fallback components.
        If the non-meter component has no meter in front of it, then it is the primary component
        and has no fallbacks.

        The method iterates through the provided components and assesses their roles as primary
        or fallback components.
        If a component:
         * can act as a primary component (e.g., a meter), then it finds its
        fallback components and pairs them together.
         * can act as a fallback (e.g., an inverter or EV charger), then it finds
        the primary component for it (usually a meter) and pairs them together.
         * has no fallback (e.g., an inverter that has no meter attached), then it
        returns an empty set for that component. This means that the component
        is a primary component and has no fallbacks.

        Args:
            components: The components to be analyzed.

        Returns:
            A dictionary where:
                * The keys are primary components.
                * The values are sets of fallback components.
        """
        graph = connection_manager.get().component_graph
        fallbacks: dict[Component, set[Component]] = {}

        for component in components:
            if component.category == ComponentCategory.METER:
                fallbacks[component] = self._get_meter_fallback_components(component)
            else:
                predecessors = graph.predecessors(component.component_id)
                if len(predecessors) == 1:
                    predecessor = predecessors.pop()
                    if self._is_primary_fallback_pair(predecessor, component):
                        # predecessor is primary component and the component is one of the
                        # fallbacks components.
                        fallbacks.setdefault(predecessor, set()).add(component)
                        continue

                # This component is primary component with no fallbacks.
                fallbacks[component] = set()
        return fallbacks

    def _get_meter_fallback_components(self, meter: Component) -> set[Component]:
        """Get the fallback components for a given meter.

        Args:
            meter: The meter to find the fallback components for.

        Returns:
            A set of fallback components for the given meter.
            An empty set is returned if the meter has no fallbacks.
        """
        assert meter.category == ComponentCategory.METER

        graph = connection_manager.get().component_graph
        successors = graph.successors(meter.component_id)

        # All fallbacks has to be of the same type and category.
        if (
            all(graph.is_chp(c) for c in successors)
            or all(graph.is_pv_inverter(c) for c in successors)
            or all(graph.is_battery_inverter(c) for c in successors)
            or all(graph.is_ev_charger(c) for c in successors)
        ):
            return successors
        return set()

    def _is_primary_fallback_pair(
        self,
        primary_candidate: Component,
        fallback_candidate: Component,
    ) -> bool:
        """Determine if a given component can act as a primary-fallback pair.

        This method checks:
         * whether the `fallback_candidate` is of a type that can have the `primary_candidate`,
         * if `primary_candidate` is the primary measuring point of the `fallback_candidate`.

        Args:
            primary_candidate: The component to be checked as a primary measuring device.
            fallback_candidate: The component to be checked as a fallback measuring device.

        Returns:
            bool: True if the provided components are a primary-fallback pair, False otherwise.
        """
        graph = connection_manager.get().component_graph

        # reassign to decrease the length of the line and make code readable
        fallback = fallback_candidate
        primary = primary_candidate

        # fmt: off
        return (
            graph.is_pv_inverter(fallback) and graph.is_pv_meter(primary)
            or graph.is_chp(fallback) and graph.is_chp_meter(primary)
            or graph.is_ev_charger(fallback) and graph.is_ev_charger_meter(primary)
            or graph.is_battery_inverter(fallback) and graph.is_battery_meter(primary)
        )
        # fmt: on
