# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Graph traversal helpers."""

from __future__ import annotations

from collections.abc import Iterable

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    Component,
    ComponentConnection,
    GridConnectionPoint,
)
from frequenz.microgrid_component_graph import ComponentGraph, InvalidGraphError


def find_first_descendant_component(
    graph: ComponentGraph[Component, ComponentConnection, ComponentId],
    *,
    descendants: Iterable[type[Component]],
) -> Component:
    """Find the first descendant component given root and descendant categories.

    This method looks for the first descendant component from the GRID
    component, considering only the immediate descendants.

    The priority of the component to search for is determined by the order
    of the descendant categories, with the first category having the
    highest priority.

    Args:
        graph: The component graph to search.
        descendants: The descendant classes to search for the first
            descendant component in.

    Returns:
        The first descendant component found in the component graph,
            considering the specified `descendants` categories.

    Raises:
        InvalidGraphError: When no GRID component is found in the graph.
        ValueError: When no component is found in the given categories.
    """
    # We always sort by component ID to ensure consistent results

    def sorted_by_id(components: Iterable[Component]) -> Iterable[Component]:
        return sorted(components, key=lambda c: c.id)

    root_component = next(
        iter(sorted_by_id(graph.components(matching_types={GridConnectionPoint}))),
        None,
    )
    if root_component is None:
        raise InvalidGraphError(
            "No GridConnectionPoint component found in the component graph!"
        )

    successors = sorted_by_id(graph.successors(root_component.id))

    def find_component(component_class: type[Component]) -> Component | None:
        return next(
            (comp for comp in successors if isinstance(comp, component_class)),
            None,
        )

    # Find the first component that matches the given descendant categories
    # in the order of the categories list.
    component = next(filter(None, map(find_component, descendants)), None)

    if component is None:
        raise ValueError("Component not found in any of the descendant categories.")

    return component
