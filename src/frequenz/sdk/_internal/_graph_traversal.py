# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Graph traversal helpers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Callable

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    BatteryInverter,
    Chp,
    Component,
    ComponentConnection,
    EvCharger,
    GridConnectionPoint,
    SolarInverter,
)
from frequenz.microgrid_component_graph import ComponentGraph, InvalidGraphError


def is_pv_inverter(component: Component) -> bool:
    """Check if the component is a PV inverter.

    Args:
        component: The component to check.

    Returns:
        `True` if the component is a PV inverter, `False` otherwise.
    """
    return isinstance(component, SolarInverter)


def is_battery_inverter(component: Component) -> bool:
    """Check if the component is a battery inverter.

    Args:
        component: The component to check.

    Returns:
        `True` if the component is a battery inverter, `False` otherwise.
    """
    return isinstance(component, BatteryInverter)


def is_chp(component: Component) -> bool:
    """Check if the component is a CHP.

    Args:
        component: The component to check.

    Returns:
        `True` if the component is a CHP, `False` otherwise.
    """
    return isinstance(component, Chp)


def is_ev_charger(component: Component) -> bool:
    """Check if the component is an EV charger.

    Args:
        component: The component to check.

    Returns:
        `True` if the component is an EV charger, `False` otherwise.
    """
    return isinstance(component, EvCharger)


def is_battery_chain(
    graph: ComponentGraph[Component, ComponentConnection, ComponentId],
    component: Component,
) -> bool:
    """Check if the specified component is part of a battery chain.

    A component is part of a battery chain if it is either a battery inverter or a
    battery meter.

    Args:
        graph: The component graph.
        component: component to check.

    Returns:
        Whether the specified component is part of a battery chain.
    """
    return is_battery_inverter(component) or graph.is_battery_meter(component.id)


def is_pv_chain(
    graph: ComponentGraph[Component, ComponentConnection, ComponentId],
    component: Component,
) -> bool:
    """Check if the specified component is part of a PV chain.

    A component is part of a PV chain if it is either a PV inverter or a PV
    meter.

    Args:
        graph: The component graph.
        component: component to check.

    Returns:
        Whether the specified component is part of a PV chain.
    """
    return is_pv_inverter(component) or graph.is_pv_meter(component.id)


def is_ev_charger_chain(
    graph: ComponentGraph[Component, ComponentConnection, ComponentId],
    component: Component,
) -> bool:
    """Check if the specified component is part of an EV charger chain.

    A component is part of an EV charger chain if it is either an EV charger or an
    EV charger meter.

    Args:
        graph: The component graph.
        component: component to check.

    Returns:
        Whether the specified component is part of an EV charger chain.
    """
    return is_ev_charger(component) or graph.is_ev_charger_meter(component.id)


def is_chp_chain(
    graph: ComponentGraph[Component, ComponentConnection, ComponentId],
    component: Component,
) -> bool:
    """Check if the specified component is part of a CHP chain.

    A component is part of a CHP chain if it is either a CHP or a CHP meter.

    Args:
        graph: The component graph.
        component: component to check.

    Returns:
        Whether the specified component is part of a CHP chain.
    """
    return is_chp(component) or graph.is_chp_meter(component.id)


def dfs(
    graph: ComponentGraph[Component, ComponentConnection, ComponentId],
    current_node: Component,
    visited: set[Component],
    condition: Callable[[Component], bool],
) -> set[Component]:
    """
    Search for components that fulfill the condition in the Graph.

    DFS is used for searching the graph. The graph traversal is stopped
    once a component fulfills the condition.

    Args:
        graph: The component graph.
        current_node: The current node to search from.
        visited: The set of visited nodes.
        condition: The condition function to check for.

    Returns:
        A set of component ids where the corresponding components fulfill
        the condition function.
    """
    if current_node in visited:
        return set()

    visited.add(current_node)

    if condition(current_node):
        return {current_node}

    component: set[Component] = set()

    for successor in graph.successors(current_node.id):
        component.update(dfs(graph, successor, visited, condition))

    return component


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
