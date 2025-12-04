# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Defines a graph representation of how microgrid components are connected.

The component graph is an approximate representation of the microgrid circuit,
abstracted to a level appropriate for higher-level monitoring and control.
Common use cases include:

* Combining component measurements to compute grid power or onsite load by using
  the graph structure to determine which measurements to aggregate

* Identifying which inverter(s) need to be engaged to charge or discharge
  a particular battery based on their connectivity in the graph

* Understanding which power flows in the microgrid are derived from green vs
  grey sources based on the component connectivity

The graph deliberately does not include all pieces of hardware placed in the microgrid,
instead limiting itself to just those that are needed to monitor and control the
flow of power.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable

import networkx as nx
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid import MicrogridApiClient
from frequenz.client.microgrid.component import (
    Battery,
    BatteryInverter,
    Chp,
    Component,
    ComponentCategory,
    ComponentConnection,
    EvCharger,
    GridConnectionPoint,
    Inverter,
    Meter,
    MismatchedCategoryComponent,
    SolarInverter,
    UnrecognizedComponent,
    UnspecifiedComponent,
)
from typing_extensions import override

_logger = logging.getLogger(__name__)

# pylint: disable=too-many-lines


# Constant to store the actual objects as data attached to the graph nodes and edges
_DATA_KEY = "data"


class InvalidGraphError(Exception):
    """Exception type that will be thrown if graph data is not valid."""


class ComponentGraph(ABC):
    """Interface for component graph implementations."""

    @abstractmethod
    def components(
        self,
        matching_ids: Iterable[ComponentId] | ComponentId | None = None,
        matching_types: Iterable[type[Component]] | type[Component] | None = None,
    ) -> set[Component]:
        """Fetch the components of the microgrid.

        Args:
            matching_ids: The component IDs that the components must match.
            matching_types: The component types that the components must match.

        Returns:
            The set of components currently connected to the microgrid, filtered by
                the provided `matching_ids` and `matching_types` values.
        """

    @abstractmethod
    def connections(
        self,
        matching_sources: Iterable[ComponentId] | ComponentId | None = None,
        matching_destinations: Iterable[ComponentId] | ComponentId | None = None,
    ) -> set[ComponentConnection]:
        """Fetch the connections between microgrid components.

        Args:
            matching_sources: The component IDs the connections' source must match.
            matching_destinations: The component IDs the connections' destination must match.

        Returns:
            The set of connections between components in the microgrid, filtered by
                the provided `matching_sources` and `matching_destinations` choices.
        """

    @abstractmethod
    def predecessors(self, component_id: ComponentId) -> set[Component]:
        """Fetch the graph predecessors of the specified component.

        Args:
            component_id: The IDs of the components whose predecessors should be
                fetched.

        Returns:
            The set of components that are predecessors of `component_id`, i.e. for
                which there is a connection from each of these components to
                `component_id`.

        Raises:
            KeyError: If the specified `component_id` is not in the graph.
        """

    @abstractmethod
    def successors(self, component_id: ComponentId) -> set[Component]:
        """Fetch the graph successors of the specified component.

        Args:
            component_id: The IDs of the components whose successors should be fetched.

        Returns:
            The set of components that are successors of `component_id`, i.e. for
                which there is a connection from `component_id` to each of these
                components.

        Raises:
            KeyError: If the specified `component_id` is not in the graph
        """

    @abstractmethod
    def is_grid_meter(self, component: Component) -> bool:
        """Check if the specified component is a grid meter.

        This is done by checking if the component is the only successor to the `Grid`
        component.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a grid meter.
        """

    @abstractmethod
    def is_pv_inverter(self, component: Component) -> bool:
        """Check if the specified component is a PV inverter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a PV inverter.
        """

    @abstractmethod
    def is_pv_meter(self, component: Component) -> bool:
        """Check if the specified component is a PV meter.

        This is done by checking if the component has only PV inverters as its
        successors.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a PV meter.
        """

    @abstractmethod
    def is_pv_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a PV chain.

        A component is part of a PV chain if it is a PV meter or a PV inverter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a PV chain.
        """

    @abstractmethod
    def is_battery_inverter(self, component: Component) -> bool:
        """Check if the specified component is a battery inverter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a battery inverter.
        """

    @abstractmethod
    def is_battery_meter(self, component: Component) -> bool:
        """Check if the specified component is a battery meter.

        This is done by checking if the component has only battery inverters as its
        predecessors.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a battery meter.
        """

    @abstractmethod
    def is_battery_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a battery chain.

        A component is part of a battery chain if it is a battery meter or a battery
        inverter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a battery chain.
        """

    @abstractmethod
    def is_ev_charger(self, component: Component) -> bool:
        """Check if the specified component is an EV charger.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is an EV charger.
        """

    @abstractmethod
    def is_ev_charger_meter(self, component: Component) -> bool:
        """Check if the specified component is an EV charger meter.

        This is done by checking if the component has only EV chargers as its
        successors.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is an EV charger meter.
        """

    @abstractmethod
    def is_ev_charger_chain(self, component: Component) -> bool:
        """Check if the specified component is part of an EV charger chain.

        A component is part of an EV charger chain if it is an EV charger meter or an
        EV charger.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of an EV charger chain.
        """

    @abstractmethod
    def is_chp(self, component: Component) -> bool:
        """Check if the specified component is a CHP.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a CHP.
        """

    @abstractmethod
    def is_chp_meter(self, component: Component) -> bool:
        """Check if the specified component is a CHP meter.

        This is done by checking if the component has only CHPs as its successors.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a CHP meter.
        """

    @abstractmethod
    def is_chp_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a CHP chain.

        A component is part of a CHP chain if it is a CHP meter or a CHP.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a CHP chain.
        """

    @abstractmethod
    def dfs(
        self,
        current_node: Component,
        visited: set[Component],
        condition: Callable[[Component], bool],
    ) -> set[Component]:
        """Search for components that fulfill the condition in the Graph.

        DFS is used for searching the graph. The graph traversal is stopped
        once a component fulfills the condition.

        Args:
            current_node: The current node to search from.
            visited: The set of visited nodes.
            condition: The condition function to check for.

        Returns:
            A set of component IDs where the corresponding components fulfill
                the `condition` function.
        """

    @abstractmethod
    def find_first_descendant_component(
        self,
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
            descendants: The descendant classes to search for the first
                descendant component in.

        Returns:
            The first descendant component found in the component graph,
                considering the specified `descendants` categories.
        """


class _MicrogridComponentGraph(
    ComponentGraph
):  # pylint: disable=too-many-public-methods
    """ComponentGraph implementation designed to work with the microgrid API.

    For internal-only use of the `microgrid` package.
    """

    def __init__(
        self,
        components: set[Component] | None = None,
        connections: set[ComponentConnection] | None = None,
    ) -> None:
        """Initialize the component graph.

        Args:
            components: The components to initialize the graph with. If set, must
                provide `connections` as well.
            connections: The connections to initialize the graph with. If set, must
                provide `components` as well.

        Raises:
            InvalidGraphError: If `components` and `connections` are not both `None`
                and either of them is either `None` or empty.
        """
        self._graph: nx.DiGraph[ComponentId] = nx.DiGraph()

        if components is None and connections is None:
            return

        if components is None or len(components) == 0:
            raise InvalidGraphError("Must provide components as well as connections")

        if connections is None or len(connections) == 0:
            raise InvalidGraphError("Must provide connections as well as components")

        self.refresh_from(components, connections)
        self.validate()

    @override
    def components(
        self,
        matching_ids: Iterable[ComponentId] | ComponentId | None = None,
        matching_types: Iterable[type[Component]] | type[Component] | None = None,
    ) -> set[Component]:
        """Fetch the components of the microgrid.

        Args:
            matching_ids: The component IDs that the components must match.
            matching_types: The component types that the components must match.

        Returns:
            The set of components currently connected to the microgrid, filtered by
                the provided `matching_ids` and `matching_types` values.
        """
        matching_ids = _comp_ids_to_iter(matching_ids)
        if isinstance(matching_types, type):
            matching_types = {matching_types}

        selection: Iterable[Component]
        selection_ids = (
            self._graph.nodes
            if matching_ids is None
            else set(matching_ids) & self._graph.nodes
        )
        selection = (self._graph.nodes[i][_DATA_KEY] for i in selection_ids)

        if matching_types is not None:
            selection = filter(
                lambda c: isinstance(c, tuple(matching_types)), selection
            )

        return set(selection)

    @override
    def connections(
        self,
        matching_sources: Iterable[ComponentId] | ComponentId | None = None,
        matching_destinations: Iterable[ComponentId] | ComponentId | None = None,
    ) -> set[ComponentConnection]:
        """Fetch the connections between microgrid components.

        Args:
            matching_sources: The component IDs that the connections' source must match.
            matching_destinations: The component IDs that the connections' destination
                must match.

        Returns:
            The set of connections between components in the microgrid, filtered by
                the provided `matching_sources` and `matching_destinations` choices.
        """
        matching_sources = _comp_ids_to_iter(matching_sources)
        matching_destinations = _comp_ids_to_iter(matching_destinations)
        selection: Iterable[tuple[ComponentId, ComponentId]]

        match (matching_sources, matching_destinations):
            case (None, None):
                selection = self._graph.edges
            case (None, _):
                selection = self._graph.in_edges(matching_destinations)
            case (_, None):
                selection = self._graph.out_edges(matching_sources)
            case (_, _):
                source_edges = self._graph.out_edges(matching_sources)
                destination_edges = self._graph.in_edges(matching_destinations)
                selection = set(source_edges).intersection(destination_edges)

        return set(self._graph.edges[i][_DATA_KEY] for i in selection)

    @override
    def predecessors(self, component_id: ComponentId) -> set[Component]:
        """Fetch the graph predecessors of the specified component.

        Args:
            component_id: The IDs of the components whose predecessors should be
                fetched.

        Returns:
            The set of components that are predecessors of `component_id`, i.e. for
                which there is a connection from each of these components to
                `component_id`.

        Raises:
            KeyError: If the specified `component_id` is not in the graph.
        """
        if component_id not in self._graph:
            raise KeyError(
                f"Component {component_id} not in graph, cannot get predecessors!"
            )

        predecessors_ids = self._graph.predecessors(component_id)

        return set(map(lambda idx: self._graph.nodes[idx][_DATA_KEY], predecessors_ids))

    @override
    def successors(self, component_id: ComponentId) -> set[Component]:
        """Fetch the graph successors of the specified component.

        Args:
            component_id: The IDs of the components whose successors should be fetched.

        Returns:
            The set of components that are successors of `component_id`, i.e. for
                which there is a connection from `component_id` to each of these
                components.

        Raises:
            KeyError: If the specified `component_id` is not in the graph
        """
        if component_id not in self._graph:
            raise KeyError(
                f"Component {component_id} not in graph, cannot get successors!"
            )

        successors_ids = self._graph.successors(component_id)

        return set(map(lambda idx: self._graph.nodes[idx][_DATA_KEY], successors_ids))

    def refresh_from(
        self,
        components: set[Component],
        connections: set[ComponentConnection],
        correct_errors: Callable[["_MicrogridComponentGraph"], None] | None = None,
    ) -> None:
        """Refresh the graph from the provided list of components and connections.

        This will completely overwrite the current graph data with the provided
        components and connections.

        Args:
            components: The components to initialize the graph with. If set, must
                provide `connections` as well.
            connections: The connections to initialize the graph with. If set, must
                provide `components` as well.
            correct_errors: The callback that, if set, will be invoked if the
                provided graph data is in any way invalid (it will attempt to
                correct the errors by inferring what the correct data should be).

        Raises:
            InvalidGraphError: If the provided `components` and `connections`
                do not form a valid component graph and `correct_errors` does
                not fix it.
        """
        issues: list[str] = []

        for connection in connections:
            issues.extend((self._validate_connection(connection)))
        for component in components:
            issues.extend((self._validate_component(component)))

        if issues:
            raise InvalidGraphError(f"Invalid component data: {', '.join(issues)}")

        new_graph: nx.DiGraph[ComponentId] = nx.DiGraph()
        new_graph.add_nodes_from(
            (component.id, {_DATA_KEY: component}) for component in components
        )

        # Store the original connection object in the edge data (third item in the
        # tuple) so that we can retrieve it later.
        new_graph.add_edges_from(
            (connection.source, connection.destination, {_DATA_KEY: connection})
            for connection in connections
        )

        # check if we can construct a valid ComponentGraph
        # from the new NetworkX graph data
        _provisional = _MicrogridComponentGraph()
        _provisional._graph = new_graph  # pylint: disable=protected-access
        if correct_errors is not None:
            try:
                _provisional.validate()
            except InvalidGraphError as err:
                _logger.warning("Attempting to fix invalid component data: %s", err)
                correct_errors(_provisional)

        try:
            _provisional.validate()
        except Exception as err:
            _logger.error("Failed to parse component graph: %s", err)
            raise InvalidGraphError(
                "Cannot populate component graph from provided input!"
            ) from err

        old_graph = self._graph
        self._graph = new_graph
        old_graph.clear()  # just in case any references remain, but should not

    def _validate_connection(self, connection: ComponentConnection) -> list[str]:
        """Check that the connection is valid.

        Args:
            connection: connection to validate.

        Returns:
            List of issues found with the connection.
        """
        issues: list[str] = []
        if connection.source == connection.destination:
            issues.append(f"Connection {connection} has same source and destination!")
        return issues

    def _validate_component(self, component: Component) -> list[str]:
        """Check that the component is valid.

        Args:
            component: component to validate.

        Returns:
            List of issues found with the component.
        """
        issues: list[str] = []
        if isinstance(component, UnspecifiedComponent):
            _logger.warning("Component %r has an unspecified category!", component)
        if isinstance(component, UnrecognizedComponent):
            issues.append(f"Component {component!r} has an unrecognized category!")
        if isinstance(component, MismatchedCategoryComponent):
            _logger.warning("Component %r has a mismatched category!", component)

        return issues

    async def refresh_from_client(
        self,
        client: MicrogridApiClient,
        correct_errors: Callable[["_MicrogridComponentGraph"], None] | None = None,
    ) -> None:
        """Refresh the contents of a component graph from the remote API.

        Args:
            client: The API client from which to fetch graph data
            correct_errors: The callback that, if set, will be invoked if the
                provided graph data is in any way invalid (it will attempt to
                correct the errors by inferring what the correct data should be).
        """
        components, connections = await asyncio.gather(
            client.list_components(),
            client.list_connections(),
        )

        self.refresh_from(set(components), set(connections), correct_errors)

    def validate(self) -> None:
        """Check that the component graph contains valid microgrid data."""
        self._validate_graph()
        self._validate_graph_root()
        self._validate_grid_endpoint()
        self._validate_intermediary_components()
        self._validate_leaf_components()

    @override
    def is_grid_meter(self, component: Component) -> bool:
        """Check if the specified component is a grid meter.

        This is done by checking if the component is the only successor to the `Grid`
        component.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a grid meter.
        """
        if component.category != ComponentCategory.METER:
            return False

        predecessors = self.predecessors(component.id)
        if len(predecessors) != 1:
            return False

        predecessor = next(iter(predecessors))
        if predecessor.category != ComponentCategory.GRID:
            return False

        grid_successors = self.successors(predecessor.id)
        return len(grid_successors) == 1

    @override
    def is_pv_inverter(self, component: Component) -> bool:
        """Check if the specified component is a PV inverter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a PV inverter.
        """
        return isinstance(component, SolarInverter)

    @override
    def is_pv_meter(self, component: Component) -> bool:
        """Check if the specified component is a PV meter.

        This is done by checking if the component has only PV inverters as its
        successors.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a PV meter.
        """
        successors = self.successors(component.id)
        return (
            isinstance(component, Meter)
            and not self.is_grid_meter(component)
            and len(successors) > 0
            and all(
                self.is_pv_inverter(successor)
                for successor in self.successors(component.id)
            )
        )

    @override
    def is_pv_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a PV chain.

        A component is part of a PV chain if it is either a PV inverter or a PV
        meter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a PV chain.
        """
        return self.is_pv_inverter(component) or self.is_pv_meter(component)

    @override
    def is_ev_charger(self, component: Component) -> bool:
        """Check if the specified component is an EV charger.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is an EV charger.
        """
        return isinstance(component, EvCharger)

    @override
    def is_ev_charger_meter(self, component: Component) -> bool:
        """Check if the specified component is an EV charger meter.

        This is done by checking if the component has only EV chargers as its
        successors.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is an EV charger meter.
        """
        successors = self.successors(component.id)
        return (
            isinstance(component, Meter)
            and not self.is_grid_meter(component)
            and len(successors) > 0
            and all(self.is_ev_charger(successor) for successor in successors)
        )

    @override
    def is_ev_charger_chain(self, component: Component) -> bool:
        """Check if the specified component is part of an EV charger chain.

        A component is part of an EV charger chain if it is either an EV charger or an
        EV charger meter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of an EV charger chain.
        """
        return self.is_ev_charger(component) or self.is_ev_charger_meter(component)

    @override
    def is_battery_inverter(self, component: Component) -> bool:
        """Check if the specified component is a battery inverter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a battery inverter.
        """
        return isinstance(component, BatteryInverter)

    @override
    def is_battery_meter(self, component: Component) -> bool:
        """Check if the specified component is a battery meter.

        This is done by checking if the component has only battery inverters as
        its successors.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a battery meter.
        """
        successors = self.successors(component.id)
        return (
            isinstance(component, Meter)
            and not self.is_grid_meter(component)
            and len(successors) > 0
            and all(self.is_battery_inverter(successor) for successor in successors)
        )

    @override
    def is_battery_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a battery chain.

        A component is part of a battery chain if it is either a battery inverter or a
        battery meter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a battery chain.
        """
        return self.is_battery_inverter(component) or self.is_battery_meter(component)

    @override
    def is_chp(self, component: Component) -> bool:
        """Check if the specified component is a CHP.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a CHP.
        """
        return isinstance(component, Chp)

    @override
    def is_chp_meter(self, component: Component) -> bool:
        """Check if the specified component is a CHP meter.

        This is done by checking if the component has only CHPs as its
        successors.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a CHP meter.
        """
        successors = self.successors(component.id)
        return (
            isinstance(component, Meter)
            and not self.is_grid_meter(component)
            and len(successors) > 0
            and all(self.is_chp(successor) for successor in successors)
        )

    @override
    def is_chp_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a CHP chain.

        A component is part of a CHP chain if it is either a CHP or a CHP meter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a CHP chain.
        """
        return self.is_chp(component) or self.is_chp_meter(component)

    @override
    def dfs(
        self,
        current_node: Component,
        visited: set[Component],
        condition: Callable[[Component], bool],
    ) -> set[Component]:
        """Search for components that fulfill the condition in the Graph.

        DFS is used for searching the graph. The graph traversal is stopped
        once a component fulfills the condition.

        Args:
            current_node: The current node to search from.
            visited: The set of visited nodes.
            condition: The condition function to check for.

        Returns:
            A set of component IDs where the corresponding components fulfill
            the condition function.
        """
        if current_node in visited:
            return set()

        visited.add(current_node)

        if condition(current_node):
            return {current_node}

        component: set[Component] = set()

        for successor in self.successors(current_node.id):
            component.update(self.dfs(successor, visited, condition))

        return component

    @override
    def find_first_descendant_component(
        self,
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
            iter(sorted_by_id(self.components(matching_types={GridConnectionPoint}))),
            None,
        )
        if root_component is None:
            raise InvalidGraphError(
                "No GridConnectionPoint component found in the component graph!"
            )

        successors = sorted_by_id(self.successors(root_component.id))

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

    def _validate_graph(self) -> None:
        """Check that the underlying graph data is valid.

        Raises:
            InvalidGraphError: If:
                - There are no components.
                - There are no connections.
                - The graph is not a tree.
                - Any node lacks its associated component data.
        """
        if self._graph.number_of_nodes() == 0:
            raise InvalidGraphError("No components in graph!")

        if self._graph.number_of_edges() == 0:
            raise InvalidGraphError("No connections in component graph!")

        if not nx.is_directed_acyclic_graph(self._graph):
            raise InvalidGraphError("Component graph is not a tree!")

        # This check doesn't seem to have much sense, it only search for nodes without
        # data associated with them. We leave it here for now, but we should consider
        # removing it in the future.
        if undefined := [
            node[0] for node in self._graph.nodes(data=True) if len(node[1]) == 0
        ]:
            undefined_str = ", ".join(map(str, map(int, sorted(undefined))))
            raise InvalidGraphError(
                "Some component IDs found in connections are missing a "
                f"component definition: {undefined_str}"
            )

        # should be true as a consequence of checks above
        if sum(1 for _ in self.components()) <= 0:
            raise InvalidGraphError("Graph must have a least one component!")
        if sum(1 for _ in self.connections()) <= 0:
            raise InvalidGraphError("Graph must have a least one connection!")

        # should be true as a consequence of the tree property:
        # there should be no unconnected components
        unconnected = filter(lambda c: self._graph.degree(c.id) == 0, self.components())
        if sum(1 for _ in unconnected) != 0:
            raise InvalidGraphError(
                "Every component must have at least one connection!"
            )

    def _validate_graph_root(self) -> None:
        """Check that there is exactly one node without predecessors, of valid type.

        Raises:
            InvalidGraphError: If there is more than one node without predecessors,
                or if there is a single such node that is not one of NONE or GRID.
        """
        no_predecessors = filter(
            lambda c: self._graph.in_degree(c.id) == 0,
            self.components(),
        )

        valid_roots = list(
            filter(
                lambda c: isinstance(c, (GridConnectionPoint, UnspecifiedComponent)),
                no_predecessors,
            )
        )

        if len(valid_roots) == 0:
            raise InvalidGraphError("No valid root nodes of component graph!")

        if len(valid_roots) > 1:
            root_nodes = ", ".join(map(str, sorted(valid_roots, key=lambda c: c.id)))
            raise InvalidGraphError(f"Multiple potential root nodes: {root_nodes}")

        root = valid_roots[0]
        if self._graph.out_degree(root.id) == 0:
            raise InvalidGraphError(f"Graph root {root} has no successors!")

    def _validate_grid_endpoint(self) -> None:
        """Check that the grid endpoint is configured correctly in the graph.

        Raises:
            InvalidGraphError: If there is more than one grid endpoint in the
                graph, or if the grid endpoint has predecessors (if it exists,
                then it should be the root of the component-graph tree), or if
                it has no successors in the graph (i.e. it is not connected to
                anything).
        """
        grid = list(self.components(matching_types={GridConnectionPoint}))

        if len(grid) == 0:
            # it's OK to not have a grid endpoint as long as other properties
            # (checked by other `_validate...` methods) hold
            return

        if len(grid) > 1:
            raise InvalidGraphError(
                f"Multiple grid endpoints in component graph: {grid}"
            )

        grid_id = grid[0].id
        if self._graph.in_degree(grid_id) > 0:
            pred = ", ".join(
                map(str, sorted(self.predecessors(grid_id), key=lambda c: c.id))
            )
            raise InvalidGraphError(f"Grid endpoint {grid_id} has predecessors: {pred}")

        if self._graph.out_degree(grid_id) == 0:
            raise InvalidGraphError(f"Grid endpoint {grid_id} has no graph successors!")

    def _validate_intermediary_components(self) -> None:
        """Check that intermediary components (e.g. meters) are configured correctly.

        Intermediary components are components that should have both predecessors and
        successors in the component graph, such as METER, or INVERTER.

        Raises:
            InvalidGraphError: If any intermediary component has zero predecessors
                or zero successors.
        """
        intermediary_components = list(self.components(matching_types={Inverter}))

        missing_predecessors = list(
            filter(
                lambda c: sum(1 for _ in self.predecessors(c.id)) == 0,
                intermediary_components,
            )
        )
        if len(missing_predecessors) > 0:
            raise InvalidGraphError(
                "Intermediary components without graph predecessors: "
                f"{list(map(str, missing_predecessors))}"
            )

    def _validate_leaf_components(self) -> None:
        """Check that leaf components (e.g. batteries) are configured correctly.

        Leaf components are components that should be leaves of the component-graph
        tree, such as LOAD, BATTERY or EV_CHARGER.  These should have only incoming
        connections and no outgoing connections.

        Raises:
            InvalidGraphError: If any leaf component in the graph has 0 predecessors,
                or has > 0 successors.
        """
        leaf_components = list(
            self.components(
                matching_types={
                    Battery,
                    EvCharger,
                }
            )
        )

        missing_predecessors = list(
            filter(
                lambda c: sum(1 for _ in self.predecessors(c.id)) == 0,
                leaf_components,
            )
        )
        if len(missing_predecessors) > 0:
            raise InvalidGraphError(
                f"Leaf components without graph predecessors: {missing_predecessors}"
            )

        with_successors = list(
            filter(
                lambda c: sum(1 for _ in self.successors(c.id)) > 0,
                leaf_components,
            )
        )
        if len(with_successors) > 0:
            raise InvalidGraphError(
                f"Leaf components with graph successors: {with_successors}"
            )

    @override
    def __repr__(self) -> str:
        """Return a string representation of the component graph."""
        return f"ComponentGraph({self._graph!r})"


def _comp_ids_to_iter(
    ids: Iterable[ComponentId] | ComponentId | None,
) -> Iterable[ComponentId] | None:
    if isinstance(ids, ComponentId):
        return (ids,)
    return ids
