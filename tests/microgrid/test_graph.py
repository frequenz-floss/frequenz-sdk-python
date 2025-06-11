# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the microgrid component graph."""

# pylint: disable=too-many-lines,use-implicit-booleaness-not-comparison
# pylint: disable=invalid-name,missing-function-docstring,too-many-statements
# pylint: disable=too-many-lines,protected-access

from unittest import mock

import pytest
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid import (
    Component,
    ComponentCategory,
    ComponentMetadata,
    Connection,
    Fuse,
    InverterType,
    MicrogridApiClient,
)

import frequenz.sdk.microgrid.component_graph as gr


def _add_components(graph: gr._MicrogridComponentGraph, *components: Component) -> None:
    """Add components to the test graph.

    Args:
        graph: The graph to add the components to.
        *components: The components to add.
    """
    graph._graph.add_nodes_from((c.component_id, {gr._DATA_KEY: c}) for c in components)


def _add_connections(
    graph: gr._MicrogridComponentGraph, *connections: Connection
) -> None:
    """Add connections to the test graph.

    Args:
        graph: The graph to add the connections to.
        *connections: The connections to add.
    """
    graph._graph.add_edges_from(
        (c.start, c.end, {gr._DATA_KEY: c}) for c in connections
    )


def _check_predecessors_and_successors(graph: gr.ComponentGraph) -> None:
    expected_predecessors: dict[ComponentId, set[Component]] = {}
    expected_successors: dict[ComponentId, set[Component]] = {}

    components: dict[ComponentId, Component] = {
        component.component_id: component for component in graph.components()
    }

    for conn in graph.connections():
        if conn.end not in expected_predecessors:
            expected_predecessors[conn.end] = set()
        expected_predecessors[conn.end].add(components[conn.start])

        if conn.start not in expected_successors:
            expected_successors[conn.start] = set()
        expected_successors[conn.start].add(components[conn.end])

    for component_id in components.keys():
        assert set(graph.predecessors(component_id)) == expected_predecessors.get(
            component_id, set()
        )
        assert set(graph.successors(component_id)) == expected_successors.get(
            component_id, set()
        )


class TestComponentGraph:
    """Test cases for the public ComponentGraph interface.

    The _MicrogridComponentGraph implementation is used with these tests,
    but the only methods tested are those exposed by ComponentGraph, i.e.
    those to query graph properties rather than set them.
    """

    @pytest.fixture()
    def sample_input_components(self) -> set[Component]:
        """Create a sample set of components for testing purposes."""
        return {
            Component(ComponentId(11), ComponentCategory.GRID),
            Component(ComponentId(21), ComponentCategory.METER),
            Component(ComponentId(41), ComponentCategory.METER),
            Component(ComponentId(51), ComponentCategory.INVERTER),
            Component(ComponentId(61), ComponentCategory.BATTERY),
        }

    @pytest.fixture()
    def sample_input_connections(self) -> set[Connection]:
        """Create a sample set of connections for testing purposes."""
        return {
            Connection(ComponentId(11), ComponentId(21)),
            Connection(ComponentId(21), ComponentId(41)),
            Connection(ComponentId(41), ComponentId(51)),
            Connection(ComponentId(51), ComponentId(61)),
        }

    @pytest.fixture()
    def sample_graph(
        self,
        sample_input_components: set[Component],
        sample_input_connections: set[Connection],
    ) -> gr.ComponentGraph:
        """Create a sample graph for testing purposes."""
        _graph_implementation = gr._MicrogridComponentGraph(
            components=sample_input_components,
            connections=sample_input_connections,
        )
        return _graph_implementation

    def test_without_filters(self) -> None:
        """Test the graph component query without filters."""
        _graph_implementation = gr._MicrogridComponentGraph()
        graph: gr.ComponentGraph = _graph_implementation

        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(
            KeyError,
            match="Component with CID1 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(ComponentId(1))
        with pytest.raises(
            KeyError,
            match="Component with CID1 not in graph, cannot get successors!",
        ):
            graph.successors(ComponentId(1))

        # simplest valid microgrid: a grid endpoint and a meter
        _graph_implementation.refresh_from(
            components={
                Component(ComponentId(1), ComponentCategory.GRID),
                Component(ComponentId(3), ComponentCategory.METER),
            },
            connections={Connection(ComponentId(1), ComponentId(3))},
        )
        expected_components = {
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(3), ComponentCategory.METER),
        }
        assert len(graph.components()) == len(expected_components)
        assert graph.components() == expected_components
        assert graph.connections() == {Connection(ComponentId(1), ComponentId(3))}

        assert graph.predecessors(ComponentId(1)) == set()
        assert graph.successors(ComponentId(1)) == {
            Component(ComponentId(3), ComponentCategory.METER)
        }
        assert graph.predecessors(ComponentId(3)) == {
            Component(ComponentId(1), ComponentCategory.GRID)
        }
        assert graph.successors(ComponentId(3)) == set()
        with pytest.raises(
            KeyError,
            match="Component with CID2 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(ComponentId(2))
        with pytest.raises(
            KeyError,
            match="Component with CID2 not in graph, cannot get successors!",
        ):
            graph.successors(ComponentId(2))

        input_components = {
            ComponentId(101): Component(ComponentId(101), ComponentCategory.GRID),
            ComponentId(102): Component(ComponentId(102), ComponentCategory.METER),
            ComponentId(104): Component(ComponentId(104), ComponentCategory.METER),
            ComponentId(105): Component(ComponentId(105), ComponentCategory.INVERTER),
            ComponentId(106): Component(ComponentId(106), ComponentCategory.BATTERY),
        }
        input_connections = {
            Connection(ComponentId(101), ComponentId(102)),
            Connection(ComponentId(102), ComponentId(104)),
            Connection(ComponentId(104), ComponentId(105)),
            Connection(ComponentId(105), ComponentId(106)),
        }

        # more complex microgrid: grid endpoint, load, grid-side meter,
        # and meter/inverter/battery setup
        _graph_implementation.refresh_from(
            components=set(input_components.values()),
            connections=input_connections,
        )

        assert len(graph.components()) == len(input_components.values())
        assert set(graph.components()) == set(input_components.values())
        assert graph.connections() == input_connections

        _check_predecessors_and_successors(graph=graph)

        with pytest.raises(
            KeyError,
            match="Component with CID9 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(ComponentId(9))
        with pytest.raises(
            KeyError,
            match="Component with CID99 not in graph, cannot get successors!",
        ):
            graph.successors(ComponentId(99))

    @pytest.mark.parametrize(
        "int_ids, expected",
        [
            ({1}, set()),
            ({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, set()),
            ({11}, {Component(ComponentId(11), ComponentCategory.GRID)}),
            ({21}, {Component(ComponentId(21), ComponentCategory.METER)}),
            ({41}, {Component(ComponentId(41), ComponentCategory.METER)}),
            ({51}, {Component(ComponentId(51), ComponentCategory.INVERTER)}),
            ({61}, {Component(ComponentId(61), ComponentCategory.BATTERY)}),
            (
                {11, 61},
                {
                    Component(ComponentId(11), ComponentCategory.GRID),
                    Component(ComponentId(61), ComponentCategory.BATTERY),
                },
            ),
            (
                {9, 51, 41, 21, 101},
                {
                    Component(ComponentId(41), ComponentCategory.METER),
                    Component(ComponentId(51), ComponentCategory.INVERTER),
                    Component(ComponentId(21), ComponentCategory.METER),
                },
            ),
        ],
    )
    def test_filter_graph_components_by_id(
        self,
        sample_graph: gr.ComponentGraph,
        int_ids: set[int],
        expected: set[Component],
    ) -> None:
        """Test the graph component query with component ID filter."""
        ids = set(ComponentId(id) for id in int_ids)
        # with component_id filter specified, we get back only components whose ID
        # matches one of the specified values
        assert len(sample_graph.components(component_ids=ids)) == len(expected)
        assert sample_graph.components(component_ids=ids) == expected

    @pytest.mark.parametrize(
        "types, expected",
        [
            ({ComponentCategory.EV_CHARGER}, set()),
            (
                {ComponentCategory.BATTERY, ComponentCategory.EV_CHARGER},
                {Component(ComponentId(61), ComponentCategory.BATTERY)},
            ),
            (
                {ComponentCategory.GRID},
                {Component(ComponentId(11), ComponentCategory.GRID)},
            ),
            (
                {ComponentCategory.METER},
                {
                    Component(ComponentId(21), ComponentCategory.METER),
                    Component(ComponentId(41), ComponentCategory.METER),
                },
            ),
            (
                {ComponentCategory.INVERTER},
                {Component(ComponentId(51), ComponentCategory.INVERTER)},
            ),
            (
                {ComponentCategory.BATTERY},
                {Component(ComponentId(61), ComponentCategory.BATTERY)},
            ),
            (
                {ComponentCategory.GRID, ComponentCategory.BATTERY},
                {
                    Component(ComponentId(11), ComponentCategory.GRID),
                    Component(ComponentId(61), ComponentCategory.BATTERY),
                },
            ),
            (
                {
                    ComponentCategory.METER,
                    ComponentCategory.BATTERY,
                    ComponentCategory.EV_CHARGER,
                },
                {
                    Component(ComponentId(21), ComponentCategory.METER),
                    Component(ComponentId(61), ComponentCategory.BATTERY),
                    Component(ComponentId(41), ComponentCategory.METER),
                },
            ),
        ],
    )
    def test_filter_graph_components_by_type(
        self,
        sample_graph: gr.ComponentGraph,
        types: set[ComponentCategory],
        expected: set[Component],
    ) -> None:
        """Test the graph component query with component category filter."""
        # with component_id filter specified, we get back only components whose ID
        # matches one of the specified values
        assert len(sample_graph.components(component_categories=types)) == len(expected)
        assert sample_graph.components(component_categories=types) == expected

    @pytest.mark.parametrize(
        "int_ids, types, expected",
        [
            (
                {11},
                {ComponentCategory.GRID},
                {Component(ComponentId(11), ComponentCategory.GRID)},
            ),
            ({31}, {ComponentCategory.GRID}, set()),
            (
                {61},
                {ComponentCategory.BATTERY},
                {Component(ComponentId(61), ComponentCategory.BATTERY)},
            ),
            (
                {11, 21, 31, 61},
                {ComponentCategory.METER, ComponentCategory.BATTERY},
                {
                    Component(ComponentId(61), ComponentCategory.BATTERY),
                    Component(ComponentId(21), ComponentCategory.METER),
                },
            ),
        ],
    )
    def test_filter_graph_components_with_composite_filter(
        self,
        sample_graph: gr.ComponentGraph,
        int_ids: set[int],
        types: set[ComponentCategory],
        expected: set[Component],
    ) -> None:
        """Test the graph component query with composite filter."""
        ids = set(ComponentId(id) for id in int_ids)
        # when both filters are applied, they are combined via AND logic, i.e.
        # the component must have one of the specified IDs and be of one of
        # the specified types
        assert len(
            sample_graph.components(component_ids=ids, component_categories=types)
        ) == len(expected)
        assert (
            set(sample_graph.components(component_ids=ids, component_categories=types))
            == expected
        )

    def test_components_without_filters(
        self, sample_input_components: set[Component], sample_graph: gr.ComponentGraph
    ) -> None:
        """Test the graph component query without filters."""
        # without any filter applied, we get back all the components in the graph
        assert len(sample_graph.components()) == len(sample_input_components)
        assert sample_graph.components() == sample_input_components

    def test_connection_filters(self) -> None:
        """Test the graph connection query with filters."""
        _graph_implementation = gr._MicrogridComponentGraph(
            components={
                Component(ComponentId(1), ComponentCategory.GRID),
                Component(ComponentId(2), ComponentCategory.METER),
                Component(ComponentId(3), ComponentCategory.METER),
                Component(ComponentId(4), ComponentCategory.EV_CHARGER),
                Component(ComponentId(5), ComponentCategory.EV_CHARGER),
                Component(ComponentId(6), ComponentCategory.EV_CHARGER),
            },
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(1), ComponentId(3)),
                Connection(ComponentId(2), ComponentId(4)),
                Connection(ComponentId(2), ComponentId(5)),
                Connection(ComponentId(2), ComponentId(6)),
            },
        )
        graph: gr.ComponentGraph = _graph_implementation

        # without any filter applied, we get back all the connections in the graph
        assert graph.connections() == {
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(1), ComponentId(3)),
            Connection(ComponentId(2), ComponentId(4)),
            Connection(ComponentId(2), ComponentId(5)),
            Connection(ComponentId(2), ComponentId(6)),
        }

        # with start filter applied, we get back only connections whose `start`
        # component matches one of the provided IDs
        assert graph.connections(start={ComponentId(8)}) == set()
        assert graph.connections(start={ComponentId(7)}) == set()
        assert graph.connections(start={ComponentId(6)}) == set()
        assert graph.connections(start={ComponentId(5)}) == set()
        assert graph.connections(start={ComponentId(4)}) == set()
        assert graph.connections(start={ComponentId(3)}) == set()
        assert graph.connections(start={ComponentId(2)}) == {
            Connection(ComponentId(2), ComponentId(4)),
            Connection(ComponentId(2), ComponentId(5)),
            Connection(ComponentId(2), ComponentId(6)),
        }
        assert graph.connections(start={ComponentId(1)}) == {
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(1), ComponentId(3)),
        }
        assert graph.connections(
            start={ComponentId(1), ComponentId(3), ComponentId(5)}
        ) == {
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(1), ComponentId(3)),
        }
        assert graph.connections(
            start={ComponentId(1), ComponentId(2), ComponentId(5), ComponentId(6)}
        ) == {
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(1), ComponentId(3)),
            Connection(ComponentId(2), ComponentId(4)),
            Connection(ComponentId(2), ComponentId(5)),
            Connection(ComponentId(2), ComponentId(6)),
        }

        # with end filter applied, we get back only connections whose `end`
        # component matches one of the provided IDs
        assert graph.connections(end={ComponentId(8)}) == set()
        assert graph.connections(end={ComponentId(6)}) == {
            Connection(ComponentId(2), ComponentId(6))
        }
        assert graph.connections(end={ComponentId(5)}) == {
            Connection(ComponentId(2), ComponentId(5))
        }
        assert graph.connections(end={ComponentId(4)}) == {
            Connection(ComponentId(2), ComponentId(4))
        }
        assert graph.connections(end={ComponentId(3)}) == {
            Connection(ComponentId(1), ComponentId(3))
        }
        assert graph.connections(end={ComponentId(2)}) == {
            Connection(ComponentId(1), ComponentId(2))
        }
        assert graph.connections(end={ComponentId(1)}) == set()
        assert graph.connections(
            end={ComponentId(1), ComponentId(2), ComponentId(3)}
        ) == {
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(1), ComponentId(3)),
        }
        assert graph.connections(
            end={ComponentId(4), ComponentId(5), ComponentId(6)}
        ) == {
            Connection(ComponentId(2), ComponentId(4)),
            Connection(ComponentId(2), ComponentId(5)),
            Connection(ComponentId(2), ComponentId(6)),
        }

        assert graph.connections(
            end={ComponentId(2), ComponentId(4), ComponentId(6), ComponentId(8)}
        ) == {
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(4)),
            Connection(ComponentId(2), ComponentId(6)),
        }
        assert graph.connections(end={ComponentId(1)}) == set()

        # when both filters are applied, they are combined via AND logic, i.e.
        # a connection must have its `start` matching one of the provided start
        # values, and its `end` matching one of the provided end values
        assert graph.connections(start={ComponentId(1)}, end={ComponentId(2)}) == {
            Connection(ComponentId(1), ComponentId(2))
        }
        assert graph.connections(start={ComponentId(2)}, end={ComponentId(3)}) == set()
        assert graph.connections(
            start={ComponentId(1), ComponentId(2)}, end={ComponentId(3), ComponentId(4)}
        ) == {
            Connection(ComponentId(1), ComponentId(3)),
            Connection(ComponentId(2), ComponentId(4)),
        }
        assert graph.connections(
            start={ComponentId(2), ComponentId(3)},
            end={ComponentId(5), ComponentId(6), ComponentId(7)},
        ) == {
            Connection(ComponentId(2), ComponentId(5)),
            Connection(ComponentId(2), ComponentId(6)),
        }

    def test_dfs_search_two_grid_meters(self) -> None:
        """Test DFS searching PV components in a graph with two grid meters."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        pv_inverters = {
            Component(ComponentId(4), ComponentCategory.INVERTER, InverterType.SOLAR),
            Component(ComponentId(5), ComponentCategory.INVERTER, InverterType.SOLAR),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(ComponentId(2), ComponentCategory.METER),
                Component(ComponentId(3), ComponentCategory.METER),
            }.union(pv_inverters),
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(1), ComponentId(3)),
                Connection(ComponentId(2), ComponentId(4)),
                Connection(ComponentId(2), ComponentId(5)),
            },
        )

        result = graph.dfs(grid, set(), graph.is_pv_inverter)
        assert result == pv_inverters

    def test_dfs_search_grid_meter(self) -> None:
        """Test DFS searching PV components in a graph with a single grid meter."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        pv_meters = {
            Component(ComponentId(3), ComponentCategory.METER),
            Component(ComponentId(4), ComponentCategory.METER),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(ComponentId(2), ComponentCategory.METER),
                Component(
                    ComponentId(5), ComponentCategory.INVERTER, InverterType.SOLAR
                ),
                Component(
                    ComponentId(6), ComponentCategory.INVERTER, InverterType.SOLAR
                ),
            }.union(pv_meters),
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(3)),
                Connection(ComponentId(2), ComponentId(4)),
                Connection(ComponentId(3), ComponentId(5)),
                Connection(ComponentId(4), ComponentId(6)),
            },
        )

        result = graph.dfs(grid, set(), graph.is_pv_chain)
        assert result == pv_meters

    def test_dfs_search_grid_meter_no_pv_meter(self) -> None:
        """Test DFS searching PV components in a graph with a single grid meter."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        pv_inverters = {
            Component(ComponentId(3), ComponentCategory.INVERTER, InverterType.SOLAR),
            Component(ComponentId(4), ComponentCategory.INVERTER, InverterType.SOLAR),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(ComponentId(2), ComponentCategory.METER),
            }.union(pv_inverters),
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(3)),
                Connection(ComponentId(2), ComponentId(4)),
            },
        )

        result = graph.dfs(grid, set(), graph.is_pv_chain)
        assert result == pv_inverters

    def test_dfs_search_no_grid_meter(self) -> None:
        """Test DFS searching PV components in a graph with no grid meter."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        pv_meters = {
            Component(ComponentId(3), ComponentCategory.METER),
            Component(ComponentId(4), ComponentCategory.METER),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(ComponentId(2), ComponentCategory.METER),
                Component(
                    ComponentId(5), ComponentCategory.INVERTER, InverterType.SOLAR
                ),
                Component(
                    ComponentId(6), ComponentCategory.INVERTER, InverterType.SOLAR
                ),
            }.union(pv_meters),
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(1), ComponentId(3)),
                Connection(ComponentId(1), ComponentId(4)),
                Connection(ComponentId(3), ComponentId(5)),
                Connection(ComponentId(4), ComponentId(6)),
            },
        )

        result = graph.dfs(grid, set(), graph.is_pv_chain)
        assert result == pv_meters

    def test_dfs_search_nested_components(self) -> None:
        """Test DFS searching PV components in a graph with nested components."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        battery_components = {
            Component(ComponentId(4), ComponentCategory.METER),
            Component(ComponentId(5), ComponentCategory.METER),
            Component(ComponentId(6), ComponentCategory.INVERTER, InverterType.BATTERY),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(ComponentId(2), ComponentCategory.METER),
                Component(ComponentId(3), ComponentCategory.METER),
                Component(
                    ComponentId(7), ComponentCategory.INVERTER, InverterType.BATTERY
                ),
                Component(
                    ComponentId(8), ComponentCategory.INVERTER, InverterType.BATTERY
                ),
            }.union(battery_components),
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(3)),
                Connection(ComponentId(2), ComponentId(6)),
                Connection(ComponentId(3), ComponentId(4)),
                Connection(ComponentId(3), ComponentId(5)),
                Connection(ComponentId(4), ComponentId(7)),
                Connection(ComponentId(5), ComponentId(8)),
            },
        )

        assert set() == graph.dfs(grid, set(), graph.is_pv_chain)
        assert battery_components == graph.dfs(grid, set(), graph.is_battery_chain)

    def test_find_first_descendant_component(self) -> None:
        """Test scenarios for finding the first descendant component."""
        graph = gr._MicrogridComponentGraph(
            components={
                Component(ComponentId(1), ComponentCategory.GRID),
                Component(ComponentId(2), ComponentCategory.METER),
                Component(ComponentId(3), ComponentCategory.METER),
                Component(
                    ComponentId(4), ComponentCategory.INVERTER, InverterType.BATTERY
                ),
                Component(
                    ComponentId(5), ComponentCategory.INVERTER, InverterType.SOLAR
                ),
                Component(ComponentId(6), ComponentCategory.EV_CHARGER),
            },
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(3)),
                Connection(ComponentId(2), ComponentId(4)),
                Connection(ComponentId(2), ComponentId(5)),
                Connection(ComponentId(3), ComponentId(6)),
            },
        )

        # Find the first descendant component of the grid endpoint.
        result = graph.find_first_descendant_component(
            descendant_categories=(ComponentCategory.METER,),
        )
        assert result == Component(ComponentId(2), ComponentCategory.METER)

        # Find the first descendant component of the grid,
        # considering meter or inverter categories.
        result = graph.find_first_descendant_component(
            descendant_categories=(ComponentCategory.METER, ComponentCategory.INVERTER),
        )
        assert result == Component(ComponentId(2), ComponentCategory.METER)

        # Verify behavior when component is not found in immediate descendant
        # categories for the first meter.
        with pytest.raises(ValueError):
            graph.find_first_descendant_component(
                descendant_categories=(
                    ComponentCategory.EV_CHARGER,
                    ComponentCategory.BATTERY,
                ),
            )

        # Verify behavior when component is not found in immediate descendant
        # categories from the grid component as root.
        with pytest.raises(ValueError):
            graph.find_first_descendant_component(
                descendant_categories=(ComponentCategory.INVERTER,),
            )


class Test_MicrogridComponentGraph:
    """Test cases for the package-internal implementation of the ComponentGraph.

    The _MicrogridComponentGraph class is internal to the `microgrid` package, and
    defines functionality intended to allow the graph to be (re)populated from the
    microgrid API.  These test cases cover those package internals.
    """

    def test___init__(self) -> None:
        """Test the constructor."""
        # it is possible to instantiate an empty graph, but
        # it will not be considered valid until it has been
        # populated with components and connections
        empty_graph = gr._MicrogridComponentGraph()
        assert set(empty_graph.components()) == set()
        assert list(empty_graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            empty_graph.validate()

        # if components and connections are provided,
        # must provide both non-empty, not one or the
        # other
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={Component(ComponentId(1), ComponentCategory.GRID)}
            )

        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                connections={Connection(ComponentId(1), ComponentId(2))}
            )

        # if both are provided, the graph data must itself
        # be valid (we give just a couple of cases of each
        # here: a comprehensive set of the different kinds
        # of invalid graph data are provided in test cases
        # for the different `_validate*` methods)

        # minimal valid microgrid data: a grid endpoint
        # connected to a meter
        grid_and_meter = gr._MicrogridComponentGraph(
            components={
                Component(ComponentId(1), ComponentCategory.GRID),
                Component(ComponentId(2), ComponentCategory.METER),
            },
            connections={Connection(ComponentId(1), ComponentId(2))},
        )
        expected = {
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
        }
        assert len(grid_and_meter.components()) == len(expected)
        assert set(grid_and_meter.components()) == expected
        assert list(grid_and_meter.connections()) == [
            Connection(ComponentId(1), ComponentId(2))
        ]
        grid_and_meter.validate()

        # invalid graph data: unknown component category
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={
                    Component(ComponentId(1), ComponentCategory.GRID),
                    Component(ComponentId(2), ComponentCategory.METER),
                    Component(ComponentId(3), 666),  # type: ignore
                },
                connections={
                    Connection(ComponentId(1), ComponentId(2)),
                    Connection(ComponentId(1), ComponentId(3)),
                },
            )

        # invalid graph data: a connection between components that do not exist
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={
                    Component(ComponentId(1), ComponentCategory.GRID),
                    Component(ComponentId(2), ComponentCategory.METER),
                },
                connections={
                    Connection(ComponentId(1), ComponentId(2)),
                    Connection(ComponentId(1), ComponentId(3)),
                },
            )

        # invalid graph data: one of the connections is not valid
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={
                    Component(ComponentId(1), ComponentCategory.GRID),
                    Component(ComponentId(2), ComponentCategory.METER),
                },
                connections={
                    Connection(ComponentId(1), ComponentId(2)),
                    Connection(ComponentId(2), ComponentId(2)),
                },
            )

    def test_refresh_from(self) -> None:
        """Test the refresh_from method."""
        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # both connections and components must be non-empty
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(set(), set())
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(set(), {Connection(ComponentId(1), ComponentId(2))})
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                {Component(ComponentId(1), ComponentCategory.GRID)}, set()
            )
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # if both are provided, valid graph data must be present

        # invalid component
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={
                    Component(ComponentId(0), ComponentCategory.GRID),
                    Component(ComponentId(1), ComponentCategory.METER),
                    Component(ComponentId(2), ComponentCategory.METER),
                },
                connections={Connection(ComponentId(1), ComponentId(2))},
            )
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # invalid connection
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={
                    Component(ComponentId(1), ComponentCategory.GRID),
                    Component(ComponentId(2), ComponentCategory.METER),
                    Component(ComponentId(3), ComponentCategory.METER),
                },
                connections={
                    Connection(ComponentId(1), ComponentId(1)),
                    Connection(ComponentId(2), ComponentId(3)),
                },
            )
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # valid graph with both load and battery setup
        graph.refresh_from(
            components={
                Component(ComponentId(1), ComponentCategory.GRID),
                Component(ComponentId(2), ComponentCategory.METER),
                Component(ComponentId(4), ComponentCategory.METER),
                Component(ComponentId(5), ComponentCategory.INVERTER),
                Component(ComponentId(6), ComponentCategory.BATTERY),
            },
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(4)),
                Connection(ComponentId(4), ComponentId(5)),
                Connection(ComponentId(5), ComponentId(6)),
            },
        )
        expected = {
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
            Component(ComponentId(4), ComponentCategory.METER),
            Component(ComponentId(5), ComponentCategory.INVERTER),
            Component(ComponentId(6), ComponentCategory.BATTERY),
        }
        assert len(graph.components()) == len(expected)
        assert set(graph.components()) == expected
        assert graph.connections() == {
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(4)),
            Connection(ComponentId(4), ComponentId(5)),
            Connection(ComponentId(5), ComponentId(6)),
        }
        graph.validate()

        # if invalid graph data is provided (in this case, the graph
        # is not a tree), then the existing contents of the component
        # graph will remain unchanged
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={
                    Component(ComponentId(7), ComponentCategory.GRID),
                    Component(ComponentId(8), ComponentCategory.METER),
                    Component(ComponentId(9), ComponentCategory.INVERTER),
                },
                connections={
                    Connection(ComponentId(7), ComponentId(8)),
                    Connection(ComponentId(8), ComponentId(9)),
                    Connection(ComponentId(9), ComponentId(8)),
                },
            )

        assert len(graph.components()) == len(expected)
        assert graph.components() == expected

        assert graph.connections() == {
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(4)),
            Connection(ComponentId(4), ComponentId(5)),
            Connection(ComponentId(5), ComponentId(6)),
        }
        graph.validate()

        # confirm that if `correct_errors` callback is not `None`,
        # it will be invoked when graph data is invalid
        error_correction = False

        def pretend_to_correct_errors(_g: gr._MicrogridComponentGraph) -> None:
            nonlocal error_correction
            error_correction = True

        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={
                    Component(ComponentId(7), ComponentCategory.GRID),
                    Component(ComponentId(9), ComponentCategory.METER),
                },
                connections={Connection(ComponentId(9), ComponentId(7))},
                correct_errors=pretend_to_correct_errors,
            )

        assert error_correction is True

        # if valid graph data is provided, then the existing graph
        # contents will be overwritten
        graph.refresh_from(
            components={
                Component(ComponentId(10), ComponentCategory.GRID),
                Component(ComponentId(11), ComponentCategory.METER),
            },
            connections={Connection(ComponentId(10), ComponentId(11))},
        )

        expected = {
            Component(ComponentId(10), ComponentCategory.GRID),
            Component(ComponentId(11), ComponentCategory.METER),
        }
        assert len(graph.components()) == len(expected)
        assert set(graph.components()) == expected
        assert graph.connections() == {Connection(ComponentId(10), ComponentId(11))}
        graph.validate()

    async def test_refresh_from_api(self) -> None:
        """Test the refresh_from_api method."""
        graph = gr._MicrogridComponentGraph()
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        client = mock.MagicMock(name="client", spec=MicrogridApiClient)
        client.components = mock.AsyncMock(name="client.components()", return_value=[])
        client.connections = mock.AsyncMock(
            name="client.connections()", return_value=[]
        )

        # both components and connections must be non-empty
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_api(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        client.components.return_value = [
            Component(ComponentId(1), ComponentCategory.GRID)
        ]
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_api(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        client.components.return_value = []
        client.connections.return_value = [Connection(ComponentId(1), ComponentId(2))]
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_api(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # if both are provided, valid graph data must be present

        # valid graph with meter, and EV charger
        client.components.return_value = [
            Component(
                ComponentId(101),
                ComponentCategory.GRID,
                metadata=ComponentMetadata(fuse=Fuse(max_current=0.0)),
            ),
            Component(ComponentId(111), ComponentCategory.METER),
            Component(ComponentId(131), ComponentCategory.EV_CHARGER),
        ]
        client.connections.return_value = [
            Connection(ComponentId(101), ComponentId(111)),
            Connection(ComponentId(111), ComponentId(131)),
        ]
        await graph.refresh_from_api(client)

        # Note: we need to add GriMetadata as a dict here, because that's what
        # the ComponentGraph does too, and we need to be able to compare the
        # two graphs.
        expected = {
            Component(
                ComponentId(101),
                ComponentCategory.GRID,
                None,
                ComponentMetadata(fuse=Fuse(max_current=0.0)),
            ),
            Component(ComponentId(111), ComponentCategory.METER),
            Component(ComponentId(131), ComponentCategory.EV_CHARGER),
        }
        assert len(graph.components()) == len(expected)
        assert graph.components() == expected
        assert graph.connections() == {
            Connection(ComponentId(101), ComponentId(111)),
            Connection(ComponentId(111), ComponentId(131)),
        }
        graph.validate()

        # if valid graph data is provided, then the existing graph
        # contents will be overwritten
        client.components.return_value = [
            Component(
                ComponentId(707),
                ComponentCategory.GRID,
                metadata=ComponentMetadata(fuse=Fuse(max_current=0.0)),
            ),
            Component(ComponentId(717), ComponentCategory.METER),
            Component(
                ComponentId(727), ComponentCategory.INVERTER, type=InverterType.NONE
            ),
            Component(ComponentId(737), ComponentCategory.BATTERY),
            Component(ComponentId(747), ComponentCategory.METER),
        ]
        client.connections.return_value = [
            Connection(ComponentId(707), ComponentId(717)),
            Connection(ComponentId(717), ComponentId(727)),
            Connection(ComponentId(727), ComponentId(737)),
            Connection(ComponentId(717), ComponentId(747)),
        ]
        await graph.refresh_from_api(client)

        expected = {
            Component(
                ComponentId(707),
                ComponentCategory.GRID,
                None,
                ComponentMetadata(fuse=Fuse(max_current=0.0)),
            ),
            Component(ComponentId(717), ComponentCategory.METER),
            Component(ComponentId(727), ComponentCategory.INVERTER, InverterType.NONE),
            Component(ComponentId(737), ComponentCategory.BATTERY),
            Component(ComponentId(747), ComponentCategory.METER),
        }
        assert len(graph.components()) == len(expected)
        assert graph.components() == expected

        assert graph.connections() == {
            Connection(ComponentId(707), ComponentId(717)),
            Connection(ComponentId(717), ComponentId(727)),
            Connection(ComponentId(717), ComponentId(747)),
            Connection(ComponentId(727), ComponentId(737)),
        }
        graph.validate()

    def test_validate(self) -> None:
        """Test the validate method."""
        # `validate` will fail if any of the following are the case:
        #
        #   * the graph data is not valid
        #   * there is not a valid graph root
        #   * a grid endpoint is present but not set up correctly
        #   * intermediary components are not set up correctly
        #   * junctions are not set up correctly
        #   * leaf components are not set up correctly
        #
        # Full coverage of the details of how that can happen is left
        # to the individual `test__validate_*` cases below: for this
        # level, we just check one case of each.
        #
        # To ensure clean testing of the method, we cheat by setting
        # underlying graph data directly.

        graph = gr._MicrogridComponentGraph()

        # graph data is not valid: no components or connections
        graph._graph.clear()
        with pytest.raises(gr.InvalidGraphError, match="No components in graph!"):
            graph.validate()

        # graph root is not valid: multiple potential root nodes
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.NONE),
            Component(ComponentId(3), ComponentCategory.METER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(3)),
            Connection(ComponentId(2), ComponentId(3)),
        )
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph.validate()

        # grid endpoint is not set up correctly: multiple grid endpoints
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.GRID),
            Component(ComponentId(3), ComponentCategory.METER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(3)),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="Multiple grid endpoints in component graph"
        ):
            graph.validate()

        # leaf components are not set up correctly: a battery has
        # a successor in the graph
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.BATTERY),
            Component(ComponentId(3), ComponentCategory.METER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(3)),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph.validate()

    def test__validate_graph(self) -> None:
        """Test the _validate_graph method."""
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # graph has no nodes (i.e. no components)
        with pytest.raises(gr.InvalidGraphError, match="No components in graph!"):
            graph._validate_graph()

        # graph has no connections
        graph._graph.clear()
        _add_components(graph, Component(ComponentId(1), ComponentCategory.GRID))
        with pytest.raises(
            gr.InvalidGraphError, match="No connections in component graph!"
        ):
            graph._validate_graph()

        # graph is not a tree
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.INVERTER),
            Component(ComponentId(3), ComponentCategory.METER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(3)),
            Connection(ComponentId(3), ComponentId(2)),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="Component graph is not a tree!"
        ):
            graph._validate_graph()

        # at least one node is completely unconnected
        # (this violates the tree property):
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
            Component(ComponentId(3), ComponentCategory.NONE),
        )
        _add_connections(graph, Connection(ComponentId(1), ComponentId(2)))
        with pytest.raises(
            gr.InvalidGraphError, match="Component graph is not a tree!"
        ):
            graph._validate_graph()

    def test__validate_graph_root(self) -> None:
        """Test the _validate_graph_root method."""
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # no node without predecessors (this should already
        # get caught by `_validate_graph` but let's confirm
        # that `_validate_graph_root` also catches it)
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.METER),
            Component(ComponentId(2), ComponentCategory.METER),
            Component(ComponentId(3), ComponentCategory.METER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(3)),
            Connection(ComponentId(3), ComponentId(1)),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="No valid root nodes of component graph!"
        ):
            graph._validate_graph_root()

        # there are nodes without predecessors, but not of
        # the valid type(s) NONE, GRID, or JUNCTION
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.METER),
            Component(ComponentId(2), ComponentCategory.INVERTER),
            Component(ComponentId(3), ComponentCategory.BATTERY),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(3)),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="No valid root nodes of component graph!"
        ):
            graph._validate_graph_root()

        # there are multiple different potentially valid
        # root notes
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.NONE),
            Component(ComponentId(2), ComponentCategory.GRID),
            Component(ComponentId(3), ComponentCategory.METER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(3)),
            Connection(ComponentId(2), ComponentId(3)),
        )
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph._validate_graph_root()

        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.GRID),
            Component(ComponentId(3), ComponentCategory.METER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(3)),
            Connection(ComponentId(2), ComponentId(3)),
        )
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph._validate_graph_root()

        # there is just one potential root node but it has no successors
        graph._graph.clear()
        _add_components(graph, Component(ComponentId(1), ComponentCategory.NONE))
        with pytest.raises(
            gr.InvalidGraphError,
            match=r"Graph root .*component_id=ComponentId\(1\).* has no successors!",
        ):
            graph._validate_graph_root()

        graph._graph.clear()
        _add_components(graph, Component(ComponentId(2), ComponentCategory.GRID))
        with pytest.raises(
            gr.InvalidGraphError,
            match=r"Graph root .*component_id=ComponentId\(2\).* has no successors!",
        ):
            graph._validate_graph_root()

        graph._graph.clear()
        _add_components(graph, Component(ComponentId(3), ComponentCategory.GRID))
        with pytest.raises(
            gr.InvalidGraphError,
            match=r"Graph root .*component_id=ComponentId\(3\).* has no successors!",
        ):
            graph._validate_graph_root()

        # there is exactly one potential root node and it has successors
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.NONE),
            Component(ComponentId(2), ComponentCategory.METER),
        )
        _add_connections(graph, Connection(ComponentId(1), ComponentId(2)))
        graph._validate_graph_root()

        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
        )
        _add_connections(graph, Connection(ComponentId(1), ComponentId(2)))
        graph._validate_graph_root()

        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
        )
        _add_connections(graph, Connection(ComponentId(1), ComponentId(2)))
        graph._validate_graph_root()

    def test__validate_grid_endpoint(self) -> None:
        """Test the _validate_grid_endpoint method."""
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # missing grid endpoint is OK as the graph might have
        # another kind of root
        graph._graph.clear()
        _add_components(graph, Component(ComponentId(2), ComponentCategory.METER))
        graph._validate_grid_endpoint()

        # multiple grid endpoints
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
            Component(ComponentId(3), ComponentCategory.GRID),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(3), ComponentId(2)),
        )
        with pytest.raises(
            gr.InvalidGraphError,
            match="Multiple grid endpoints in component graph",
        ):
            graph._validate_grid_endpoint()

        # grid endpoint has predecessors
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(99), ComponentCategory.METER),
        )
        _add_connections(graph, Connection(ComponentId(99), ComponentId(1)))
        with pytest.raises(
            gr.InvalidGraphError,
            match=r"Grid endpoint with CID1 has graph predecessors: \[Component"
            r"\(component_id=ComponentId\(99\), category=<ComponentCategory.METER.*>, "
            r"type=None, metadata=None\)\]",
        ):
            graph._validate_grid_endpoint()

        # grid endpoint has no successors
        graph._graph.clear()
        _add_components(graph, Component(ComponentId(101), ComponentCategory.GRID))
        with pytest.raises(
            gr.InvalidGraphError,
            match="Grid endpoint with CID101 has no graph successors!",
        ):
            graph._validate_grid_endpoint()

        # valid grid endpoint with at least one successor
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
        )
        _add_connections(graph, Connection(ComponentId(1), ComponentId(2)))
        graph._validate_grid_endpoint()

    def test__validate_intermediary_components(self) -> None:
        """Test the _validate_intermediary_components method."""
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # missing predecessor for at least one intermediary node
        graph._graph.clear()
        _add_components(graph, Component(ComponentId(3), ComponentCategory.INVERTER))
        with pytest.raises(
            gr.InvalidGraphError,
            match="Intermediary components without graph predecessors",
        ):
            graph._validate_intermediary_components()

        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(3), ComponentCategory.INVERTER),
        )
        _add_connections(graph, Connection(ComponentId(1), ComponentId(3)))
        graph._validate_intermediary_components()

        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
            Component(ComponentId(3), ComponentCategory.INVERTER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(3)),
        )
        graph._validate_intermediary_components()

        # all intermediary nodes have at least one predecessor
        # and at least one successor
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
            Component(ComponentId(3), ComponentCategory.INVERTER),
            Component(ComponentId(4), ComponentCategory.BATTERY),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(3)),
            Connection(ComponentId(3), ComponentId(4)),
        )
        graph._validate_intermediary_components()

    def test__validate_leaf_components(self) -> None:
        """Test the _validate_leaf_components method."""
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # missing predecessor for at least one leaf node
        graph._graph.clear()
        _add_components(graph, Component(ComponentId(3), ComponentCategory.BATTERY))
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components without graph predecessors"
        ):
            graph._validate_leaf_components()

        graph._graph.clear()
        _add_components(graph, Component(ComponentId(4), ComponentCategory.EV_CHARGER))
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components without graph predecessors"
        ):
            graph._validate_leaf_components()

        # successors present for at least one leaf node
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.EV_CHARGER),
            Component(ComponentId(3), ComponentCategory.BATTERY),
        )

        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(2), ComponentId(3)),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph._validate_leaf_components()

        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(3), ComponentCategory.BATTERY),
            Component(ComponentId(4), ComponentCategory.EV_CHARGER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(3)),
            Connection(ComponentId(3), ComponentId(4)),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph._validate_leaf_components()

        # all leaf nodes have at least one predecessor
        # and no successors
        graph._graph.clear()
        _add_components(
            graph,
            Component(ComponentId(1), ComponentCategory.GRID),
            Component(ComponentId(2), ComponentCategory.METER),
            Component(ComponentId(3), ComponentCategory.BATTERY),
            Component(ComponentId(4), ComponentCategory.EV_CHARGER),
        )
        _add_connections(
            graph,
            Connection(ComponentId(1), ComponentId(2)),
            Connection(ComponentId(1), ComponentId(3)),
            Connection(ComponentId(1), ComponentId(4)),
        )
        graph._validate_leaf_components()


class TestComponentTypeIdentification:
    """Test the component type identification methods in the component graph."""

    def test_no_comp_meters_pv(self) -> None:
        """Test the case where there are no meters in the graph."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        grid_meter = Component(ComponentId(2), ComponentCategory.METER)
        pv_inv_1 = Component(
            ComponentId(3), ComponentCategory.INVERTER, InverterType.SOLAR
        )
        pv_inv_2 = Component(
            ComponentId(4), ComponentCategory.INVERTER, InverterType.SOLAR
        )

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                grid_meter,
                pv_inv_1,
                pv_inv_2,
            },
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(3)),
                Connection(ComponentId(2), ComponentId(4)),
            },
        )

        assert graph.is_grid_meter(grid_meter)
        assert not graph.is_pv_meter(grid_meter)
        assert not graph.is_pv_chain(grid_meter)

        assert graph.is_pv_inverter(pv_inv_1) and graph.is_pv_chain(pv_inv_1)
        assert graph.is_pv_inverter(pv_inv_2) and graph.is_pv_chain(pv_inv_2)

    def test_no_comp_meters_mixed(self) -> None:
        """Test the case where there are no meters in the graph."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        grid_meter = Component(ComponentId(2), ComponentCategory.METER)
        pv_inv = Component(
            ComponentId(3), ComponentCategory.INVERTER, InverterType.SOLAR
        )
        battery_inv = Component(
            ComponentId(4), ComponentCategory.INVERTER, InverterType.BATTERY
        )
        battery = Component(ComponentId(5), ComponentCategory.BATTERY)

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                grid_meter,
                pv_inv,
                battery_inv,
                battery,
            },
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(3)),
                Connection(ComponentId(2), ComponentId(4)),
                Connection(ComponentId(4), ComponentId(5)),
            },
        )

        assert graph.is_grid_meter(grid_meter)
        assert not graph.is_pv_meter(grid_meter)
        assert not graph.is_pv_chain(grid_meter)

        assert graph.is_pv_inverter(pv_inv) and graph.is_pv_chain(pv_inv)
        assert not graph.is_battery_inverter(pv_inv) and not graph.is_battery_chain(
            pv_inv
        )

        assert graph.is_battery_inverter(battery_inv) and graph.is_battery_chain(
            battery_inv
        )
        assert not graph.is_pv_inverter(battery_inv) and not graph.is_pv_chain(
            battery_inv
        )

    def test_with_meters(self) -> None:
        """Test the case where there are meters in the graph."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        grid_meter = Component(ComponentId(2), ComponentCategory.METER)
        pv_meter = Component(ComponentId(3), ComponentCategory.METER)
        pv_inv = Component(
            ComponentId(4), ComponentCategory.INVERTER, InverterType.SOLAR
        )
        battery_meter = Component(ComponentId(5), ComponentCategory.METER)
        battery_inv = Component(
            ComponentId(6), ComponentCategory.INVERTER, InverterType.BATTERY
        )
        battery = Component(ComponentId(7), ComponentCategory.BATTERY)

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                grid_meter,
                pv_meter,
                pv_inv,
                battery_meter,
                battery_inv,
                battery,
            },
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(3)),
                Connection(ComponentId(3), ComponentId(4)),
                Connection(ComponentId(2), ComponentId(5)),
                Connection(ComponentId(5), ComponentId(6)),
                Connection(ComponentId(6), ComponentId(7)),
            },
        )

        assert graph.is_grid_meter(grid_meter)
        assert not graph.is_pv_meter(grid_meter)
        assert not graph.is_pv_chain(grid_meter)

        assert graph.is_pv_meter(pv_meter)
        assert graph.is_pv_chain(pv_meter)
        assert graph.is_pv_chain(pv_inv)
        assert graph.is_pv_inverter(pv_inv)

        assert graph.is_battery_meter(battery_meter)
        assert graph.is_battery_chain(battery_meter)
        assert graph.is_battery_chain(battery_inv)
        assert graph.is_battery_inverter(battery_inv)

    def test_without_grid_meters(self) -> None:
        """Test the case where there are no grid meters in the graph."""
        grid = Component(ComponentId(1), ComponentCategory.GRID)
        ev_meter = Component(ComponentId(2), ComponentCategory.METER)
        ev_charger = Component(ComponentId(3), ComponentCategory.EV_CHARGER)
        chp_meter = Component(ComponentId(4), ComponentCategory.METER)
        chp = Component(ComponentId(5), ComponentCategory.CHP)

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                ev_meter,
                ev_charger,
                chp_meter,
                chp,
            },
            connections={
                Connection(ComponentId(1), ComponentId(2)),
                Connection(ComponentId(2), ComponentId(3)),
                Connection(ComponentId(1), ComponentId(4)),
                Connection(ComponentId(4), ComponentId(5)),
            },
        )

        assert not graph.is_grid_meter(ev_meter)
        assert not graph.is_grid_meter(chp_meter)

        assert graph.is_ev_charger_meter(ev_meter)
        assert graph.is_ev_charger(ev_charger)
        assert graph.is_ev_charger_chain(ev_meter)
        assert graph.is_ev_charger_chain(ev_charger)

        assert graph.is_chp_meter(chp_meter)
        assert graph.is_chp(chp)
        assert graph.is_chp_chain(chp_meter)
        assert graph.is_chp_chain(chp)
