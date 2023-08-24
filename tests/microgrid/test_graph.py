# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Tests for the microgrid component graph.
"""

# pylint: disable=too-many-lines,use-implicit-booleaness-not-comparison
# pylint: disable=invalid-name,missing-function-docstring,too-many-statements
# pylint: disable=too-many-lines,protected-access,no-member

from dataclasses import asdict
from typing import Dict, Set

import frequenz.api.common.components_pb2 as components_pb
import grpc
import pytest

import frequenz.sdk.microgrid._graph as gr
from frequenz.sdk.microgrid.client import Connection, MicrogridGrpcClient
from frequenz.sdk.microgrid.component import (
    Component,
    ComponentCategory,
    GridMetadata,
    InverterType,
)

from .mock_api import MockGrpcServer, MockMicrogridServicer


def _check_predecessors_and_successors(graph: gr.ComponentGraph) -> None:
    expected_predecessors: Dict[int, Set[Component]] = {}
    expected_successors: Dict[int, Set[Component]] = {}

    components: Dict[int, Component] = {
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
    def sample_input_components(self) -> Set[Component]:
        return {
            Component(11, ComponentCategory.GRID),
            Component(21, ComponentCategory.METER),
            Component(41, ComponentCategory.METER),
            Component(51, ComponentCategory.INVERTER),
            Component(61, ComponentCategory.BATTERY),
        }

    @pytest.fixture()
    def sample_input_connections(self) -> Set[Connection]:
        return {
            Connection(11, 21),
            Connection(21, 41),
            Connection(41, 51),
            Connection(51, 61),
        }

    @pytest.fixture()
    def sample_graph(
        self,
        sample_input_components: Set[Component],
        sample_input_connections: Set[Connection],
    ) -> gr.ComponentGraph:
        """Create a sample graph for testing purposes."""
        _graph_implementation = gr._MicrogridComponentGraph(
            components=sample_input_components,
            connections=sample_input_connections,
        )
        return _graph_implementation

    def test_without_filters(self) -> None:
        _graph_implementation = gr._MicrogridComponentGraph()
        graph: gr.ComponentGraph = _graph_implementation

        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(
            KeyError,
            match="Component 1 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(1)
        with pytest.raises(
            KeyError,
            match="Component 1 not in graph, cannot get successors!",
        ):
            graph.successors(1)

        # simplest valid microgrid: a grid endpoint and a meter
        _graph_implementation.refresh_from(
            components={
                Component(1, ComponentCategory.GRID),
                Component(3, ComponentCategory.METER),
            },
            connections={Connection(1, 3)},
        )
        expected_components = {
            Component(1, ComponentCategory.GRID),
            Component(3, ComponentCategory.METER),
        }
        assert len(graph.components()) == len(expected_components)
        assert graph.components() == expected_components
        assert graph.connections() == {Connection(1, 3)}

        assert graph.predecessors(1) == set()
        assert graph.successors(1) == {Component(3, ComponentCategory.METER)}
        assert graph.predecessors(3) == {Component(1, ComponentCategory.GRID)}
        assert graph.successors(3) == set()
        with pytest.raises(
            KeyError,
            match="Component 2 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(2)
        with pytest.raises(
            KeyError,
            match="Component 2 not in graph, cannot get successors!",
        ):
            graph.successors(2)

        input_components = {
            101: Component(101, ComponentCategory.GRID),
            102: Component(102, ComponentCategory.METER),
            104: Component(104, ComponentCategory.METER),
            105: Component(105, ComponentCategory.INVERTER),
            106: Component(106, ComponentCategory.BATTERY),
        }
        input_connections = {
            Connection(101, 102),
            Connection(102, 104),
            Connection(104, 105),
            Connection(105, 106),
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
            match="Component 9 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(9)
        with pytest.raises(
            KeyError,
            match="Component 99 not in graph, cannot get successors!",
        ):
            graph.successors(99)

    @pytest.mark.parametrize(
        "ids, expected",
        [
            ({1}, set()),
            ({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, set()),
            ({11}, {Component(11, ComponentCategory.GRID)}),
            ({21}, {Component(21, ComponentCategory.METER)}),
            ({41}, {Component(41, ComponentCategory.METER)}),
            ({51}, {Component(51, ComponentCategory.INVERTER)}),
            ({61}, {Component(61, ComponentCategory.BATTERY)}),
            (
                {11, 61},
                {
                    Component(11, ComponentCategory.GRID),
                    Component(61, ComponentCategory.BATTERY),
                },
            ),
            (
                {9, 51, 41, 21, 101},
                {
                    Component(41, ComponentCategory.METER),
                    Component(51, ComponentCategory.INVERTER),
                    Component(21, ComponentCategory.METER),
                },
            ),
        ],
    )
    def test_filter_graph_components_by_id(
        self, sample_graph: gr.ComponentGraph, ids: Set[int], expected: Set[Component]
    ) -> None:
        # with component_id filter specified, we get back only components whose ID
        # matches one of the specified values
        assert len(sample_graph.components(component_id=ids)) == len(expected)
        assert sample_graph.components(component_id=ids) == expected

    @pytest.mark.parametrize(
        "types, expected",
        [
            ({ComponentCategory.EV_CHARGER}, set()),
            (
                {ComponentCategory.BATTERY, ComponentCategory.EV_CHARGER},
                {Component(61, ComponentCategory.BATTERY)},
            ),
            ({ComponentCategory.GRID}, {Component(11, ComponentCategory.GRID)}),
            (
                {ComponentCategory.METER},
                {
                    Component(21, ComponentCategory.METER),
                    Component(41, ComponentCategory.METER),
                },
            ),
            ({ComponentCategory.INVERTER}, {Component(51, ComponentCategory.INVERTER)}),
            ({ComponentCategory.BATTERY}, {Component(61, ComponentCategory.BATTERY)}),
            (
                {ComponentCategory.GRID, ComponentCategory.BATTERY},
                {
                    Component(11, ComponentCategory.GRID),
                    Component(61, ComponentCategory.BATTERY),
                },
            ),
            (
                {
                    ComponentCategory.METER,
                    ComponentCategory.BATTERY,
                    ComponentCategory.EV_CHARGER,
                },
                {
                    Component(21, ComponentCategory.METER),
                    Component(61, ComponentCategory.BATTERY),
                    Component(41, ComponentCategory.METER),
                },
            ),
        ],
    )
    def test_filter_graph_components_by_type(
        self,
        sample_graph: gr.ComponentGraph,
        types: Set[ComponentCategory],
        expected: Set[Component],
    ) -> None:
        # with component_id filter specified, we get back only components whose ID
        # matches one of the specified values
        assert len(sample_graph.components(component_category=types)) == len(expected)
        assert sample_graph.components(component_category=types) == expected

    @pytest.mark.parametrize(
        "ids, types, expected",
        [
            ({11}, {ComponentCategory.GRID}, {Component(11, ComponentCategory.GRID)}),
            ({31}, {ComponentCategory.GRID}, set()),
            (
                {61},
                {ComponentCategory.BATTERY},
                {Component(61, ComponentCategory.BATTERY)},
            ),
            (
                {11, 21, 31, 61},
                {ComponentCategory.METER, ComponentCategory.BATTERY},
                {
                    Component(61, ComponentCategory.BATTERY),
                    Component(21, ComponentCategory.METER),
                },
            ),
        ],
    )
    def test_filter_graph_components_with_composite_filter(
        self,
        sample_graph: gr.ComponentGraph,
        ids: Set[int],
        types: Set[ComponentCategory],
        expected: Set[Component],
    ) -> None:
        # when both filters are applied, they are combined via AND logic, i.e.
        # the component must have one of the specified IDs and be of one of
        # the specified types
        assert len(
            sample_graph.components(component_id=ids, component_category=types)
        ) == len(expected)
        assert (
            set(sample_graph.components(component_id=ids, component_category=types))
            == expected
        )

    def test_components_without_filters(
        self, sample_input_components: Set[Component], sample_graph: gr.ComponentGraph
    ) -> None:
        # without any filter applied, we get back all the components in the graph
        assert len(sample_graph.components()) == len(sample_input_components)
        assert sample_graph.components() == sample_input_components

    def test_connection_filters(self) -> None:
        _graph_implementation = gr._MicrogridComponentGraph(
            components={
                Component(1, ComponentCategory.GRID),
                Component(2, ComponentCategory.METER),
                Component(3, ComponentCategory.METER),
                Component(4, ComponentCategory.EV_CHARGER),
                Component(5, ComponentCategory.EV_CHARGER),
                Component(6, ComponentCategory.EV_CHARGER),
            },
            connections={
                Connection(1, 2),
                Connection(1, 3),
                Connection(2, 4),
                Connection(2, 5),
                Connection(2, 6),
            },
        )
        graph: gr.ComponentGraph = _graph_implementation

        # without any filter applied, we get back all the connections in the graph
        assert graph.connections() == {
            Connection(1, 2),
            Connection(1, 3),
            Connection(2, 4),
            Connection(2, 5),
            Connection(2, 6),
        }

        # with start filter applied, we get back only connections whose `start`
        # component matches one of the provided IDs
        assert graph.connections(start={8}) == set()
        assert graph.connections(start={7}) == set()
        assert graph.connections(start={6}) == set()
        assert graph.connections(start={5}) == set()
        assert graph.connections(start={4}) == set()
        assert graph.connections(start={3}) == set()
        assert graph.connections(start={2}) == {
            Connection(2, 4),
            Connection(2, 5),
            Connection(2, 6),
        }
        assert graph.connections(start={1}) == {
            Connection(1, 2),
            Connection(1, 3),
        }
        assert graph.connections(start={1, 3, 5}) == {
            Connection(1, 2),
            Connection(1, 3),
        }
        assert graph.connections(start={1, 2, 5, 6}) == {
            Connection(1, 2),
            Connection(1, 3),
            Connection(2, 4),
            Connection(2, 5),
            Connection(2, 6),
        }

        # with end filter applied, we get back only connections whose `end`
        # component matches one of the provided IDs
        assert graph.connections(end={8}) == set()
        assert graph.connections(end={6}) == {Connection(2, 6)}
        assert graph.connections(end={5}) == {Connection(2, 5)}
        assert graph.connections(end={4}) == {Connection(2, 4)}
        assert graph.connections(end={3}) == {Connection(1, 3)}
        assert graph.connections(end={2}) == {Connection(1, 2)}
        assert graph.connections(end={1}) == set()
        assert graph.connections(end={1, 2, 3}) == {
            Connection(1, 2),
            Connection(1, 3),
        }
        assert graph.connections(end={4, 5, 6}) == {
            Connection(2, 4),
            Connection(2, 5),
            Connection(2, 6),
        }

        assert graph.connections(end={2, 4, 6, 8}) == {
            Connection(1, 2),
            Connection(2, 4),
            Connection(2, 6),
        }
        assert graph.connections(end={1}) == set()

        # when both filters are applied, they are combined via AND logic, i.e.
        # a connection must have its `start` matching one of the provided start
        # values, and its `end` matching one of the provided end values
        assert graph.connections(start={1}, end={2}) == {Connection(1, 2)}
        assert graph.connections(start={2}, end={3}) == set()
        assert graph.connections(start={1, 2}, end={3, 4}) == {
            Connection(1, 3),
            Connection(2, 4),
        }
        assert graph.connections(start={2, 3}, end={5, 6, 7}) == {
            Connection(2, 5),
            Connection(2, 6),
        }

    def test_dfs_search_two_grid_meters(self) -> None:
        """Test DFS searching PV components in a graph with two grid meters."""
        grid = Component(1, ComponentCategory.GRID)
        pv_inverters = {
            Component(4, ComponentCategory.INVERTER, InverterType.SOLAR),
            Component(5, ComponentCategory.INVERTER, InverterType.SOLAR),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(2, ComponentCategory.METER),
                Component(3, ComponentCategory.METER),
            }.union(pv_inverters),
            connections={
                Connection(1, 2),
                Connection(1, 3),
                Connection(2, 4),
                Connection(2, 5),
            },
        )

        result = graph.dfs(grid, set(), graph.is_pv_inverter)
        assert result == pv_inverters

    def test_dfs_search_grid_meter(self) -> None:
        """Test DFS searching PV components in a graph with a single grid meter."""
        grid = Component(1, ComponentCategory.GRID)
        pv_meters = {
            Component(3, ComponentCategory.METER),
            Component(4, ComponentCategory.METER),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(2, ComponentCategory.METER),
                Component(5, ComponentCategory.INVERTER, InverterType.SOLAR),
                Component(6, ComponentCategory.INVERTER, InverterType.SOLAR),
            }.union(pv_meters),
            connections={
                Connection(1, 2),
                Connection(2, 3),
                Connection(2, 4),
                Connection(3, 5),
                Connection(4, 6),
            },
        )

        result = graph.dfs(grid, set(), graph.is_pv_chain)
        assert result == pv_meters

    def test_dfs_search_no_grid_meter(self) -> None:
        """Test DFS searching PV components in a graph with no grid meter."""
        grid = Component(1, ComponentCategory.GRID)
        pv_meters = {
            Component(3, ComponentCategory.METER),
            Component(4, ComponentCategory.METER),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(2, ComponentCategory.METER),
                Component(5, ComponentCategory.INVERTER, InverterType.SOLAR),
                Component(6, ComponentCategory.INVERTER, InverterType.SOLAR),
            }.union(pv_meters),
            connections={
                Connection(1, 2),
                Connection(1, 3),
                Connection(1, 4),
                Connection(3, 5),
                Connection(4, 6),
            },
        )

        result = graph.dfs(grid, set(), graph.is_pv_chain)
        assert result == pv_meters

    def test_dfs_search_nested_components(self) -> None:
        """Test DFS searching PV components in a graph with nested components."""
        grid = Component(1, ComponentCategory.GRID)
        battery_components = {
            Component(4, ComponentCategory.METER),
            Component(5, ComponentCategory.METER),
            Component(6, ComponentCategory.INVERTER, InverterType.BATTERY),
        }

        graph = gr._MicrogridComponentGraph(
            components={
                grid,
                Component(2, ComponentCategory.METER),
                Component(3, ComponentCategory.METER),
                Component(7, ComponentCategory.INVERTER, InverterType.BATTERY),
                Component(8, ComponentCategory.INVERTER, InverterType.BATTERY),
            }.union(battery_components),
            connections={
                Connection(1, 2),
                Connection(2, 3),
                Connection(2, 6),
                Connection(3, 4),
                Connection(3, 5),
                Connection(4, 7),
                Connection(5, 8),
            },
        )

        assert set() == graph.dfs(grid, set(), graph.is_pv_chain)
        assert battery_components == graph.dfs(grid, set(), graph.is_battery_chain)


class Test_MicrogridComponentGraph:
    """Test cases for the package-internal implementation of the ComponentGraph.

    The _MicrogridComponentGraph class is internal to the `microgrid` package, and
    defines functionality intended to allow the graph to be (re)populated from the
    microgrid API.  These test cases cover those package internals.
    """

    def test___init__(self) -> None:
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
                components={Component(1, ComponentCategory.GRID)}
            )

        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(connections={Connection(1, 2)})

        # if both are provided, the graph data must itself
        # be valid (we give just a couple of cases of each
        # here: a comprehensive set of the different kinds
        # of invalid graph data are provided in test cases
        # for the different `_validate*` methods)

        # minimal valid microgrid data: a grid endpoint
        # connected to a meter
        grid_and_meter = gr._MicrogridComponentGraph(
            components={
                Component(1, ComponentCategory.GRID),
                Component(2, ComponentCategory.METER),
            },
            connections={Connection(1, 2)},
        )
        expected = {
            Component(1, ComponentCategory.GRID),
            Component(2, ComponentCategory.METER),
        }
        assert len(grid_and_meter.components()) == len(expected)
        assert set(grid_and_meter.components()) == expected
        assert list(grid_and_meter.connections()) == [Connection(1, 2)]
        grid_and_meter.validate()

        # invalid graph data: unknown component category
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={
                    Component(1, ComponentCategory.GRID),
                    Component(2, ComponentCategory.METER),
                    Component(3, 666),  # type: ignore
                },
                connections={Connection(1, 2), Connection(1, 3)},
            )

        # invalid graph data: a connection between components that do not exist
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={
                    Component(1, ComponentCategory.GRID),
                    Component(2, ComponentCategory.METER),
                },
                connections={Connection(1, 2), Connection(1, 3)},
            )

        # invalid graph data: one of the connections is not valid
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={
                    Component(1, ComponentCategory.GRID),
                    Component(2, ComponentCategory.METER),
                },
                connections={Connection(1, 2), Connection(2, 2)},
            )

    def test_refresh_from(self) -> None:
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
            graph.refresh_from(set(), {Connection(1, 2)})
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from({Component(1, ComponentCategory.GRID)}, set())
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # if both are provided, valid graph data must be present

        # invalid component
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={
                    Component(0, ComponentCategory.GRID),
                    Component(1, ComponentCategory.METER),
                    Component(2, ComponentCategory.METER),
                },
                connections={Connection(1, 2)},
            )
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # invalid connection
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={
                    Component(1, ComponentCategory.GRID),
                    Component(2, ComponentCategory.METER),
                    Component(3, ComponentCategory.METER),
                },
                connections={Connection(1, 1), Connection(2, 3)},
            )
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # valid graph with both load and battery setup
        graph.refresh_from(
            components={
                Component(1, ComponentCategory.GRID),
                Component(2, ComponentCategory.METER),
                Component(4, ComponentCategory.METER),
                Component(5, ComponentCategory.INVERTER),
                Component(6, ComponentCategory.BATTERY),
            },
            connections={
                Connection(1, 2),
                Connection(2, 4),
                Connection(4, 5),
                Connection(5, 6),
            },
        )
        expected = {
            Component(1, ComponentCategory.GRID),
            Component(2, ComponentCategory.METER),
            Component(4, ComponentCategory.METER),
            Component(5, ComponentCategory.INVERTER),
            Component(6, ComponentCategory.BATTERY),
        }
        assert len(graph.components()) == len(expected)
        assert set(graph.components()) == expected
        assert graph.connections() == {
            Connection(1, 2),
            Connection(2, 4),
            Connection(4, 5),
            Connection(5, 6),
        }
        graph.validate()

        # if invalid graph data is provided (in this case, the graph
        # is not a tree), then the existing contents of the component
        # graph will remain unchanged
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={
                    Component(7, ComponentCategory.GRID),
                    Component(8, ComponentCategory.METER),
                    Component(9, ComponentCategory.INVERTER),
                },
                connections={
                    Connection(7, 8),
                    Connection(8, 9),
                    Connection(9, 8),
                },
            )

        assert len(graph.components()) == len(expected)
        assert graph.components() == expected

        assert graph.connections() == {
            Connection(1, 2),
            Connection(2, 4),
            Connection(4, 5),
            Connection(5, 6),
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
                    Component(7, ComponentCategory.GRID),
                    Component(9, ComponentCategory.METER),
                },
                connections={Connection(9, 7)},
                correct_errors=pretend_to_correct_errors,
            )

        assert error_correction is True

        # if valid graph data is provided, then the existing graph
        # contents will be overwritten
        graph.refresh_from(
            components={
                Component(10, ComponentCategory.GRID),
                Component(11, ComponentCategory.METER),
            },
            connections={Connection(10, 11)},
        )

        expected = {
            Component(10, ComponentCategory.GRID),
            Component(11, ComponentCategory.METER),
        }
        assert len(graph.components()) == len(expected)
        assert set(graph.components()) == expected
        assert graph.connections() == {Connection(10, 11)}
        graph.validate()

    async def test_refresh_from_api(self) -> None:
        graph = gr._MicrogridComponentGraph()
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        servicer = MockMicrogridServicer()
        server = MockGrpcServer(servicer, port=58765)
        await server.start()

        target = "[::]:58765"
        client = MicrogridGrpcClient(grpc.aio.insecure_channel(target), target)

        # both components and connections must be non-empty
        servicer.set_components([])
        servicer.set_connections([])
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_api(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        servicer.set_components(
            [(1, components_pb.ComponentCategory.COMPONENT_CATEGORY_GRID)]
        )
        servicer.set_connections([])
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_api(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        servicer.set_components([])
        servicer.set_connections([(1, 2)])
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_api(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # if both are provided, valid graph data must be present

        # valid graph with meter, and EV charger
        servicer.set_components(
            [
                (101, components_pb.ComponentCategory.COMPONENT_CATEGORY_GRID),
                (111, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER),
                (131, components_pb.ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER),
            ]
        )
        servicer.set_connections([(101, 111), (111, 131)])
        await graph.refresh_from_api(client)

        # Note: we need to add GriMetadata as a dict here, because that's what
        # the ComponentGraph does too, and we need to be able to compare the
        # two graphs.
        expected = {
            Component(
                101,
                ComponentCategory.GRID,
                None,
                asdict(GridMetadata(max_current=0.0)),  # type: ignore
            ),
            Component(111, ComponentCategory.METER),
            Component(131, ComponentCategory.EV_CHARGER),
        }
        assert len(graph.components()) == len(expected)
        assert graph.components() == expected
        assert graph.connections() == {
            Connection(101, 111),
            Connection(111, 131),
        }
        graph.validate()

        # if valid graph data is provided, then the existing graph
        # contents will be overwritten
        servicer.set_components(
            [
                (707, components_pb.ComponentCategory.COMPONENT_CATEGORY_GRID),
                (717, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER),
                (727, components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER),
                (737, components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY),
                (747, components_pb.ComponentCategory.COMPONENT_CATEGORY_METER),
            ]
        )
        servicer.set_connections([(707, 717), (717, 727), (727, 737), (717, 747)])
        await graph.refresh_from_api(client)
        expected = {
            Component(
                707,
                ComponentCategory.GRID,
                None,
                asdict(GridMetadata(max_current=0.0)),  # type: ignore
            ),
            Component(717, ComponentCategory.METER),
            Component(727, ComponentCategory.INVERTER, InverterType.NONE),
            Component(737, ComponentCategory.BATTERY),
            Component(747, ComponentCategory.METER),
        }
        assert len(graph.components()) == len(expected)
        assert graph.components() == expected

        assert graph.connections() == {
            Connection(707, 717),
            Connection(717, 727),
            Connection(717, 747),
            Connection(727, 737),
        }
        graph.validate()

        assert await server.graceful_shutdown()

    def test_validate(self) -> None:
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
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.NONE))),
                (3, asdict(Component(3, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 3), (2, 3)])
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph.validate()

        # grid endpoint is not set up correctly: multiple grid endpoints
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.GRID))),
                (3, asdict(Component(3, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (2, 3)])
        with pytest.raises(
            gr.InvalidGraphError, match="Multiple grid endpoints in component graph"
        ):
            graph.validate()

        # leaf components are not set up correctly: a battery has
        # a successor in the graph
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.BATTERY))),
                (3, asdict(Component(3, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (2, 3)])
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph.validate()

    def test__validate_graph(self) -> None:
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
        graph._graph.add_node(1, category=ComponentCategory.GRID)
        with pytest.raises(
            gr.InvalidGraphError, match="No connections in component graph!"
        ):
            graph._validate_graph()

        # graph is not a tree
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.INVERTER))),
                (3, asdict(Component(3, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (2, 3), (3, 2)])
        with pytest.raises(
            gr.InvalidGraphError, match="Component graph is not a tree!"
        ):
            graph._validate_graph()

        # at least one node is completely unconnected
        # (this violates the tree property):
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.METER))),
                (3, asdict(Component(3, ComponentCategory.NONE))),
            ]
        )
        graph._graph.add_edges_from([(1, 2)])
        with pytest.raises(
            gr.InvalidGraphError, match="Component graph is not a tree!"
        ):
            graph._validate_graph()

    def test__validate_graph_root(self) -> None:
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # no node without predecessors (this should already
        # get caught by `_validate_graph` but let's confirm
        # that `_validate_graph_root` also catches it)
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.METER))),
                (2, asdict(Component(2, ComponentCategory.METER))),
                (3, asdict(Component(3, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (2, 3), (3, 1)])
        with pytest.raises(
            gr.InvalidGraphError, match="No valid root nodes of component graph!"
        ):
            graph._validate_graph_root()

        # there are nodes without predecessors, but not of
        # the valid type(s) NONE, GRID, or JUNCTION
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.METER))),
                (2, asdict(Component(2, ComponentCategory.INVERTER))),
                (3, asdict(Component(3, ComponentCategory.BATTERY))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (2, 3)])
        with pytest.raises(
            gr.InvalidGraphError, match="No valid root nodes of component graph!"
        ):
            graph._validate_graph_root()

        # there are multiple different potentially valid
        # root notes
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.NONE))),
                (2, asdict(Component(2, ComponentCategory.GRID))),
                (3, asdict(Component(3, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 3), (2, 3)])
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph._validate_graph_root()

        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.GRID))),
                (3, asdict(Component(3, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 3), (2, 3)])
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph._validate_graph_root()

        # there is just one potential root node but it has no successors
        graph._graph.clear()

        graph._graph.add_nodes_from([(1, asdict(Component(1, ComponentCategory.NONE)))])
        with pytest.raises(
            gr.InvalidGraphError, match="Graph root .*id=1.* has no successors!"
        ):
            graph._validate_graph_root()

        graph._graph.clear()
        graph._graph.add_nodes_from([(2, asdict(Component(2, ComponentCategory.GRID)))])
        with pytest.raises(
            gr.InvalidGraphError, match="Graph root .*id=2.* has no successors!"
        ):
            graph._validate_graph_root()

        graph._graph.clear()

        graph._graph.add_nodes_from([(3, asdict(Component(3, ComponentCategory.GRID)))])
        with pytest.raises(
            gr.InvalidGraphError, match="Graph root .*id=3.* has no successors!"
        ):
            graph._validate_graph_root()

        # there is exactly one potential root node and it has successors
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.NONE))),
                (2, asdict(Component(2, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 2)])
        graph._validate_graph_root()

        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 2)])
        graph._validate_graph_root()

        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edges_from([(1, 2)])
        graph._validate_graph_root()

    def test__validate_grid_endpoint(self) -> None:
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # missing grid endpoint is OK as the graph might have
        # another kind of root
        graph._graph.clear()
        graph._graph.add_node(2, **asdict(Component(2, ComponentCategory.METER)))

        graph._validate_grid_endpoint()

        # multiple grid endpoints
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.METER))),
                (3, asdict(Component(3, ComponentCategory.GRID))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (3, 2)])
        with pytest.raises(
            gr.InvalidGraphError,
            match="Multiple grid endpoints in component graph",
        ):
            graph._validate_grid_endpoint()

        # grid endpoint has predecessors
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (99, asdict(Component(99, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edge(99, 1)
        with pytest.raises(
            gr.InvalidGraphError,
            match=r"Grid endpoint 1 has graph predecessors: \[Component"
            r"\(component_id=99, category=<ComponentCategory.METER: 2>, "
            r"type=None, metadata=None\)\]",
        ) as _err_predecessors:
            graph._validate_grid_endpoint()

        # grid endpoint has no successors
        graph._graph.clear()

        graph._graph.add_node(101, **asdict(Component(101, ComponentCategory.GRID)))
        with pytest.raises(
            gr.InvalidGraphError,
            match="Grid endpoint 101 has no graph successors!",
        ):
            graph._validate_grid_endpoint()

        # valid grid endpoint with at least one successor
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.METER))),
            ]
        )
        graph._graph.add_edge(1, 2)
        graph._validate_grid_endpoint()

    def test__validate_intermediary_components(self) -> None:
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # missing predecessor for at least one intermediary node
        graph._graph.clear()
        graph._graph.add_node(3, **asdict(Component(3, ComponentCategory.INVERTER)))
        with pytest.raises(
            gr.InvalidGraphError,
            match="Intermediary components without graph predecessors",
        ):
            graph._validate_intermediary_components()

        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (3, asdict(Component(3, ComponentCategory.INVERTER))),
            ]
        )
        graph._graph.add_edges_from([(1, 3)])
        graph._validate_intermediary_components()

        graph._graph.clear()

        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.METER))),
                (3, asdict(Component(3, ComponentCategory.INVERTER))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (2, 3)])
        graph._validate_intermediary_components()

        # all intermediary nodes have at least one predecessor
        # and at least one successor
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.METER))),
                (3, asdict(Component(3, ComponentCategory.INVERTER))),
                (4, asdict(Component(4, ComponentCategory.BATTERY))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (2, 3), (3, 4)])
        graph._validate_intermediary_components()

    def test__validate_leaf_components(self) -> None:
        # to ensure clean testing of the individual method,
        # we cheat by setting underlying graph data directly

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # missing predecessor for at least one leaf node
        graph._graph.clear()
        graph._graph.add_node(3, **asdict(Component(3, ComponentCategory.BATTERY)))
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components without graph predecessors"
        ):
            graph._validate_leaf_components()

        graph._graph.clear()
        graph._graph.add_node(4, **asdict(Component(4, ComponentCategory.EV_CHARGER)))
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components without graph predecessors"
        ):
            graph._validate_leaf_components()

        graph._graph.clear()
        graph._graph.add_node(5, **asdict(Component(5, ComponentCategory.PV_ARRAY)))
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components without graph predecessors"
        ):
            graph._validate_leaf_components()

        # successors present for at least one leaf node
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.EV_CHARGER))),
                (3, asdict(Component(3, ComponentCategory.BATTERY))),
            ]
        )

        graph._graph.add_edges_from([(1, 2), (2, 3)])
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph._validate_leaf_components()

        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (3, asdict(Component(3, ComponentCategory.BATTERY))),
                (4, asdict(Component(4, ComponentCategory.EV_CHARGER))),
            ]
        )
        graph._graph.add_edges_from([(1, 3), (3, 4)])
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph._validate_leaf_components()

        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (4, asdict(Component(4, ComponentCategory.EV_CHARGER))),
                (5, asdict(Component(5, ComponentCategory.PV_ARRAY))),
            ]
        )
        graph._graph.add_edges_from([(1, 5), (5, 4)])
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph._validate_leaf_components()

        # all leaf nodes have at least one predecessor
        # and no successors
        graph._graph.clear()
        graph._graph.add_nodes_from(
            [
                (1, asdict(Component(1, ComponentCategory.GRID))),
                (2, asdict(Component(2, ComponentCategory.METER))),
                (3, asdict(Component(3, ComponentCategory.BATTERY))),
                (4, asdict(Component(4, ComponentCategory.EV_CHARGER))),
                (5, asdict(Component(5, ComponentCategory.PV_ARRAY))),
            ]
        )
        graph._graph.add_edges_from([(1, 2), (1, 3), (1, 4), (1, 5)])
        graph._validate_leaf_components()

    def test_graph_correction(self) -> None:
        # Simple test cases for our built-in graph correction
        # functionality.  We test only with `refresh_from`:
        # for `refresh_from_api` it suffices to test that any
        # provided `correct_errors` callback gets invoked,
        # which is already done in `test_refresh_from_api`.

        graph = gr._MicrogridComponentGraph()
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []

        # valid graph data: no correction will be applied
        graph.refresh_from(
            components={
                Component(1, ComponentCategory.GRID),
                Component(2, ComponentCategory.METER),
            },
            connections={Connection(1, 2)},
            correct_errors=gr._correct_graph_errors,
        )
        expected = {
            Component(1, ComponentCategory.GRID),
            Component(2, ComponentCategory.METER),
        }
        assert len(graph.components()) == len(expected)
        assert set(graph.components()) == expected
        assert list(graph.connections()) == [(1, 2)]

        # invalid graph data that (for now at least)
        # cannot be corrected
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={Component(4, ComponentCategory.METER)},
                connections={Connection(3, 4)},
                correct_errors=gr._correct_graph_errors,
            )

        # graph is still in last known good state
        assert len(graph.components()) == len(expected)
        assert set(graph.components()) == expected
        assert list(graph.connections()) == [(1, 2)]

        # invalid graph data where there is no grid
        # endpoint but a node has the magic value 0
        # for its predecessor

        # without the callback, this is identified as
        # invalid
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={Component(8, ComponentCategory.METER)},
                connections={Connection(0, 8)},
            )

        # graph is still in last known good state
        assert len(graph.components()) == len(expected)
        assert set(graph.components()) == expected
        assert list(graph.connections()) == [(1, 2)]

        # with the callback, this can be corrected
        graph.refresh_from(
            components={Component(8, ComponentCategory.METER)},
            connections={Connection(0, 8)},
            correct_errors=gr._correct_graph_errors,
        )
        expected = {
            Component(8, ComponentCategory.METER),
            Component(0, ComponentCategory.GRID),
        }
        assert len(graph.components()) == len(expected)
        assert set(graph.components()) == expected

        assert list(graph.connections()) == [(0, 8)]
