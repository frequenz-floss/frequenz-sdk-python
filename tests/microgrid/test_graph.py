# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the microgrid component graph."""

# pylint: disable=too-many-lines,use-implicit-booleaness-not-comparison
# pylint: disable=invalid-name,missing-function-docstring,too-many-statements
# pylint: disable=too-many-lines,protected-access

import re
from unittest import mock

import pytest
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid import MicrogridApiClient
from frequenz.client.microgrid.component import (
    Battery,
    BatteryInverter,
    Chp,
    Component,
    ComponentConnection,
    EvCharger,
    GridConnectionPoint,
    Inverter,
    Meter,
    SolarInverter,
    UnrecognizedComponent,
    UnspecifiedBattery,
    UnspecifiedComponent,
    UnspecifiedEvCharger,
    UnspecifiedInverter,
)

import frequenz.sdk.microgrid.component_graph as gr

_MICROGRID_ID = MicrogridId(1)


def _add_components(graph: gr._MicrogridComponentGraph, *components: Component) -> None:
    """Add components to the test graph.

    Args:
        graph: The graph to add the components to.
        *components: The components to add.
    """
    graph._graph.add_nodes_from((c.id, {gr._DATA_KEY: c}) for c in components)


def _add_connections(
    graph: gr._MicrogridComponentGraph, *connections: ComponentConnection
) -> None:
    """Add connections to the test graph.

    Args:
        graph: The graph to add the connections to.
        *connections: The connections to add.
    """
    graph._graph.add_edges_from(
        (c.source, c.destination, {gr._DATA_KEY: c}) for c in connections
    )


def _check_predecessors_and_successors(graph: gr.ComponentGraph) -> None:
    expected_predecessors: dict[ComponentId, set[Component]] = {}
    expected_successors: dict[ComponentId, set[Component]] = {}

    components: dict[ComponentId, Component] = {
        component.id: component for component in graph.components()
    }

    for conn in graph.connections():
        if conn.destination not in expected_predecessors:
            expected_predecessors[conn.destination] = set()
        expected_predecessors[conn.destination].add(components[conn.source])

        if conn.source not in expected_successors:
            expected_successors[conn.source] = set()
        expected_successors[conn.source].add(components[conn.destination])

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
            GridConnectionPoint(
                id=ComponentId(11),
                microgrid_id=_MICROGRID_ID,
                rated_fuse_current=10_000,
            ),
            Meter(id=ComponentId(21), microgrid_id=_MICROGRID_ID),
            Meter(id=ComponentId(41), microgrid_id=_MICROGRID_ID),
            BatteryInverter(id=ComponentId(51), microgrid_id=_MICROGRID_ID),
            UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID),
        }

    @pytest.fixture()
    def sample_input_connections(self) -> set[ComponentConnection]:
        """Create a sample set of connections for testing purposes."""
        return {
            ComponentConnection(source=ComponentId(11), destination=ComponentId(21)),
            ComponentConnection(source=ComponentId(21), destination=ComponentId(41)),
            ComponentConnection(source=ComponentId(41), destination=ComponentId(51)),
            ComponentConnection(source=ComponentId(51), destination=ComponentId(61)),
        }

    @pytest.fixture()
    def sample_graph(
        self,
        sample_input_components: set[Component],
        sample_input_connections: set[ComponentConnection],
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
            match="Component CID1 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(ComponentId(1))
        with pytest.raises(
            KeyError,
            match="Component CID1 not in graph, cannot get successors!",
        ):
            graph.successors(ComponentId(1))

        expected_connection = ComponentConnection(
            source=ComponentId(1), destination=ComponentId(3)
        )

        expected_components = [
            GridConnectionPoint(
                id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
            ),
            Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID),
        ]
        # simplest valid microgrid: a grid endpoint and a meter
        _graph_implementation.refresh_from(
            components=set(expected_components),
            connections={expected_connection},
        )
        assert len(graph.components()) == len(expected_components)
        assert graph.components() == set(expected_components)
        assert graph.connections() == {expected_connection}

        assert graph.predecessors(ComponentId(1)) == set()
        assert graph.successors(ComponentId(1)) == {expected_components[1]}
        assert graph.predecessors(ComponentId(3)) == {expected_components[0]}
        assert graph.successors(ComponentId(3)) == set()
        with pytest.raises(
            KeyError,
            match="Component CID2 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(ComponentId(2))
        with pytest.raises(
            KeyError,
            match="Component CID2 not in graph, cannot get successors!",
        ):
            graph.successors(ComponentId(2))

        input_components = {
            ComponentId(101): GridConnectionPoint(
                id=ComponentId(101),
                microgrid_id=_MICROGRID_ID,
                rated_fuse_current=10_000,
            ),
            ComponentId(102): Meter(id=ComponentId(102), microgrid_id=_MICROGRID_ID),
            ComponentId(104): Meter(id=ComponentId(104), microgrid_id=_MICROGRID_ID),
            ComponentId(105): BatteryInverter(
                id=ComponentId(105), microgrid_id=_MICROGRID_ID
            ),
            ComponentId(106): UnspecifiedBattery(
                id=ComponentId(106), microgrid_id=_MICROGRID_ID
            ),
        }
        input_connections = {
            ComponentConnection(source=ComponentId(101), destination=ComponentId(102)),
            ComponentConnection(source=ComponentId(102), destination=ComponentId(104)),
            ComponentConnection(source=ComponentId(104), destination=ComponentId(105)),
            ComponentConnection(source=ComponentId(105), destination=ComponentId(106)),
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
            match="Component CID9 not in graph, cannot get predecessors!",
        ):
            graph.predecessors(ComponentId(9))
        with pytest.raises(
            KeyError,
            match="Component CID99 not in graph, cannot get successors!",
        ):
            graph.successors(ComponentId(99))

    @pytest.mark.parametrize(
        "int_ids, expected",
        [
            ({1}, set()),
            ({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, set()),
            (
                {11},
                {
                    GridConnectionPoint(
                        id=ComponentId(11),
                        microgrid_id=_MICROGRID_ID,
                        rated_fuse_current=10_000,
                    )
                },
            ),
            ({21}, {Meter(id=ComponentId(21), microgrid_id=_MICROGRID_ID)}),
            ({41}, {Meter(id=ComponentId(41), microgrid_id=_MICROGRID_ID)}),
            ({51}, {BatteryInverter(id=ComponentId(51), microgrid_id=_MICROGRID_ID)}),
            (
                {61},
                {UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID)},
            ),
            (
                {11, 61},
                {
                    GridConnectionPoint(
                        id=ComponentId(11),
                        microgrid_id=_MICROGRID_ID,
                        rated_fuse_current=10_000,
                    ),
                    UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID),
                },
            ),
            (
                {9, 51, 41, 21, 101},
                {
                    Meter(id=ComponentId(41), microgrid_id=_MICROGRID_ID),
                    BatteryInverter(id=ComponentId(51), microgrid_id=_MICROGRID_ID),
                    Meter(id=ComponentId(21), microgrid_id=_MICROGRID_ID),
                },
            ),
        ],
    )
    def test_matching_ids(
        self,
        sample_graph: gr.ComponentGraph,
        int_ids: set[int],
        expected: set[Component],
    ) -> None:
        """Test the graph component query with component ID filter."""
        components = sample_graph.components(
            matching_ids=(ComponentId(id) for id in int_ids)
        )
        assert components == expected

    @pytest.mark.parametrize(
        "types, expected",
        [
            ({EvCharger}, set()),
            (
                {Battery, EvCharger},
                {UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID)},
            ),
            (
                {GridConnectionPoint},
                {
                    GridConnectionPoint(
                        id=ComponentId(11),
                        microgrid_id=_MICROGRID_ID,
                        rated_fuse_current=10_000,
                    )
                },
            ),
            (
                {Meter},
                {
                    Meter(id=ComponentId(21), microgrid_id=_MICROGRID_ID),
                    Meter(id=ComponentId(41), microgrid_id=_MICROGRID_ID),
                },
            ),
            (
                {BatteryInverter},
                {BatteryInverter(id=ComponentId(51), microgrid_id=_MICROGRID_ID)},
            ),
            (
                {Battery},
                {UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID)},
            ),
            (
                {GridConnectionPoint, Battery},
                {
                    GridConnectionPoint(
                        id=ComponentId(11),
                        microgrid_id=_MICROGRID_ID,
                        rated_fuse_current=10_000,
                    ),
                    UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID),
                },
            ),
            (
                {Meter, Battery, EvCharger},
                {
                    Meter(id=ComponentId(21), microgrid_id=_MICROGRID_ID),
                    Meter(id=ComponentId(41), microgrid_id=_MICROGRID_ID),
                    UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID),
                },
            ),
        ],
    )
    def test_matching_types(
        self,
        sample_graph: gr.ComponentGraph,
        types: set[type[Component]],
        expected: set[Component],
    ) -> None:
        """Test the graph component query with component type filter."""
        assert sample_graph.components(matching_types=types) == expected

    @pytest.mark.parametrize(
        "int_ids, types, expected",
        [
            (
                {11},
                {GridConnectionPoint},
                {
                    GridConnectionPoint(
                        id=ComponentId(11),
                        microgrid_id=_MICROGRID_ID,
                        rated_fuse_current=10_000,
                    )
                },
            ),
            ({31}, {GridConnectionPoint}, set()),
            (
                {61},
                {Battery},
                {UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID)},
            ),
            (
                {11, 21, 31, 61},
                {Meter, Battery},
                {
                    UnspecifiedBattery(id=ComponentId(61), microgrid_id=_MICROGRID_ID),
                    Meter(id=ComponentId(21), microgrid_id=_MICROGRID_ID),
                },
            ),
        ],
    )
    def test_matching_ids_and_types(
        self,
        sample_graph: gr.ComponentGraph,
        int_ids: set[int],
        types: set[type[Component]],
        expected: set[Component],
    ) -> None:
        """Test the graph component query with composite filter."""
        # when both filters are applied, they are combined via AND logic, i.e.
        # the component must have one of the specified IDs and be of one of
        # the specified types
        components = sample_graph.components(
            matching_ids=(ComponentId(id) for id in int_ids), matching_types=types
        )
        assert components == expected

    def test_components_without_filters(
        self, sample_input_components: set[Component], sample_graph: gr.ComponentGraph
    ) -> None:
        """Test the graph component query without filters."""
        # without any filter applied, we get back all the components in the graph
        assert len(sample_graph.components()) == len(sample_input_components)
        assert sample_graph.components() == sample_input_components

    def test_connection_filters(self) -> None:  # pylint: disable=too-many-locals
        """Test the graph connection query with filters."""
        # Components
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        charger_4 = UnspecifiedEvCharger(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        charger_5 = UnspecifiedEvCharger(id=ComponentId(5), microgrid_id=_MICROGRID_ID)
        charger_6 = UnspecifiedEvCharger(id=ComponentId(6), microgrid_id=_MICROGRID_ID)

        components = {grid_1, meter_2, meter_3, charger_4, charger_5, charger_6}

        # Connections
        conn_1_2 = ComponentConnection(source=grid_1.id, destination=meter_2.id)
        conn_1_3 = ComponentConnection(source=grid_1.id, destination=meter_3.id)
        conn_2_4 = ComponentConnection(source=meter_2.id, destination=charger_4.id)
        conn_2_5 = ComponentConnection(source=meter_2.id, destination=charger_5.id)
        conn_2_6 = ComponentConnection(source=meter_2.id, destination=charger_6.id)

        connections = {conn_1_2, conn_1_3, conn_2_4, conn_2_5, conn_2_6}
        _graph_implementation = gr._MicrogridComponentGraph(
            components=components,
            connections=connections,
        )
        graph: gr.ComponentGraph = _graph_implementation

        # without any filter applied, we get back all the connections in the graph
        assert graph.connections() == connections

        # with start filter applied, we get back only connections whose `start`
        # component matches one of the provided IDs
        assert graph.connections(matching_sources=ComponentId(8)) == set()
        assert graph.connections(matching_sources=ComponentId(7)) == set()
        assert graph.connections(matching_sources={charger_6.id}) == set()
        assert graph.connections(matching_sources={charger_5.id}) == set()
        assert graph.connections(matching_sources={charger_4.id}) == set()
        assert graph.connections(matching_sources={meter_3.id}) == set()
        assert graph.connections(matching_sources={meter_2.id}) == {
            conn_2_4,
            conn_2_5,
            conn_2_6,
        }
        assert graph.connections(matching_sources={grid_1.id}) == {conn_1_2, conn_1_3}
        assert graph.connections(
            matching_sources={grid_1.id, meter_3.id, charger_5.id}
        ) == {conn_1_2, conn_1_3}
        assert graph.connections(
            matching_sources={grid_1.id, meter_2.id, charger_5.id, charger_6.id}
        ) == {conn_1_2, conn_1_3, conn_2_4, conn_2_5, conn_2_6}

        # with end filter applied, we get back only connections whose `end`
        # component matches one of the provided IDs
        assert graph.connections(matching_destinations=ComponentId(8)) == set()
        assert graph.connections(matching_destinations={charger_6.id}) == {conn_2_6}
        assert graph.connections(matching_destinations={charger_5.id}) == {conn_2_5}
        assert graph.connections(matching_destinations={charger_4.id}) == {conn_2_4}
        assert graph.connections(matching_destinations={meter_3.id}) == {conn_1_3}
        assert graph.connections(matching_destinations={meter_2.id}) == {conn_1_2}
        assert graph.connections(matching_destinations={grid_1.id}) == set()
        assert graph.connections(
            matching_destinations={grid_1.id, meter_2.id, meter_3.id}
        ) == {
            conn_1_2,
            conn_1_3,
        }
        assert graph.connections(
            matching_destinations={charger_4.id, charger_5.id, charger_6.id}
        ) == {conn_2_4, conn_2_5, conn_2_6}

        assert graph.connections(
            matching_destinations={
                meter_2.id,
                charger_4.id,
                charger_6.id,
                ComponentId(8),
            }
        ) == {conn_1_2, conn_2_4, conn_2_6}
        assert graph.connections(matching_destinations={grid_1.id}) == set()

        # when both filters are applied, they are combined via AND logic, i.e.
        # a connection must have its `start` matching one of the provided start
        # values, and its `end` matching one of the provided end values
        assert graph.connections(
            matching_sources={grid_1.id}, matching_destinations={meter_2.id}
        ) == {conn_1_2}
        assert (
            graph.connections(
                matching_sources={meter_2.id}, matching_destinations={meter_3.id}
            )
            == set()
        )
        assert graph.connections(
            matching_sources={grid_1.id, meter_2.id},
            matching_destinations={meter_3.id, charger_4.id},
        ) == {
            conn_1_3,
            conn_2_4,
        }
        assert graph.connections(
            matching_sources={meter_2.id, meter_3.id},
            matching_destinations={charger_5.id, charger_6.id, ComponentId(7)},
        ) == {
            conn_2_5,
            conn_2_6,
        }

    def test_dfs_search_two_grid_meters(self) -> None:
        """Test DFS searching PV components in a graph with two grid meters."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        solar_inverter_4 = SolarInverter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        solar_inverter_5 = SolarInverter(id=ComponentId(5), microgrid_id=_MICROGRID_ID)
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)

        graph = gr._MicrogridComponentGraph(
            components={grid_1, meter_2, meter_3, solar_inverter_4, solar_inverter_5},
            connections={
                ComponentConnection(source=grid_1.id, destination=meter_2.id),
                ComponentConnection(source=grid_1.id, destination=meter_3.id),
                ComponentConnection(source=meter_2.id, destination=solar_inverter_4.id),
                ComponentConnection(source=meter_2.id, destination=solar_inverter_5.id),
            },
        )

        result = graph.dfs(grid_1, set(), graph.is_pv_inverter)
        assert result == {solar_inverter_4, solar_inverter_5}

    def test_dfs_search_grid_meter(self) -> None:
        """Test DFS searching PV components in a graph with a single grid meter."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        solar_meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        solar_meter_4 = Meter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        solar_inverter_5 = SolarInverter(id=ComponentId(5), microgrid_id=_MICROGRID_ID)
        solar_inverter_6 = SolarInverter(id=ComponentId(6), microgrid_id=_MICROGRID_ID)

        solar_meters = {solar_meter_3, solar_meter_4}

        graph = gr._MicrogridComponentGraph(
            components={
                grid_1,
                meter_2,
                *solar_meters,
                solar_inverter_5,
                solar_inverter_6,
            },
            connections={
                ComponentConnection(source=grid_1.id, destination=meter_2.id),
                ComponentConnection(source=meter_2.id, destination=solar_meter_3.id),
                ComponentConnection(source=meter_2.id, destination=solar_meter_4.id),
                ComponentConnection(
                    source=solar_meter_3.id, destination=solar_inverter_5.id
                ),
                ComponentConnection(
                    source=solar_meter_4.id, destination=solar_inverter_6.id
                ),
            },
        )

        result = graph.dfs(grid_1, set(), graph.is_pv_chain)
        assert result == solar_meters

    def test_dfs_search_grid_meter_no_pv_meter(self) -> None:
        """Test DFS searching PV components in a graph with a single grid meter."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        solar_inverter_3 = SolarInverter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        solar_inverter_4 = SolarInverter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        solar_inverters = {solar_inverter_3, solar_inverter_4}

        graph = gr._MicrogridComponentGraph(
            components={grid_1, meter_2, *solar_inverters},
            connections={
                ComponentConnection(source=grid_1.id, destination=meter_2.id),
                ComponentConnection(source=meter_2.id, destination=solar_inverter_3.id),
                ComponentConnection(source=meter_2.id, destination=solar_inverter_4.id),
            },
        )

        result = graph.dfs(grid_1, set(), graph.is_pv_chain)
        assert result == solar_inverters

    def test_dfs_search_no_grid_meter(self) -> None:
        """Test DFS searching PV components in a graph with no grid meter."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        solar_meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        solar_meter_4 = Meter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        solar_meters = {solar_meter_3, solar_meter_4}
        solar_inverter_5 = SolarInverter(id=ComponentId(5), microgrid_id=_MICROGRID_ID)
        solar_inverter_6 = SolarInverter(id=ComponentId(6), microgrid_id=_MICROGRID_ID)

        graph = gr._MicrogridComponentGraph(
            components={
                grid_1,
                meter_2,
                *solar_meters,
                solar_inverter_5,
                solar_inverter_6,
            },
            connections={
                ComponentConnection(source=grid_1.id, destination=meter_2.id),
                ComponentConnection(source=grid_1.id, destination=solar_meter_3.id),
                ComponentConnection(source=grid_1.id, destination=solar_meter_4.id),
                ComponentConnection(
                    source=solar_meter_3.id, destination=solar_inverter_5.id
                ),
                ComponentConnection(
                    source=solar_meter_4.id, destination=solar_inverter_6.id
                ),
            },
        )

        result = graph.dfs(grid_1, set(), graph.is_pv_chain)
        assert result == solar_meters

    def test_dfs_search_nested_components(self) -> None:
        """Test DFS searching PV components in a graph with nested components."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        meter_4 = Meter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        meter_5 = Meter(id=ComponentId(5), microgrid_id=_MICROGRID_ID)
        battery_inverter_6 = BatteryInverter(
            id=ComponentId(6), microgrid_id=_MICROGRID_ID
        )
        battery_inverter_7 = BatteryInverter(
            id=ComponentId(7), microgrid_id=_MICROGRID_ID
        )
        battery_inverter_8 = BatteryInverter(
            id=ComponentId(8), microgrid_id=_MICROGRID_ID
        )
        battery_components = {meter_4, meter_5, battery_inverter_6}

        graph = gr._MicrogridComponentGraph(
            components={
                grid_1,
                meter_2,
                meter_3,
                battery_inverter_7,
                battery_inverter_8,
            }.union(battery_components),
            connections={
                ComponentConnection(source=grid_1.id, destination=meter_2.id),
                ComponentConnection(source=meter_2.id, destination=meter_3.id),
                ComponentConnection(
                    source=meter_2.id, destination=battery_inverter_6.id
                ),
                ComponentConnection(source=meter_3.id, destination=meter_4.id),
                ComponentConnection(source=meter_3.id, destination=meter_5.id),
                ComponentConnection(
                    source=meter_4.id, destination=battery_inverter_7.id
                ),
                ComponentConnection(
                    source=meter_5.id, destination=battery_inverter_8.id
                ),
            },
        )

        assert set() == graph.dfs(grid_1, set(), graph.is_pv_chain)
        assert battery_components == graph.dfs(grid_1, set(), graph.is_battery_chain)

    def test_find_first_descendant_component(self) -> None:
        """Test scenarios for finding the first descendant component."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        battery_inverter_4 = BatteryInverter(
            id=ComponentId(4), microgrid_id=_MICROGRID_ID
        )
        solar_inverter_5 = SolarInverter(id=ComponentId(5), microgrid_id=_MICROGRID_ID)
        ev_charger_6 = UnspecifiedEvCharger(
            id=ComponentId(6), microgrid_id=_MICROGRID_ID
        )

        graph = gr._MicrogridComponentGraph(
            components={
                grid_1,
                meter_2,
                meter_3,
                battery_inverter_4,
                solar_inverter_5,
                ev_charger_6,
            },
            connections={
                ComponentConnection(source=grid_1.id, destination=meter_2.id),
                ComponentConnection(source=meter_2.id, destination=meter_3.id),
                ComponentConnection(
                    source=meter_2.id, destination=battery_inverter_4.id
                ),
                ComponentConnection(source=meter_2.id, destination=solar_inverter_5.id),
                ComponentConnection(source=meter_3.id, destination=ev_charger_6.id),
            },
        )

        # Find the first descendant component of the grid endpoint.
        result = graph.find_first_descendant_component(
            descendants=[Meter],
        )
        assert result == meter_2

        # Find the first descendant component of the grid,
        # considering meter or inverter categories.
        result = graph.find_first_descendant_component(
            descendants=[Meter, Inverter],
        )
        assert result == meter_2

        # Find the first descendant component of the grid,
        # considering only meter category - should return the first meter.
        result = graph.find_first_descendant_component(
            descendants=[Meter],
        )
        assert result == meter_2

        # Verify behavior when component is not found in immediate descendant
        # categories for the first meter.
        with pytest.raises(ValueError):
            graph.find_first_descendant_component(
                descendants=[EvCharger, Battery],
            )

        # Verify behavior when component is not found in immediate descendant
        # categories from the grid component as root.
        with pytest.raises(ValueError):
            graph.find_first_descendant_component(
                descendants=[Inverter],
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

        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        unrecognized_3 = UnrecognizedComponent(
            id=ComponentId(3), microgrid_id=_MICROGRID_ID, category=666
        )
        conn_1_2 = ComponentConnection(source=grid_1.id, destination=meter_2.id)
        conn_1_3 = ComponentConnection(source=grid_1.id, destination=unrecognized_3.id)

        # if components and connections are provided,
        # must provide both non-empty, not one or the
        # other
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(components={grid_1})

        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(connections={conn_1_2})

        # if both are provided, the graph data must itself
        # be valid (we give just a couple of cases of each
        # here: a comprehensive set of the different kinds
        # of invalid graph data are provided in test cases
        # for the different `_validate*` methods)

        # minimal valid microgrid data: a grid endpoint
        # connected to a meter
        grid_and_meter = gr._MicrogridComponentGraph(
            components={grid_1, meter_2}, connections={conn_1_2}
        )
        expected = {grid_1, meter_2}
        assert len(grid_and_meter.components()) == len(expected)
        assert set(grid_and_meter.components()) == expected
        assert list(grid_and_meter.connections()) == [conn_1_2]
        grid_and_meter.validate()

        # invalid graph data: unknown component category
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={grid_1, meter_2, unrecognized_3},
                connections={conn_1_2, conn_1_3},
            )

        # invalid graph data: a connection between components that do not exist
        with pytest.raises(gr.InvalidGraphError):
            gr._MicrogridComponentGraph(
                components={grid_1, meter_2},
                connections={conn_1_2, conn_1_3},
            )

    def test_refresh_from(self) -> None:  # pylint: disable=too-many-locals
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

        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        meter_4 = Meter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        inverter_5 = UnspecifiedInverter(id=ComponentId(5), microgrid_id=_MICROGRID_ID)
        battery_6 = UnspecifiedBattery(id=ComponentId(6), microgrid_id=_MICROGRID_ID)
        grid_7 = GridConnectionPoint(
            id=ComponentId(7), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_8 = Meter(id=ComponentId(8), microgrid_id=_MICROGRID_ID)
        inverter_9 = UnspecifiedInverter(id=ComponentId(9), microgrid_id=_MICROGRID_ID)
        grid_10 = GridConnectionPoint(
            id=ComponentId(10), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        meter_11 = Meter(id=ComponentId(11), microgrid_id=_MICROGRID_ID)

        conn_1_2 = ComponentConnection(source=grid_1.id, destination=meter_2.id)
        conn_2_3 = ComponentConnection(source=meter_2.id, destination=meter_3.id)
        conn_2_4 = ComponentConnection(source=meter_2.id, destination=meter_4.id)
        conn_4_5 = ComponentConnection(source=meter_4.id, destination=inverter_5.id)
        conn_5_6 = ComponentConnection(source=inverter_5.id, destination=battery_6.id)
        conn_7_8 = ComponentConnection(source=meter_3.id, destination=meter_4.id)
        conn_8_9 = ComponentConnection(source=meter_4.id, destination=inverter_5.id)
        conn_9_8 = ComponentConnection(source=inverter_5.id, destination=meter_3.id)
        conn_9_7 = ComponentConnection(source=inverter_5.id, destination=grid_7.id)
        conn_10_11 = ComponentConnection(source=grid_10.id, destination=meter_11.id)

        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(set(), {conn_1_2})
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from({grid_1}, set())
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # if both are provided, valid graph data must be present

        # invalid component
        with pytest.raises(ValueError, match=r"ComponentId can't be negative."):
            graph.refresh_from(
                components={
                    GridConnectionPoint(
                        id=ComponentId(-1),
                        microgrid_id=_MICROGRID_ID,
                        rated_fuse_current=10_000,
                    ),
                    meter_2,
                    meter_3,
                },
                connections={conn_1_2},
            )
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # invalid connection
        with pytest.raises(
            ValueError, match=r"Source and destination components must be different"
        ):
            graph.refresh_from(
                components={grid_1, meter_2, meter_3},
                connections={
                    ComponentConnection(source=grid_1.id, destination=grid_1.id),
                    conn_2_3,
                },
            )
        assert set(graph.components()) == set()
        assert list(graph.connections()) == []
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        expected_components = {grid_1, meter_2, meter_4, inverter_5, battery_6}
        expected_connections = {conn_1_2, conn_2_4, conn_4_5, conn_5_6}
        # valid graph with both load and battery setup
        graph.refresh_from(
            components=expected_components,
            connections=expected_connections,
        )
        assert len(graph.components()) == len(expected_components)
        assert set(graph.components()) == expected_components
        assert graph.connections() == expected_connections
        graph.validate()

        # if invalid graph data is provided (in this case, the graph
        # is not a tree), then the existing contents of the component
        # graph will remain unchanged
        with pytest.raises(gr.InvalidGraphError):
            graph.refresh_from(
                components={grid_7, meter_8, inverter_9},
                connections={conn_7_8, conn_8_9, conn_9_8},
            )

        assert len(graph.components()) == len(expected_components)
        assert graph.components() == expected_components
        assert graph.connections() == expected_connections
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
                    grid_7,
                    inverter_9,
                },
                connections={conn_9_7},
                correct_errors=pretend_to_correct_errors,
            )

        assert error_correction is True

        # if valid graph data is provided, then the existing graph
        # contents will be overwritten
        expected_components = {
            grid_10,
            meter_11,
        }
        graph.refresh_from(
            components=expected_components,
            connections={conn_10_11},
        )
        assert len(graph.components()) == len(expected_components)
        assert set(graph.components()) == expected_components
        assert graph.connections() == {conn_10_11}
        graph.validate()

    async def test_refresh_from_client(self) -> None:
        """Test the refresh_from_client method."""
        graph = gr._MicrogridComponentGraph()
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        client = mock.MagicMock(name="client", spec=MicrogridApiClient)
        client.list_components = mock.AsyncMock(
            name="client.list_components()", return_value=[]
        )
        client.list_connections = mock.AsyncMock(
            name="client.list_connections()", return_value=[]
        )

        # both components and connections must be non-empty
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_client(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        client.list_components.return_value = [
            GridConnectionPoint(
                id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
            )
        ]
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_client(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        client.list_components.return_value = []
        client.list_connections.return_value = [
            ComponentConnection(source=ComponentId(1), destination=ComponentId(2))
        ]
        with pytest.raises(gr.InvalidGraphError):
            await graph.refresh_from_client(client)
        assert graph.components() == set()
        assert graph.connections() == set()
        with pytest.raises(gr.InvalidGraphError):
            graph.validate()

        # if both are provided, valid graph data must be present

        # valid graph with meter, and EV charger
        grid_101 = GridConnectionPoint(
            id=ComponentId(101), microgrid_id=_MICROGRID_ID, rated_fuse_current=0
        )
        meter_111 = Meter(id=ComponentId(111), microgrid_id=_MICROGRID_ID)
        charger_131 = UnspecifiedEvCharger(
            id=ComponentId(131), microgrid_id=_MICROGRID_ID
        )
        expected_components = [grid_101, meter_111, charger_131]
        expected_connections = [
            ComponentConnection(source=grid_101.id, destination=meter_111.id),
            ComponentConnection(source=meter_111.id, destination=charger_131.id),
        ]
        client.list_components.return_value = expected_components
        client.list_connections.return_value = expected_connections
        await graph.refresh_from_client(client)

        # Note: we need to add GriMetadata as a dict here, because that's what
        # the ComponentGraph does too, and we need to be able to compare the
        # two graphs.
        assert graph.components() == set(expected_components)
        assert graph.connections() == set(expected_connections)
        graph.validate()

        # if valid graph data is provided, then the existing graph
        # contents will be overwritten
        grid_707 = GridConnectionPoint(
            id=ComponentId(707), microgrid_id=_MICROGRID_ID, rated_fuse_current=0
        )
        meter_717 = Meter(id=ComponentId(717), microgrid_id=_MICROGRID_ID)
        inverter_727 = UnspecifiedInverter(
            id=ComponentId(727), microgrid_id=_MICROGRID_ID
        )
        battery_737 = UnspecifiedBattery(
            id=ComponentId(737), microgrid_id=_MICROGRID_ID
        )
        meter_747 = Meter(id=ComponentId(747), microgrid_id=_MICROGRID_ID)
        expected_components = [
            grid_707,
            meter_717,
            inverter_727,
            battery_737,
            meter_747,
        ]
        expected_connections = [
            ComponentConnection(source=grid_707.id, destination=meter_717.id),
            ComponentConnection(source=meter_717.id, destination=inverter_727.id),
            ComponentConnection(source=inverter_727.id, destination=battery_737.id),
            ComponentConnection(source=meter_717.id, destination=meter_747.id),
        ]
        client.list_components.return_value = expected_components
        client.list_connections.return_value = expected_connections
        await graph.refresh_from_client(client)

        assert graph.components() == set(expected_components)
        assert graph.connections() == set(expected_connections)
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
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        unspecified_2 = UnspecifiedComponent(
            id=ComponentId(2), microgrid_id=_MICROGRID_ID
        )
        meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        _add_components(graph, grid_1, unspecified_2, meter_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_3.id),
            ComponentConnection(source=unspecified_2.id, destination=meter_3.id),
        )
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph.validate()

        # grid endpoint is not set up correctly: multiple grid endpoints
        graph._graph.clear()
        grid_2 = GridConnectionPoint(
            id=ComponentId(2), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        _add_components(graph, grid_1, grid_2, meter_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_3.id),
            ComponentConnection(source=grid_2.id, destination=meter_3.id),
        )
        with pytest.raises(
            gr.InvalidGraphError,
            match=re.escape(
                r"Multiple potential root nodes: CID1<GridConnectionPoint>, "
                r"CID2<GridConnectionPoint>"
            ),
        ):
            graph.validate()

        # leaf components are not set up correctly: a battery has
        # a successor in the graph
        graph._graph.clear()
        battery_2 = UnspecifiedBattery(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        _add_components(graph, grid_1, battery_2, meter_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=battery_2.id),
            ComponentConnection(source=battery_2.id, destination=meter_3.id),
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
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        _add_components(graph, grid_1)
        with pytest.raises(
            gr.InvalidGraphError, match="No connections in component graph!"
        ):
            graph._validate_graph()

        # graph is not a tree
        graph._graph.clear()
        inverter_2 = UnspecifiedInverter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        _add_components(graph, grid_1, inverter_2, meter_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=inverter_2.id),
            ComponentConnection(source=inverter_2.id, destination=meter_3.id),
            ComponentConnection(source=meter_3.id, destination=inverter_2.id),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="Component graph is not a tree!"
        ):
            graph._validate_graph()

        # at least one node is completely unconnected
        # (this violates the tree property):
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        unspecified_3 = UnspecifiedComponent(
            id=ComponentId(3), microgrid_id=_MICROGRID_ID
        )
        _add_components(graph, grid_1, meter_2, unspecified_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_2.id),
        )
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
        meter_1 = Meter(id=ComponentId(1), microgrid_id=_MICROGRID_ID)
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        _add_components(graph, meter_1, meter_2, meter_3)
        _add_connections(
            graph,
            ComponentConnection(source=meter_1.id, destination=meter_2.id),
            ComponentConnection(source=meter_2.id, destination=meter_3.id),
            ComponentConnection(source=meter_3.id, destination=meter_1.id),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="No valid root nodes of component graph!"
        ):
            graph._validate_graph_root()

        # there are nodes without predecessors, but not of
        # the valid type(s) NONE, GRID, or JUNCTION
        graph._graph.clear()
        inverter_2 = UnspecifiedInverter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        battery_3 = UnspecifiedBattery(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        _add_components(graph, meter_1, inverter_2, battery_3)
        _add_connections(
            graph,
            ComponentConnection(source=meter_1.id, destination=inverter_2.id),
            ComponentConnection(source=inverter_2.id, destination=battery_3.id),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="No valid root nodes of component graph!"
        ):
            graph._validate_graph_root()

        # there are multiple different potentially valid
        # root notes
        graph._graph.clear()
        unspecified_1 = UnspecifiedComponent(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID
        )
        grid_2 = GridConnectionPoint(
            id=ComponentId(2), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        _add_components(graph, unspecified_1, grid_2, meter_3)
        _add_connections(
            graph,
            ComponentConnection(source=unspecified_1.id, destination=meter_3.id),
            ComponentConnection(source=grid_2.id, destination=meter_3.id),
        )
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph._validate_graph_root()

        graph._graph.clear()
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        _add_components(graph, grid_1, grid_2, meter_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_3.id),
            ComponentConnection(source=grid_2.id, destination=meter_3.id),
        )
        with pytest.raises(gr.InvalidGraphError, match="Multiple potential root nodes"):
            graph._validate_graph_root()

        # there is just one potential root node but it has no successors
        graph._graph.clear()
        _add_components(graph, unspecified_1)
        with pytest.raises(
            gr.InvalidGraphError, match="Graph root .*CID1.* has no successors!"
        ):
            graph._validate_graph_root()

        graph._graph.clear()
        _add_components(graph, grid_2)
        with pytest.raises(
            gr.InvalidGraphError, match="Graph root .*CID2.* has no successors!"
        ):
            graph._validate_graph_root()

        graph._graph.clear()
        grid_3 = GridConnectionPoint(
            id=ComponentId(3), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        _add_components(graph, grid_3)
        with pytest.raises(
            gr.InvalidGraphError,
            match=r"Graph root CID3<GridConnectionPoint> has no successors!",
        ):
            graph._validate_graph_root()

        # there is exactly one potential root node and it has successors
        graph._graph.clear()
        _add_components(graph, unspecified_1, meter_2)
        _add_connections(
            graph,
            ComponentConnection(source=unspecified_1.id, destination=meter_2.id),
        )
        graph._validate_graph_root()

        graph._graph.clear()
        _add_components(graph, grid_1, meter_2)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_2.id),
        )
        graph._validate_graph_root()

        graph._graph.clear()
        _add_components(graph, grid_1, meter_2)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_2.id),
        )
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
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        _add_components(graph, meter_2)
        graph._validate_grid_endpoint()

        # multiple grid endpoints
        graph._graph.clear()
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        grid_3 = GridConnectionPoint(
            id=ComponentId(3), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        _add_components(graph, grid_1, meter_2, grid_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_2.id),
            ComponentConnection(source=grid_3.id, destination=meter_2.id),
        )
        with pytest.raises(
            gr.InvalidGraphError,
            match="Multiple grid endpoints in component graph",
        ):
            graph._validate_grid_endpoint()

        # grid endpoint has predecessors
        graph._graph.clear()
        meter_99 = Meter(id=ComponentId(99), microgrid_id=_MICROGRID_ID)
        _add_components(graph, grid_1, meter_99)
        _add_connections(
            graph,
            ComponentConnection(source=meter_99.id, destination=grid_1.id),
        )
        with pytest.raises(
            gr.InvalidGraphError,
            match=re.escape(r"Grid endpoint CID1 has predecessors: CID99<Meter>"),
        ):
            graph._validate_grid_endpoint()

        # grid endpoint has no successors
        graph._graph.clear()
        grid_101 = GridConnectionPoint(
            id=ComponentId(101), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        _add_components(graph, grid_101)
        with pytest.raises(
            gr.InvalidGraphError,
            match="Grid endpoint CID101 has no graph successors!",
        ):
            graph._validate_grid_endpoint()

        # valid grid endpoint with at least one successor
        graph._graph.clear()
        _add_components(
            graph,
            grid_1,
            meter_2,
        )
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_2.id),
        )
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
        inverter_3 = UnspecifiedInverter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        _add_components(graph, inverter_3)
        with pytest.raises(
            gr.InvalidGraphError,
            match="Intermediary components without graph predecessors",
        ):
            graph._validate_intermediary_components()

        graph._graph.clear()
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        _add_components(graph, grid_1, inverter_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=inverter_3.id),
        )
        graph._validate_intermediary_components()

        graph._graph.clear()
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        _add_components(graph, grid_1, meter_2, inverter_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_2.id),
            ComponentConnection(source=meter_2.id, destination=inverter_3.id),
        )
        graph._validate_intermediary_components()

        # all intermediary nodes have at least one predecessor
        # and at least one successor
        graph._graph.clear()
        battery_4 = UnspecifiedBattery(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        _add_components(graph, grid_1, meter_2, inverter_3, battery_4)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_2.id),
            ComponentConnection(source=meter_2.id, destination=inverter_3.id),
            ComponentConnection(source=inverter_3.id, destination=battery_4.id),
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
        battery_3 = UnspecifiedBattery(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        _add_components(graph, battery_3)
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components without graph predecessors"
        ):
            graph._validate_leaf_components()

        graph._graph.clear()
        charger_4 = UnspecifiedEvCharger(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        _add_components(graph, charger_4)
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components without graph predecessors"
        ):
            graph._validate_leaf_components()

        # successors present for at least one leaf node
        graph._graph.clear()
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        charger_2 = UnspecifiedEvCharger(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        _add_components(graph, grid_1, charger_2, battery_3)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=charger_2.id),
            ComponentConnection(source=charger_2.id, destination=battery_3.id),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph._validate_leaf_components()

        graph._graph.clear()
        _add_components(graph, grid_1, battery_3, charger_4)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=battery_3.id),
            ComponentConnection(source=battery_3.id, destination=charger_4.id),
        )
        with pytest.raises(
            gr.InvalidGraphError, match="Leaf components with graph successors"
        ):
            graph._validate_leaf_components()

        # all leaf nodes have at least one predecessor
        # and no successors
        graph._graph.clear()
        meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        _add_components(graph, grid_1, meter_2, battery_3, charger_4)
        _add_connections(
            graph,
            ComponentConnection(source=grid_1.id, destination=meter_2.id),
            ComponentConnection(source=grid_1.id, destination=battery_3.id),
            ComponentConnection(source=grid_1.id, destination=charger_4.id),
        )
        graph._validate_leaf_components()


class TestComponentTypeIdentification:
    """Test the component type identification methods in the component graph."""

    def test_no_comp_meters_pv(self) -> None:
        """Test the case where there are no meters in the graph."""
        grid = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        grid_meter = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        solar_inv_3 = SolarInverter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        solar_inv_4 = SolarInverter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)

        graph = gr._MicrogridComponentGraph(
            components={grid, grid_meter, solar_inv_3, solar_inv_4},
            connections={
                ComponentConnection(source=grid.id, destination=grid_meter.id),
                ComponentConnection(source=grid_meter.id, destination=solar_inv_3.id),
                ComponentConnection(source=grid_meter.id, destination=solar_inv_4.id),
            },
        )

        assert graph.is_grid_meter(grid_meter)
        assert not graph.is_pv_meter(grid_meter)
        assert not graph.is_pv_chain(grid_meter)

        assert graph.is_pv_inverter(solar_inv_3) and graph.is_pv_chain(solar_inv_3)
        assert graph.is_pv_inverter(solar_inv_4) and graph.is_pv_chain(solar_inv_4)

    def test_no_comp_meters_mixed(self) -> None:
        """Test the case where there are no meters in the graph."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        grid_meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        solar_inv_3 = SolarInverter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        battery_inv_4 = BatteryInverter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        battery_5 = UnspecifiedBattery(id=ComponentId(5), microgrid_id=_MICROGRID_ID)

        graph = gr._MicrogridComponentGraph(
            components={
                grid_1,
                grid_meter_2,
                solar_inv_3,
                battery_inv_4,
                battery_5,
            },
            connections={
                ComponentConnection(source=grid_1.id, destination=grid_meter_2.id),
                ComponentConnection(source=grid_meter_2.id, destination=solar_inv_3.id),
                ComponentConnection(
                    source=grid_meter_2.id, destination=battery_inv_4.id
                ),
                ComponentConnection(source=battery_inv_4.id, destination=battery_5.id),
            },
        )

        assert graph.is_grid_meter(grid_meter_2)
        assert not graph.is_pv_meter(grid_meter_2)
        assert not graph.is_pv_chain(grid_meter_2)

        assert graph.is_pv_inverter(solar_inv_3) and graph.is_pv_chain(solar_inv_3)
        assert not graph.is_battery_inverter(
            solar_inv_3
        ) and not graph.is_battery_chain(solar_inv_3)

        assert graph.is_battery_inverter(battery_inv_4) and graph.is_battery_chain(
            battery_inv_4
        )
        assert not graph.is_pv_inverter(battery_inv_4) and not graph.is_pv_chain(
            battery_inv_4
        )

    def test_with_meters(self) -> None:
        """Test the case where there are meters in the graph."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        grid_meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        pv_meter_3 = Meter(id=ComponentId(3), microgrid_id=_MICROGRID_ID)
        pv_inv_4 = SolarInverter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        battery_meter_5 = Meter(id=ComponentId(5), microgrid_id=_MICROGRID_ID)
        battery_inv_6 = BatteryInverter(id=ComponentId(6), microgrid_id=_MICROGRID_ID)
        battery_7 = UnspecifiedBattery(id=ComponentId(7), microgrid_id=_MICROGRID_ID)

        graph = gr._MicrogridComponentGraph(
            components={
                grid_1,
                grid_meter_2,
                pv_meter_3,
                pv_inv_4,
                battery_meter_5,
                battery_inv_6,
                battery_7,
            },
            connections={
                ComponentConnection(source=grid_1.id, destination=grid_meter_2.id),
                ComponentConnection(source=grid_meter_2.id, destination=pv_meter_3.id),
                ComponentConnection(source=pv_meter_3.id, destination=pv_inv_4.id),
                ComponentConnection(
                    source=grid_meter_2.id, destination=battery_meter_5.id
                ),
                ComponentConnection(
                    source=battery_meter_5.id, destination=battery_inv_6.id
                ),
                ComponentConnection(source=battery_inv_6.id, destination=battery_7.id),
            },
        )

        assert graph.is_grid_meter(grid_meter_2)
        assert not graph.is_pv_meter(grid_meter_2)
        assert not graph.is_pv_chain(grid_meter_2)

        assert graph.is_pv_meter(pv_meter_3)
        assert graph.is_pv_chain(pv_meter_3)
        assert graph.is_pv_chain(pv_inv_4)
        assert graph.is_pv_inverter(pv_inv_4)

        assert graph.is_battery_meter(battery_meter_5)
        assert graph.is_battery_chain(battery_meter_5)
        assert graph.is_battery_chain(battery_inv_6)
        assert graph.is_battery_inverter(battery_inv_6)

    def test_without_grid_meters(self) -> None:
        """Test the case where there are no grid meters in the graph."""
        grid_1 = GridConnectionPoint(
            id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
        )
        ev_meter_2 = Meter(id=ComponentId(2), microgrid_id=_MICROGRID_ID)
        ev_charger_3 = UnspecifiedEvCharger(
            id=ComponentId(3), microgrid_id=_MICROGRID_ID
        )
        chp_meter_4 = Meter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
        chp_5 = Chp(id=ComponentId(5), microgrid_id=_MICROGRID_ID)

        graph = gr._MicrogridComponentGraph(
            components={
                grid_1,
                ev_meter_2,
                ev_charger_3,
                chp_meter_4,
                chp_5,
            },
            connections={
                ComponentConnection(source=grid_1.id, destination=ev_meter_2.id),
                ComponentConnection(source=ev_meter_2.id, destination=ev_charger_3.id),
                ComponentConnection(source=grid_1.id, destination=chp_meter_4.id),
                ComponentConnection(source=chp_meter_4.id, destination=chp_5.id),
            },
        )

        assert not graph.is_grid_meter(ev_meter_2)
        assert not graph.is_grid_meter(chp_meter_4)

        assert graph.is_ev_charger_meter(ev_meter_2)
        assert graph.is_ev_charger(ev_charger_3)
        assert graph.is_ev_charger_chain(ev_meter_2)
        assert graph.is_ev_charger_chain(ev_charger_3)

        assert graph.is_chp_meter(chp_meter_4)
        assert graph.is_chp(chp_5)
        assert graph.is_chp_chain(chp_meter_4)
        assert graph.is_chp_chain(chp_5)
