# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utils for tests that uses component graph."""

from dataclasses import dataclass

from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    BatteryInverter,
    Component,
    ComponentConnection,
    DcEvCharger,
    GridConnectionPoint,
    LiIonBattery,
    Meter,
    SolarInverter,
)
from frequenz.microgrid_component_graph import ComponentGraph


@dataclass
class ComponentGraphSpec:
    """Config with information how the component graph should be created."""

    grid_side_meter: bool = True
    """True if the main meter should be by grid side."""

    batteries_num: int = 0
    """Number of batteries in the component graph.

    Each battery will have its own inverter and meter connected.
    """

    solar_inverters_num: int = 0
    """Number of pv inverters in the component graph.

    Each pv inverter will have its own pv meter connected.
    """

    ev_chargers: int = 0
    """Number of ev chargers in the component graph."""


def create_component_graph_structure(
    component_graph_spec: ComponentGraphSpec,
) -> tuple[set[Component], set[ComponentConnection]]:
    """Create structure of components graph.

    Args:
        component_graph_spec: spec that tells what the graph should have.

    Returns:
        Create set of components and set of connections between them.
    """
    microgrid_id = MicrogridId(1)
    grid_id = ComponentId(1)
    main_meter_id = ComponentId(2)

    components = {
        GridConnectionPoint(
            id=grid_id,
            microgrid_id=microgrid_id,
            rated_fuse_current=1000,
        ),
        Meter(
            id=main_meter_id,
            microgrid_id=microgrid_id,
        ),
    }
    connections = {ComponentConnection(source=grid_id, destination=main_meter_id)}

    junction_id = grid_id
    if component_graph_spec.grid_side_meter:
        junction_id = main_meter_id

    start_idx = 3
    for _ in range(component_graph_spec.batteries_num):
        meter_id = ComponentId(start_idx)
        inv_id = ComponentId(int(start_idx) + 1)
        battery_id = ComponentId(start_idx + 2)
        start_idx += 3

        components.add(Meter(id=meter_id, microgrid_id=microgrid_id))
        components.add(LiIonBattery(id=battery_id, microgrid_id=microgrid_id))
        components.add(BatteryInverter(id=inv_id, microgrid_id=microgrid_id))

        connections.add(ComponentConnection(source=junction_id, destination=meter_id))
        connections.add(ComponentConnection(source=meter_id, destination=inv_id))
        connections.add(ComponentConnection(source=inv_id, destination=battery_id))

    for _ in range(component_graph_spec.solar_inverters_num):
        meter_id = ComponentId(start_idx)
        inv_id = ComponentId(start_idx + 1)
        start_idx += 2

        components.add(Meter(id=meter_id, microgrid_id=microgrid_id))
        components.add(SolarInverter(id=inv_id, microgrid_id=microgrid_id))
        connections.add(ComponentConnection(source=junction_id, destination=meter_id))
        connections.add(ComponentConnection(source=meter_id, destination=inv_id))

    for _ in range(component_graph_spec.ev_chargers):
        ev_id = ComponentId(start_idx)
        start_idx += 1

        components.add(DcEvCharger(id=ev_id, microgrid_id=microgrid_id))
        connections.add(ComponentConnection(source=junction_id, destination=ev_id))
    return components, connections


def component_graph_to_mermaid(
    comp_graph: ComponentGraph[Component, ComponentConnection, ComponentId],
) -> str:
    """Return a string representation of the component graph in Mermaid format."""

    def component_to_mermaid(component: Component) -> str:
        return f'"{component.id}"["{component}"]'

    def connection_to_mermaid(connection: ComponentConnection) -> str:
        return f'"{connection.source}" --> "{connection.destination}"'

    components = "\n".join(map(component_to_mermaid, comp_graph.components()))
    connections = "\n".join(map(connection_to_mermaid, comp_graph.connections()))

    return f"graph TD\n{components}\n{connections}"
