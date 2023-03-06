# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utils for tests that uses component graph."""
from __future__ import annotations

from dataclasses import dataclass

from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory, InverterType


@dataclass
class ComponentGraphConfig:
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
    component_graph_config: ComponentGraphConfig,
) -> tuple[set[Component], set[Connection]]:
    """Create structure of components graph.

    Args:
        component_graph_config: config that tells what graph should have.

    Returns:
        Create set of components and set of connections between them.
    """
    grid_id = 1
    main_meter_id = 2

    components = {
        Component(grid_id, ComponentCategory.GRID),
        Component(main_meter_id, ComponentCategory.METER),
    }
    connections = {Connection(grid_id, main_meter_id)}

    junction_id: int = grid_id
    if component_graph_config.grid_side_meter:
        junction_id = main_meter_id

    start_idx = 3
    for _ in range(component_graph_config.batteries_num):
        meter_id = start_idx
        inv_id = start_idx + 1
        battery_id = start_idx + 2
        start_idx += 3

        components.add(Component(meter_id, ComponentCategory.METER))
        components.add(Component(battery_id, ComponentCategory.BATTERY))
        components.add(
            Component(inv_id, ComponentCategory.INVERTER, InverterType.BATTERY)
        )

        connections.add(Connection(junction_id, meter_id))
        connections.add(Connection(meter_id, inv_id))
        connections.add(Connection(inv_id, battery_id))

    for _ in range(component_graph_config.solar_inverters_num):
        meter_id = start_idx
        inv_id = start_idx + 1
        start_idx += 2

        components.add(Component(meter_id, ComponentCategory.METER))
        components.add(
            Component(inv_id, ComponentCategory.INVERTER, InverterType.SOLAR)
        )
        connections.add(Connection(junction_id, meter_id))
        connections.add(Connection(meter_id, inv_id))

    for _ in range(component_graph_config.ev_chargers):
        ev_id = start_idx
        start_idx += 1

        components.add(Component(ev_id, ComponentCategory.EV_CHARGER))
        connections.add(Connection(junction_id, ev_id))
    return components, connections
