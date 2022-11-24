# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Definition of component infos."""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from ..microgrid import ComponentGraph
from ..microgrid.component import ComponentCategory

logger = logging.Logger(__name__)


@dataclass
class ComponentInfo:
    """Object containing component information."""

    # component id
    component_id: int
    # category of component
    category: ComponentCategory
    # type of meter; "pv"/"market"/"grid"/None (when component_category != "Meter")
    meter_connection: Optional[ComponentCategory] = None


# pylint:disable=too-many-branches
def infer_microgrid_config(
    graph: ComponentGraph,
) -> Tuple[List[ComponentInfo], Dict[int, int]]:
    """Infer microgrid configuration from component graph.

    Args:
        graph: component graph of microgrid

    Returns:
        Tuple containing 1) list of components relevant for calculation of microgrid
            data. 2) mappings between batteries and battery inverters.
    """
    component_infos = []
    bat_inv_mappings = {}
    component_category_mappings = {
        component.component_id: component.category for component in graph.components()
    }
    for component in graph.components():
        meter_connection = None
        predecessors = graph.predecessors(component.component_id)
        successors = graph.successors(component.component_id)
        if component.category == ComponentCategory.METER:
            connections = [
                comp
                for comp in predecessors | successors
                if component_category_mappings[comp.component_id]
                != ComponentCategory.JUNCTION
            ]
            if len(connections) == 1:
                meter_connection = component_category_mappings[
                    connections[0].component_id
                ]
            if meter_connection not in {
                ComponentCategory.INVERTER,
                ComponentCategory.EV_CHARGER,
            }:
                component_infos.append(
                    ComponentInfo(
                        component.component_id,
                        component.category,
                        meter_connection=meter_connection,
                    )
                )

        elif component.category == ComponentCategory.BATTERY:
            if len(predecessors) == 0:
                logger.warning(
                    "Battery %d doesn't have any predecessors!",
                    component.component_id,
                )
                continue

            if len(predecessors) > 1:
                inverter_ids = [
                    comp.component_id
                    for comp in predecessors
                    if comp.category == ComponentCategory.INVERTER
                ]
                logger.warning(
                    "Configurations with a single battery %d "
                    "and multiple inverters %s are not supported!",
                    component.component_id,
                    inverter_ids,
                )

            if len(predecessors) >= 1:
                predecessor = predecessors.pop()
                bat_inv_mappings[component.component_id] = predecessor.component_id
                component_infos.append(
                    ComponentInfo(
                        component.component_id,
                        component.category,
                        meter_connection=meter_connection,
                    )
                )
        elif component.category == ComponentCategory.INVERTER:
            if len(successors) > 1:
                battery_ids = [
                    comp.component_id
                    for comp in successors
                    if comp.category == ComponentCategory.BATTERY
                ]
                logger.warning(
                    "Configurations with a single inverter %d "
                    "and multiple batteries %s are not supported!",
                    component.component_id,
                    battery_ids,
                )

            if len(successors) >= 1:
                successor = successors.pop()
                if (
                    component_category_mappings[successor.component_id]
                    == ComponentCategory.BATTERY
                ):
                    component_infos.append(
                        ComponentInfo(
                            component.component_id,
                            component.category,
                            meter_connection=meter_connection,
                        )
                    )
        elif component.category == ComponentCategory.EV_CHARGER:
            component_infos.append(
                ComponentInfo(
                    component.component_id,
                    component.category,
                    meter_connection=meter_connection,
                )
            )
    return component_infos, bat_inv_mappings
