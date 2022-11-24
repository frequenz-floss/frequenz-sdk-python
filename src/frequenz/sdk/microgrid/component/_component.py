# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Defines the components that can be used in a microgrid."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import frequenz.api.microgrid.microgrid_pb2 as microgrid_pb


class ComponentCategory(Enum):
    """Possible types of microgrid component."""

    NONE = microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_UNSPECIFIED
    GRID = microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_GRID
    JUNCTION = microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_JUNCTION
    METER = microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER
    INVERTER = microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
    BATTERY = microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
    EV_CHARGER = microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER
    LOAD = microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_LOAD

    # types not yet supported by the API but which can be inferred
    # from available graph info
    PV_ARRAY = 1000001
    CHP = 1000002  # combined heat and power plant


def _component_category_from_protobuf(
    component_category: microgrid_pb.ComponentCategory.ValueType,
) -> ComponentCategory:
    """Convert a protobuf ComponentCategory message to ComponentCategory enum.

    For internal-only use by the `microgrid` package.

    Args:
        component_category: protobuf enum to convert

    Returns:
        Enum value corresponding to the protobuf message.

    Raises:
        ValueError: if `component_category` is a sensor (this is not considered
            a valid component category as it does not form part of the
            microgrid itself)
    """
    if component_category == microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR:
        raise ValueError("Cannot create a component from a sensor!")

    if not any(t.value == component_category for t in ComponentCategory):
        return ComponentCategory.NONE

    return ComponentCategory(component_category)


@dataclass(frozen=True)
class Component:
    """Metadata for a single microgrid component."""

    component_id: int
    category: ComponentCategory

    def is_valid(self) -> bool:
        """Check if this instance contains valid data.

        Returns:
            `True` if `id > 0` and `type` is a valid `ComponentCategory`, or if `id
                == 0` and `type` is `GRID`, `False` otherwise
        """
        return (
            self.component_id > 0 and any(t == self.category for t in ComponentCategory)
        ) or (self.component_id == 0 and self.category == ComponentCategory.GRID)


class ComponentMetricId(Enum):
    """An enum representing the various metrics available in the microgrid."""

    ACTIVE_POWER = "active_power"

    CURRENT_PHASE_1 = "current_phase_1"
    CURRENT_PHASE_2 = "current_phase_2"
    CURRENT_PHASE_3 = "current_phase_3"

    VOLTAGE_PHASE_1 = "voltage_phase_1"
    VOLTAGE_PHASE_2 = "voltage_phase_2"
    VOLTAGE_PHASE_3 = "voltage_phase_3"

    SOC = "soc"
    SOC_LOWER_BOUND = "soc_lower_bound"
    SOC_UPPER_BOUND = "soc_upper_bound"
    CAPACITY = "capacity"

    POWER_LOWER_BOUND = "power_lower_bound"
    POWER_UPPER_BOUND = "power_upper_bound"

    ACTIVE_POWER_LOWER_BOUND = "active_power_lower_bound"
    ACTIVE_POWER_UPPER_BOUND = "active_power_upper_bound"
