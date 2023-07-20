# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Defines the components that can be used in a microgrid."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

import frequenz.api.common.components_pb2 as components_pb
import frequenz.api.microgrid.inverter_pb2 as inverter_pb


class ComponentType(Enum):
    """A base class from which individual component types are derived."""


# pylint: disable=no-member


class InverterType(ComponentType):
    """Enum representing inverter types."""

    NONE = inverter_pb.Type.TYPE_UNSPECIFIED
    BATTERY = inverter_pb.Type.TYPE_BATTERY
    SOLAR = inverter_pb.Type.TYPE_SOLAR
    HYBRID = inverter_pb.Type.TYPE_HYBRID


def _component_type_from_protobuf(
    component_category: components_pb.ComponentCategory.ValueType,
    component_metadata: inverter_pb.Metadata,
) -> Optional[ComponentType]:
    """Convert a protobuf InverterType message to Component enum.

    For internal-only use by the `microgrid` package.

    Args:
        component_category: category the type belongs to.
        component_metadata: protobuf metadata to fetch type from.

    Returns:
        Enum value corresponding to the protobuf message.
    """
    # ComponentType values in the protobuf definition are not unique across categories
    # as of v0.11.0, so we need to check the component category first, before doing any
    # component type checks.
    if (
        component_category
        == components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
    ):
        # mypy 1.4.1 crashes at this line, maybe it doesn't like the name of the "type"
        # attribute in this context.  Hence the "# type: ignore".
        if not any(
            t.value == component_metadata.type for t in InverterType  # type: ignore
        ):
            return None

        return InverterType(component_metadata.type)

    return None


class ComponentCategory(Enum):
    """Possible types of microgrid component."""

    NONE = components_pb.ComponentCategory.COMPONENT_CATEGORY_UNSPECIFIED
    GRID = components_pb.ComponentCategory.COMPONENT_CATEGORY_GRID
    METER = components_pb.ComponentCategory.COMPONENT_CATEGORY_METER
    INVERTER = components_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
    BATTERY = components_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
    EV_CHARGER = components_pb.ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER
    CHP = components_pb.ComponentCategory.COMPONENT_CATEGORY_CHP

    # types not yet supported by the API but which can be inferred
    # from available graph info
    PV_ARRAY = 1000001


def _component_category_from_protobuf(
    component_category: components_pb.ComponentCategory.ValueType,
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
    if component_category == components_pb.ComponentCategory.COMPONENT_CATEGORY_SENSOR:
        raise ValueError("Cannot create a component from a sensor!")

    if not any(t.value == component_category for t in ComponentCategory):
        return ComponentCategory.NONE

    return ComponentCategory(component_category)


@dataclass(frozen=True)
class Component:
    """Metadata for a single microgrid component."""

    component_id: int
    category: ComponentCategory
    type: Optional[ComponentType] = None

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

    POWER_INCLUSION_LOWER_BOUND = "power_inclusion_lower_bound"
    POWER_EXCLUSION_LOWER_BOUND = "power_exclusion_lower_bound"
    POWER_EXCLUSION_UPPER_BOUND = "power_exclusion_upper_bound"
    POWER_INCLUSION_UPPER_BOUND = "power_inclusion_upper_bound"

    ACTIVE_POWER_INCLUSION_LOWER_BOUND = "active_power_inclusion_lower_bound"
    ACTIVE_POWER_EXCLUSION_LOWER_BOUND = "active_power_exclusion_lower_bound"
    ACTIVE_POWER_EXCLUSION_UPPER_BOUND = "active_power_exclusion_upper_bound"
    ACTIVE_POWER_INCLUSION_UPPER_BOUND = "active_power_inclusion_upper_bound"

    TEMPERATURE = "temperature"
