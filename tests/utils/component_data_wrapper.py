# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Wrappers for the ComponentData.

This module allows the easy construction of mock ComponentData instances to be used in
unit tests. Usually, all parameters for an instance need to be defined, this module
helps by allowing users to only specify the parameters they are interested in for their
tests. The rest will be filled with default protobuf values.
This also abstracts away changes in the protobuf definition and minimizes the places
that will need to be updated in such cases.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Tuple

import frequenz.api.microgrid.battery_pb2 as battery_pb
import frequenz.api.microgrid.inverter_pb2 as inverter_pb

from frequenz.sdk.microgrid.component import (
    BatteryData,
    EVChargerCableState,
    EVChargerComponentState,
    EVChargerData,
    InverterData,
    MeterData,
)


@dataclass(frozen=True)
class BatteryDataWrapper(BatteryData):
    """Wrapper for the BatteryData with default arguments."""

    soc: float = float("NaN")
    soc_lower_bound: float = float("NaN")
    soc_upper_bound: float = float("NaN")
    capacity: float = float("NaN")
    power_lower_bound: float = float("NaN")
    power_upper_bound: float = float("NaN")
    temperature_max: float = float("NaN")
    _relay_state: battery_pb.RelayState.ValueType = (
        battery_pb.RelayState.RELAY_STATE_UNSPECIFIED
    )
    _component_state: battery_pb.ComponentState.ValueType = (
        battery_pb.ComponentState.COMPONENT_STATE_UNSPECIFIED
    )
    _errors: list[battery_pb.Error] = field(default_factory=list)

    def copy_with_new_timestamp(self, new_timestamp: datetime) -> BatteryDataWrapper:
        """Copy the component data but insert new timestamp.

        Because the dataclass is frozen, we can't just replace the timestamp.
        We have to copy it.

        Args:
            new_timestamp: New timestamp.

        Returns:
            Copied component data.
        """
        return replace(self, timestamp=new_timestamp)


@dataclass(frozen=True)
class InverterDataWrapper(InverterData):
    """Wrapper for the InverterData with default arguments."""

    active_power: float = float("NaN")
    active_power_lower_bound: float = float("NaN")
    active_power_upper_bound: float = float("NaN")
    _component_state: inverter_pb.ComponentState.ValueType = (
        inverter_pb.ComponentState.COMPONENT_STATE_UNSPECIFIED
    )
    _errors: list[inverter_pb.Error] = field(default_factory=list)

    def copy_with_new_timestamp(self, new_timestamp: datetime) -> InverterDataWrapper:
        """Copy the component data but insert new timestamp.

        Because the dataclass is frozen, we can't just replace the timestamp.
        We have to copy it.

        Args:
            new_timestamp: New timestamp.

        Returns:
            Copied component data.
        """
        return replace(self, timestamp=new_timestamp)


@dataclass(frozen=True)
class EvChargerDataWrapper(EVChargerData):
    """Wrapper for the EvChargerData with default arguments."""

    active_power: float = float("NaN")
    current_per_phase: Tuple[float, float, float] = field(
        default_factory=lambda: (float("NaN"), float("NaN"), float("NaN"))
    )
    voltage_per_phase: Tuple[float, float, float] = field(
        default_factory=lambda: (float("NaN"), float("NaN"), float("NaN"))
    )
    cable_state: EVChargerCableState = EVChargerCableState.UNSPECIFIED
    component_state: EVChargerComponentState = EVChargerComponentState.UNSPECIFIED

    def copy_with_new_timestamp(self, new_timestamp: datetime) -> EvChargerDataWrapper:
        """Copy the component data but insert new timestamp.

        Because the dataclass is frozen, we can't just replace the timestamp.
        We have to copy it.

        Args:
            new_timestamp: New timestamp.

        Returns:
            Copied component data.
        """
        return replace(self, timestamp=new_timestamp)


@dataclass(frozen=True)
class MeterDataWrapper(MeterData):
    """Wrapper for the MeterData with default arguments."""

    active_power: float = float("NaN")
    current_per_phase: Tuple[float, float, float] = field(
        default_factory=lambda: (float("NaN"), float("NaN"), float("NaN"))
    )
    voltage_per_phase: Tuple[float, float, float] = field(
        default_factory=lambda: (float("NaN"), float("NaN"), float("NaN"))
    )
    frequency: float = float("NaN")

    def copy_with_new_timestamp(self, new_timestamp: datetime) -> MeterDataWrapper:
        """Copy the component data but insert new timestamp.

        Because the dataclass is frozen, we can't just replace the timestamp.
        We have to copy it.

        Args:
            new_timestamp: New timestamp.

        Returns:
            Copied component data.
        """
        return replace(self, timestamp=new_timestamp)
