# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Defines states of components that can be used in a microgrid."""
from __future__ import annotations

from enum import Enum

from frequenz.api.microgrid import ev_charger_pb2 as ev_charger_pb

# pylint: disable=no-member


class EVChargerCableState(Enum):
    """Cable states of an EV Charger."""

    UNSPECIFIED = ev_charger_pb.CableState.CABLE_STATE_UNSPECIFIED
    UNPLUGGED = ev_charger_pb.CableState.CABLE_STATE_UNPLUGGED
    CHARGING_STATION_PLUGGED = (
        ev_charger_pb.CableState.CABLE_STATE_CHARGING_STATION_PLUGGED
    )
    CHARGING_STATION_LOCKED = (
        ev_charger_pb.CableState.CABLE_STATE_CHARGING_STATION_LOCKED
    )
    EV_PLUGGED = ev_charger_pb.CableState.CABLE_STATE_EV_PLUGGED
    EV_LOCKED = ev_charger_pb.CableState.CABLE_STATE_EV_LOCKED

    @classmethod
    def from_pb(
        cls, evc_state: ev_charger_pb.CableState.ValueType
    ) -> EVChargerCableState:
        """Convert a protobuf CableState value to EVChargerCableState enum.

        Args:
            evc_state: protobuf cable state to convert.

        Returns:
            Enum value corresponding to the protobuf message.
        """
        if not any(t.value == evc_state for t in EVChargerCableState):
            return cls.UNSPECIFIED

        return EVChargerCableState(evc_state)


class EVChargerComponentState(Enum):
    """Component State of an EV Charger."""

    UNSPECIFIED = ev_charger_pb.ComponentState.COMPONENT_STATE_UNSPECIFIED
    STARTING = ev_charger_pb.ComponentState.COMPONENT_STATE_STARTING
    NOT_READY = ev_charger_pb.ComponentState.COMPONENT_STATE_NOT_READY
    READY = ev_charger_pb.ComponentState.COMPONENT_STATE_READY
    CHARGING = ev_charger_pb.ComponentState.COMPONENT_STATE_CHARGING
    DISCHARGING = ev_charger_pb.ComponentState.COMPONENT_STATE_DISCHARGING
    ERROR = ev_charger_pb.ComponentState.COMPONENT_STATE_ERROR
    AUTHORIZATION_REJECTED = (
        ev_charger_pb.ComponentState.COMPONENT_STATE_AUTHORIZATION_REJECTED
    )
    INTERRUPTED = ev_charger_pb.ComponentState.COMPONENT_STATE_INTERRUPTED

    @classmethod
    def from_pb(
        cls, evc_state: ev_charger_pb.ComponentState.ValueType
    ) -> EVChargerComponentState:
        """Convert a protobuf ComponentState value to EVChargerComponentState enum.

        Args:
            evc_state: protobuf component state to convert.

        Returns:
            Enum value corresponding to the protobuf message.
        """
        if not any(t.value == evc_state for t in EVChargerComponentState):
            return cls.UNSPECIFIED

        return EVChargerComponentState(evc_state)
