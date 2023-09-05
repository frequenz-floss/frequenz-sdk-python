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
    """Unspecified cable state."""

    UNPLUGGED = ev_charger_pb.CableState.CABLE_STATE_UNPLUGGED
    """The cable is unplugged."""

    CHARGING_STATION_PLUGGED = (
        ev_charger_pb.CableState.CABLE_STATE_CHARGING_STATION_PLUGGED
    )
    """The cable is plugged into the charging station."""

    CHARGING_STATION_LOCKED = (
        ev_charger_pb.CableState.CABLE_STATE_CHARGING_STATION_LOCKED
    )
    """The cable is plugged into the charging station and locked."""

    EV_PLUGGED = ev_charger_pb.CableState.CABLE_STATE_EV_PLUGGED
    """The cable is plugged into the EV."""

    EV_LOCKED = ev_charger_pb.CableState.CABLE_STATE_EV_LOCKED
    """The cable is plugged into the EV and locked."""

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
    """Unspecified component state."""

    STARTING = ev_charger_pb.ComponentState.COMPONENT_STATE_STARTING
    """The component is starting."""

    NOT_READY = ev_charger_pb.ComponentState.COMPONENT_STATE_NOT_READY
    """The component is not ready."""

    READY = ev_charger_pb.ComponentState.COMPONENT_STATE_READY
    """The component is ready."""

    CHARGING = ev_charger_pb.ComponentState.COMPONENT_STATE_CHARGING
    """The component is charging."""

    DISCHARGING = ev_charger_pb.ComponentState.COMPONENT_STATE_DISCHARGING
    """The component is discharging."""

    ERROR = ev_charger_pb.ComponentState.COMPONENT_STATE_ERROR
    """The component is in error state."""

    AUTHORIZATION_REJECTED = (
        ev_charger_pb.ComponentState.COMPONENT_STATE_AUTHORIZATION_REJECTED
    )
    """The component rejected authorization."""

    INTERRUPTED = ev_charger_pb.ComponentState.COMPONENT_STATE_INTERRUPTED
    """The component is interrupted."""

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
