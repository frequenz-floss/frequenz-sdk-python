# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Defines states of components that can be used in a microgrid."""
from __future__ import annotations

from enum import Enum

from frequenz.api.microgrid import ev_charger_pb2 as ev_charger_pb


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
