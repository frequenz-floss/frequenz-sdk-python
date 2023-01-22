# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Class to return battery status."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Generic, Optional, Set, TypeVar, Union

from frequenz.api.microgrid.battery_pb2 import ComponentState as BatteryComponentState
from frequenz.api.microgrid.battery_pb2 import RelayState as BatteryRelayState
from frequenz.api.microgrid.common_pb2 import ErrorLevel
from frequenz.api.microgrid.inverter_pb2 import ComponentState as InverterComponentState
from frequenz.channels import Peekable

from ..._internal.asyncio import AsyncConstructible
from .. import get as get_microgrid
from ..component import BatteryData, ComponentCategory, ComponentData, InverterData

# Time needed to get first component data.
# Constant for now. In future should be configurable in UI.
_START_DELAY_SEC = 2.0

_logger = logging.getLogger(__name__)

T = TypeVar("T")


class BatteryStatus(Enum):
    """Tells if battery is can be used."""

    NOT_WORKING = 0
    """Component is not working and should not be used"""

    UNCERTAIN = 1
    """Component should work, but it failed in last request. It is blocked for few
    seconds and it is not recommended to use it unless it is necessary.
    """

    WORKING = 2
    """Component is working"""


@dataclass
class _ComponentData(Generic[T]):
    component_id: int
    receiver: Peekable[T]


@dataclass
class _BlockingStatus:
    min_duration_sec: float
    max_duration_sec: float

    def __post_init__(self):
        self.last_blocking_duration_sec: float = self.min_duration_sec
        self.blocked_until: Optional[datetime] = None

    def block(self) -> float:
        """Block battery.

        Battery can be unblocked using `self.unblock()` method.

        Returns:
            For how long (in seconds) the battery is blocked.
        """
        now = datetime.now(tz=timezone.utc)

        # If is not blocked
        if self.blocked_until is None:
            self.last_blocking_duration_sec = self.min_duration_sec
            self.blocked_until = now + timedelta(
                seconds=self.last_blocking_duration_sec
            )
            return self.last_blocking_duration_sec

        # If still blocked, then do nothing
        if self.blocked_until > now:
            return 0.0

        # If previous blocking time expired, then blocked it once again.
        # Increase last blocking time, unless it reach the maximum.
        self.last_blocking_duration_sec = min(
            2 * self.last_blocking_duration_sec, self.max_duration_sec
        )
        self.blocked_until = now + timedelta(seconds=self.last_blocking_duration_sec)

        return self.last_blocking_duration_sec

    def unblock(self):
        """Unblock battery.

        This will reset duration of the next blocking timeout.

        Battery can be blocked using `self.block()` method.
        """
        self.blocked_until = None

    def is_blocked(self) -> bool:
        """Return if battery is blocked.

        Battery can be blocked if last request for that battery failed.

        Returns:
            True if battery is blocked, False otherwise.
        """
        if self.blocked_until is None:
            return False
        return self.blocked_until > datetime.now(tz=timezone.utc)


class BatteryStatusTracker(AsyncConstructible):
    """Class for tracking if battery is working.

    To create an instance of this class you should use `async_new` class method.
    Standard constructor (__init__) is not supported and using it will raise
    `NotSyncConstructible` error.
    """

    # Class attributes
    _battery_valid_relay: Set[BatteryRelayState.ValueType] = {
        BatteryRelayState.RELAY_STATE_CLOSED
    }
    _battery_valid_state: Set[BatteryComponentState.ValueType] = {
        BatteryComponentState.COMPONENT_STATE_IDLE,
        BatteryComponentState.COMPONENT_STATE_CHARGING,
        BatteryComponentState.COMPONENT_STATE_DISCHARGING,
    }
    _inverter_valid_state: Set[InverterComponentState.ValueType] = {
        InverterComponentState.COMPONENT_STATE_STANDBY,
        InverterComponentState.COMPONENT_STATE_IDLE,
        InverterComponentState.COMPONENT_STATE_CHARGING,
        InverterComponentState.COMPONENT_STATE_STANDBY,
    }

    # Instance attributes
    _battery_id: int
    _max_data_age: float
    _last_status: BatteryStatus
    _blocking_status: _BlockingStatus
    _battery: _ComponentData[BatteryData]
    _inverter: _ComponentData[InverterData]

    @classmethod
    async def async_new(
        cls, battery_id: int, max_data_age_sec: float, max_blocking_duration_sec: float
    ) -> BatteryStatusTracker:
        """Create class instance.

        Args:
            battery_id: Id of this battery
            max_data_age_sec: If component stopped sending data, then
                this is the maximum time when its last message should be considered as
                valid. After that time, component won't be used until it starts sending
                data.
            max_blocking_duration_sec: This value tell what should be the maximum
                timeout used for blocking failing component.

        Returns:
            New instance of this class.

        Raises:
            RuntimeError: If battery has no adjacent inverter.
        """
        self: BatteryStatusTracker = BatteryStatusTracker.__new__(cls)
        self._max_data_age = max_data_age_sec

        self._last_status = BatteryStatus.WORKING

        self._blocking_status = _BlockingStatus(1.0, max_blocking_duration_sec)

        inverter_id = self._find_adjacent_inverter_id(battery_id)
        if inverter_id is None:
            raise RuntimeError(f"Can't find inverter adjacent to battery: {battery_id}")

        api_client = get_microgrid().api_client

        bat_recv = await api_client.battery_data(battery_id)
        self._battery = _ComponentData(battery_id, bat_recv.into_peekable())

        inv_recv = await api_client.inverter_data(inverter_id)
        self._inverter = _ComponentData(inverter_id, inv_recv.into_peekable())

        await asyncio.sleep(_START_DELAY_SEC)
        return self

    @property
    def battery_id(self) -> int:
        """Get battery id.

        Returns:
            Battery id
        """
        return self._battery.component_id

    def get_status(self) -> BatteryStatus:
        """Return status of the battery.

        The decision is made based on last message from battery and adjacent inverter
        and result from last request.

        Raises:
            RuntimeError: If method `async_init` was not called or not awaited.

        Returns:
            Battery status
        """
        bat_msg = self._battery.receiver.peek()
        inv_msg = self._inverter.receiver.peek()
        if bat_msg is None or inv_msg is None:
            if self._last_status == BatteryStatus.WORKING:
                if not bat_msg:
                    component_id = self._battery.component_id
                else:
                    component_id = self._inverter.component_id

                _logger.warning(
                    "None returned from component %d receiver.", component_id
                )
            self._last_status = BatteryStatus.NOT_WORKING
            return self._last_status

        older_message = bat_msg if bat_msg.timestamp < inv_msg.timestamp else inv_msg
        is_msg_ok = (
            self._is_message_reliable(older_message)
            and self._is_battery_state_correct(bat_msg)
            and self._is_inverter_state_correct(inv_msg)
            and self._no_critical_error(bat_msg)
            and self._no_critical_error(inv_msg)
        )

        self._last_status = self._get_current_status(is_msg_ok)
        return self._last_status

    def _get_current_status(self, is_msg_correct: bool) -> BatteryStatus:
        """Get current battery status.

        Args:
            is_msg_correct: Whether messages from components are correct.

        Returns:
            Battery status.
        """

        if not is_msg_correct:
            return BatteryStatus.NOT_WORKING
        if self._last_status == BatteryStatus.NOT_WORKING:
            # If message just become correct, then try to use it
            self._blocking_status.unblock()
            return BatteryStatus.WORKING
        if self._blocking_status.is_blocked():
            return BatteryStatus.UNCERTAIN

        return BatteryStatus.WORKING

    def is_blocked(self) -> bool:
        """Return if battery is blocked.

        Battery can be blocked if last request for that battery failed.

        Returns:
            True if battery is blocked, False otherwise.
        """
        return self._blocking_status.is_blocked()

    def unblock(self) -> None:
        """Unblock battery.

        This will reset duration of the next blocking timeout.

        Battery can be blocked using `self.block()` method.
        """
        self._blocking_status.unblock()

    def block(self) -> float:
        """Block battery.

        Battery can be unblocked using `self.unblock()` method.

        Returns:
            For how long (in seconds) the battery is blocked.
        """
        return self._blocking_status.block()

    @property
    def blocked_until(self) -> Optional[datetime]:
        """Return timestamp when the battery will be unblocked.

        Returns:
            Timestamp when the battery will be unblocked. Return None if battery is
                not blocked.
        """
        return self._blocking_status.blocked_until

    def _no_critical_error(self, msg: Union[BatteryData, InverterData]) -> bool:
        """Check if battery or inverter message has any critical error.

        Args:
            msg: message.

        Returns:
            True if message has no critical error, False otherwise.
        """
        critical = ErrorLevel.ERROR_LEVEL_CRITICAL
        # pylint: disable=protected-access
        critical_err = next((err for err in msg._errors if err.level == critical), None)
        if critical_err is not None:
            if self._last_status == BatteryStatus.WORKING:
                _logger.warning(
                    "Component %d has critical error: %s",
                    msg.component_id,
                    str(critical_err),
                )
            return False
        return True

    def _is_inverter_state_correct(self, msg: InverterData) -> bool:
        """Check if inverter is in correct state from message.

        Args:
            msg: message

        Returns:
            True if inverter is in correct state. False otherwise.
        """
        # Component state is not exposed to the user.
        # pylint: disable=protected-access
        state = msg._component_state
        if state not in BatteryStatusTracker._inverter_valid_state:
            if self._last_status == BatteryStatus.WORKING:
                _logger.warning(
                    "Inverter %d has invalid state: %s",
                    msg.component_id,
                    str(state),
                )
            return False
        return True

    def _is_battery_state_correct(self, msg: BatteryData) -> bool:
        """Check if battery is in correct state from message.

        Args:
            msg: message

        Returns:
            True if battery is in correct state. False otherwise.
        """
        # Component state is not exposed to the user.
        # pylint: disable=protected-access
        state = msg._component_state
        if state not in BatteryStatusTracker._battery_valid_state:
            if self._last_status == BatteryStatus.WORKING:
                _logger.warning(
                    "Battery %d has invalid state: %s",
                    self._battery.component_id,
                    str(state),
                )
            return False

        # Component state is not exposed to the user.
        # pylint: disable=protected-access
        relay_state = msg._relay_state
        if relay_state not in BatteryStatusTracker._battery_valid_relay:
            if self._last_status == BatteryStatus.WORKING:
                _logger.warning(
                    "Battery %d has invalid relay state: %s",
                    self._battery.component_id,
                    str(relay_state),
                )
            return False
        return True

    def _is_message_reliable(self, message: ComponentData) -> bool:
        """Check if message is too old to be considered as reliable.

        Args:
            message: message to check

        Returns:
            True if message is reliable, False otherwise.
        """
        now = datetime.now(tz=timezone.utc)
        diff = (now - message.timestamp).total_seconds()
        is_outdated = diff > self._max_data_age

        if is_outdated and self._last_status == BatteryStatus.WORKING:
            _logger.warning(
                "Component %d stopped sending data. Last timestamp: %s.",
                message.component_id,
                str(message.timestamp),
            )

        return not is_outdated

    def _find_adjacent_inverter_id(self, battery_id: int) -> Optional[int]:
        """Find inverter adjacent to this battery.

        Args:
            battery_id: battery id adjacent to the wanted inverter

        Returns:
            Id of the inverter. If battery hasn't adjacent inverter, then return None.
        """
        graph = get_microgrid().component_graph
        return next(
            (
                comp.component_id
                for comp in graph.predecessors(battery_id)
                if comp.category == ComponentCategory.INVERTER
            ),
            None,
        )
