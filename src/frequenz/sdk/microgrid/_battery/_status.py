# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Class to return battery status."""

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

from .. import ComponentGraph
from .. import get as get_microgrid
from ..component import BatteryData, ComponentCategory, ComponentData, InverterData

# Time needed to get first component data.
# Constant for now. In future should be configurable in UI.
_START_DELAY_SEC = 2.0

_logger = logging.getLogger(__name__)

T = TypeVar("T")


class BatteryStatus(Enum):
    """Tells if battery is can be used."""

    # Component it is not working.
    NOT_WORKING = 0
    # Component should work, but it failed in last request. It is blocked for few
    # seconds and it is not recommended to use it until it is necessary.
    UNCERTAIN = 1
    # Component is working
    WORKING = 2


@dataclass
class _ComponentReceiver(Generic[T]):
    component_id: int
    receiver: Peekable[T]


class StatusTracker:
    """Class for tracking if battery is working."""

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
        InverterComponentState.COMPONENT_STATE_DISCHARGING,
    }

    def __init__(
        self, battery_id: int, max_data_age_sec: float, max_blocking_duration_sec: float
    ) -> None:
        """Create class instance.

        Args:
            battery_id: Id of this battery
            max_data_age_sec: If component stopped sending data, then
                this is the maximum time when its last message should be considered as
                valid. After that time, component won't be used until it starts sending
                data.
            max_blocking_duration_sec: This value tell what should be the maximum
                timeout used for blocking failing component.
        """
        self._battery_id: int = battery_id
        self._max_data_age: float = max_data_age_sec

        self._init_method_called: bool = False
        self._last_status: BatteryStatus = BatteryStatus.WORKING

        self._min_blocking_duration_sec: float = 1.0
        self._max_blocking_duration_sec: float = max_blocking_duration_sec
        self._blocked_until: Optional[datetime] = None
        self._last_blocking_duration_sec: float = self._min_blocking_duration_sec

    @property
    def battery_id(self) -> int:
        """Get battery id.

        Returns:
            Battery id
        """
        return self._battery_id

    async def async_init(self) -> None:
        """Async part of constructor.

        This should be called in order to subscribe for necessary data.

        Raises:
            RuntimeError: If given battery as no adjacent inverter.
        """
        component_graph = get_microgrid().component_graph
        inverter_id = self._find_adjacent_inverter_id(component_graph)
        if inverter_id is None:
            raise RuntimeError(
                f"Can't find inverter adjacent to battery: {self._battery_id}"
            )

        api_client = get_microgrid().api_client

        bat_recv = await api_client.battery_data(self._battery_id)
        # Defining battery_receiver and inverter receiver needs await.
        # Because of that it is impossible to define it in constructor.
        # Setting it to None first would require to check None condition in
        # every call.
        # pylint: disable=attribute-defined-outside-init
        self._battery_receiver = _ComponentReceiver(
            self._battery_id, bat_recv.into_peekable()
        )
        inv_recv = await api_client.inverter_data(inverter_id)
        # pylint: disable=attribute-defined-outside-init
        self._inverter_receiver = _ComponentReceiver(
            inverter_id, inv_recv.into_peekable()
        )

        await asyncio.sleep(_START_DELAY_SEC)
        self._init_method_called = True

    def get_status(self) -> BatteryStatus:
        """Return status of the battery.

        The decision is made based on last message from battery and adjacent inverter
        and result from last request.

        Raises:
            RuntimeError: If method `async_init` was not called or not awaited.

        Returns:
            Battery status
        """
        if not self._init_method_called:
            raise RuntimeError(
                "`async_init` method not called or not awaited. Run it before first use"
            )

        bat_msg = self._battery_receiver.receiver.peek()
        inv_msg = self._inverter_receiver.receiver.peek()
        if bat_msg is None or inv_msg is None:
            if self._last_status == BatteryStatus.WORKING:
                if not bat_msg:
                    component_id = self._battery_id
                else:
                    component_id = self._inverter_receiver.component_id

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

        if not is_msg_ok:
            self._last_status = BatteryStatus.NOT_WORKING
            return self._last_status

        # Use battery as soon as its message is back correct.
        if self._last_status == BatteryStatus.NOT_WORKING:
            self.unblock()

        if self.is_blocked():
            self._last_status = BatteryStatus.UNCERTAIN
        else:
            self._last_status = BatteryStatus.WORKING

        return self._last_status

    def is_blocked(self) -> bool:
        """Return if battery is blocked.

        Battery can be blocked if last request for that battery failed.

        Returns:
            True if battery is blocked, False otherwise.
        """
        if self._blocked_until is None:
            return False
        return self._blocked_until > datetime.now(tz=timezone.utc)

    def unblock(self) -> None:
        """Unblock battery.

        This will reset duration of the next blocking timeout.

        Battery can be blocked using `self.block()` method.
        """
        self._blocked_until = None

    def block(self) -> float:
        """Block battery.

        Battery can be unblocked using `self.unblock()` method.

        Returns:
            For how long (in seconds) the battery is blocked.
        """
        now = datetime.now(tz=timezone.utc)

        if self._blocked_until is None:
            self._last_blocking_duration_sec = self._min_blocking_duration_sec
            self._blocked_until = now + timedelta(
                seconds=self._last_blocking_duration_sec
            )
            return self._last_blocking_duration_sec

        # If still blocked, then do nothing
        if self._blocked_until > now:
            return 0.0

        # Increase blocking duration twice or until it reach the max.
        self._last_blocking_duration_sec = min(
            2 * self._last_blocking_duration_sec, self._max_blocking_duration_sec
        )
        self._blocked_until = now + timedelta(seconds=self._last_blocking_duration_sec)

        return self._last_blocking_duration_sec

    @property
    def blocked_until(self) -> Optional[datetime]:
        """Return timestamp when the battery will be unblocked.

        Returns:
            Timestamp when the battery will be unblocked. Return None if battery is
                not blocked.
        """
        if self._blocked_until is None or self._blocked_until < datetime.now(
            tz=timezone.utc
        ):
            return None
        return self._blocked_until

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
        if state not in StatusTracker._inverter_valid_state:
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
        if state not in StatusTracker._battery_valid_state:
            if self._last_status == BatteryStatus.WORKING:
                _logger.warning(
                    "Battery %d has invalid state: %s",
                    self._battery_id,
                    str(state),
                )
            return False

        # Component state is not exposed to the user.
        # pylint: disable=protected-access
        relay_state = msg._relay_state
        if relay_state not in StatusTracker._battery_valid_relay:
            if self._last_status == BatteryStatus.WORKING:
                _logger.warning(
                    "Battery %d has invalid relay state: %s",
                    self._battery_id,
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

    def _find_adjacent_inverter_id(self, graph: ComponentGraph) -> Optional[int]:
        """Find inverter adjacent to this battery.

        Args:
            graph: Component graph

        Returns:
            Id of the inverter. If battery hasn't adjacent inverter, then return None.
        """
        return next(
            (
                comp.component_id
                for comp in graph.predecessors(self._battery_id)
                if comp.category == ComponentCategory.INVERTER
            ),
            None,
        )
