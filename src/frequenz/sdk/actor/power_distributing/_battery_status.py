# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Class to return battery status."""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable, Optional, Set, TypeVar, Union

from frequenz.api.microgrid.battery_pb2 import ComponentState as BatteryComponentState
from frequenz.api.microgrid.battery_pb2 import RelayState as BatteryRelayState
from frequenz.api.microgrid.common_pb2 import ErrorLevel
from frequenz.api.microgrid.inverter_pb2 import ComponentState as InverterComponentState
from frequenz.channels import Receiver, Sender
from frequenz.channels.util import Select, Timer

from frequenz.sdk._internal.asyncio import cancel_and_await
from frequenz.sdk.microgrid import connection_manager
from frequenz.sdk.microgrid.component import (
    BatteryData,
    ComponentCategory,
    ComponentData,
    InverterData,
)

_logger = logging.getLogger(__name__)


class Status(Enum):
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
class SetPowerResult:
    """Information what batteries succeed or failed the last request."""

    succeed: Iterable[int]
    """Set of the batteries that succeed."""

    failed: Iterable[int]
    """Set of the batteries that failed."""


T = TypeVar("T")


@dataclass
class _ComponentStreamStatus:
    component_id: int
    """Component id."""

    data_recv_timer: Timer
    """Timer that is set when no component data has been received for some time."""

    last_msg_timestamp: datetime = datetime.now(tz=timezone.utc)
    """Timestamp of the last message from the component."""

    last_msg_correct: bool = False
    """Flag whether last message was correct or not."""


@dataclass
class _BlockingStatus:
    min_duration_sec: float
    max_duration_sec: float

    def __post_init__(self) -> None:
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

    def unblock(self) -> None:
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


class BatteryStatusTracker:
    """Class for tracking if battery is working.

    Status updates are sent out only when there is a status change.
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
        InverterComponentState.COMPONENT_STATE_DISCHARGING,
    }

    def __init__(  # pylint: disable=too-many-arguments
        self,
        battery_id: int,
        max_data_age_sec: float,
        max_blocking_duration_sec: float,
        status_sender: Sender[Status],
        set_power_result_receiver: Receiver[SetPowerResult],
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
            status_sender: Channel to send status updates.
            set_power_result_receiver: Channel to receive results of the requests to the
                components.

        Raises:
            RuntimeError: If battery has no adjacent inverter.
        """
        self._max_data_age = max_data_age_sec
        # First battery is considered as not working.
        # Change status after first messages are received.
        self._last_status: Status = Status.NOT_WORKING
        self._blocking_status: _BlockingStatus = _BlockingStatus(
            1.0, max_blocking_duration_sec
        )

        inverter_id = self._find_adjacent_inverter_id(battery_id)
        if inverter_id is None:
            raise RuntimeError(f"Can't find inverter adjacent to battery: {battery_id}")

        self._battery: _ComponentStreamStatus = _ComponentStreamStatus(
            battery_id, data_recv_timer=Timer(max_data_age_sec)
        )
        self._inverter: _ComponentStreamStatus = _ComponentStreamStatus(
            inverter_id, data_recv_timer=Timer(max_data_age_sec)
        )

        # Select needs receivers that can be get in async way only.
        self._select: Select | None = None

        self._task: asyncio.Task[None] = asyncio.create_task(
            self._run(status_sender, set_power_result_receiver)
        )

    @property
    def battery_id(self) -> int:
        """Get battery id.

        Returns:
            Battery id
        """
        return self._battery.component_id

    async def stop(self) -> None:
        """Stop tracking battery status."""
        await cancel_and_await(self._task)

        if self._select is not None:
            await self._select.stop()

    async def _run(
        self,
        status_sender: Sender[Status],
        set_power_result_receiver: Receiver[SetPowerResult],
    ) -> None:
        """Process data from the components and set_power_result_receiver.

        New status is send only when it change.

        Args:
            status_sender: Channel to send status updates.
            set_power_result_receiver: Channel to receive results of the requests to the
                components.
        """
        api_client = connection_manager.get().api_client

        battery_receiver = await api_client.battery_data(self._battery.component_id)
        inverter_receiver = await api_client.inverter_data(self._inverter.component_id)

        self._select = Select(
            battery=battery_receiver,
            battery_timer=self._battery.data_recv_timer,
            inverter_timer=self._inverter.data_recv_timer,
            inverter=inverter_receiver,
            set_power_result=set_power_result_receiver,
        )

        while True:
            try:
                while await self._select.ready():
                    new_status = self._update_status(self._select)

                    if new_status is not None:
                        await status_sender.send(new_status)

            except Exception as err:  # pylint: disable=broad-except
                _logger.exception("BatteryStatusTracker crashed with error: %s", err)

    def _update_status(self, select: Select) -> Optional[Status]:
        if msg := select.battery:
            self._battery.last_msg_correct = (
                self._is_message_reliable(msg.inner)
                and self._is_battery_state_correct(msg.inner)
                and self._no_critical_error(msg.inner)
                and self._is_capacity_present(msg.inner)
            )
            self._battery.last_msg_timestamp = msg.inner.timestamp
            self._battery.data_recv_timer.reset()

        elif msg := select.inverter:
            self._inverter.last_msg_correct = (
                self._is_message_reliable(msg.inner)
                and self._is_inverter_state_correct(msg.inner)
                and self._no_critical_error(msg.inner)
            )
            self._inverter.last_msg_timestamp = msg.inner.timestamp
            self._inverter.data_recv_timer.reset()

        elif msg := select.set_power_result:
            result: SetPowerResult = msg.inner
            if self.battery_id in result.succeed:
                self._blocking_status.unblock()

            elif (
                self.battery_id in result.failed
                and self._last_status != Status.NOT_WORKING
            ):
                duration = self._blocking_status.block()

                if duration > 0:
                    _logger.warning(
                        "battery %d failed last response. block it for %f sec",
                        self.battery_id,
                        duration,
                    )
        elif msg := select.battery_timer:
            if self._battery.last_msg_correct:
                self._battery.last_msg_correct = False
                _logger.warning(
                    "Battery %d stopped sending data, last timestamp: %s",
                    self._battery.component_id,
                    self._battery.last_msg_timestamp,
                )
        elif msg := select.inverter_timer:
            if self._inverter.last_msg_correct:
                self._inverter.last_msg_correct = False
                _logger.warning(
                    "Inverter %d stopped sending data, last timestamp: %s",
                    self._inverter.component_id,
                    self._inverter.last_msg_timestamp,
                )

        else:
            _logger.error("Unknown message returned from select")

        current_status = self._get_current_status()
        if self._last_status != current_status:
            self._last_status = current_status
            _logger.info(
                "battery %d changed status %s",
                self.battery_id,
                str(self._last_status),
            )
            return current_status

        return None

    def _get_current_status(self) -> Status:
        """Get current battery status.

        Returns:
            Battery status.
        """
        is_msg_correct = (
            self._battery.last_msg_correct and self._inverter.last_msg_correct
        )

        if not is_msg_correct:
            return Status.NOT_WORKING
        if self._last_status == Status.NOT_WORKING:
            # If message just become correct, then try to use it
            self._blocking_status.unblock()
            return Status.WORKING
        if self._blocking_status.is_blocked():
            return Status.UNCERTAIN

        return Status.WORKING

    def _is_capacity_present(self, msg: BatteryData) -> bool:
        """Check whether the battery capacity is NaN or not.

        If battery capacity is missing, then we can't work with it.

        Args:
            msg: battery message

        Returns:
            True if battery capacity is present, false otherwise.
        """
        if math.isnan(msg.capacity):
            if self._last_status == Status.WORKING:
                _logger.warning(
                    "Battery %d capacity is NaN",
                    msg.component_id,
                )
            return False
        return True

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
            if self._last_status == Status.WORKING:
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
            if self._last_status == Status.WORKING:
                _logger.warning(
                    "Inverter %d has invalid state: %s",
                    msg.component_id,
                    InverterComponentState.Name(state),
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
            if self._last_status == Status.WORKING:
                _logger.warning(
                    "Battery %d has invalid state: %s",
                    self.battery_id,
                    BatteryComponentState.Name(state),
                )
            return False

        # Component state is not exposed to the user.
        # pylint: disable=protected-access
        relay_state = msg._relay_state
        if relay_state not in BatteryStatusTracker._battery_valid_relay:
            if self._last_status == Status.WORKING:
                _logger.warning(
                    "Battery %d has invalid relay state: %s",
                    self.battery_id,
                    BatteryRelayState.Name(relay_state),
                )
            return False
        return True

    def _is_timestamp_outdated(self, timestamp: datetime) -> bool:
        """Return if timestamp is to old.

        Args:
            timestamp: timestamp

        Returns:
            _True if timestamp is to old, False otherwise
        """
        now = datetime.now(tz=timezone.utc)
        diff = (now - timestamp).total_seconds()
        return diff > self._max_data_age

    def _is_message_reliable(self, message: ComponentData) -> bool:
        """Check if message is too old to be considered as reliable.

        Args:
            message: message to check

        Returns:
            True if message is reliable, False otherwise.
        """
        is_outdated = self._is_timestamp_outdated(message.timestamp)

        if is_outdated and self._last_status == Status.WORKING:
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
        graph = connection_manager.get().component_graph
        return next(
            (
                comp.component_id
                for comp in graph.predecessors(battery_id)
                if comp.category == ComponentCategory.INVERTER
            ),
            None,
        )
