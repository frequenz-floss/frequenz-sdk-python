# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Background service that tracks the status of a battery.

A battery is consider to be WORKING if both the battery and the adjacent inverter are
sending data that shows that they are working.

If either of them stops sending data, or if the data shows that they are not working,
then the battery is considered to be NOT_WORKING.

If a battery and its adjacent inverter are WORKING, but the last request to the battery
failed, then the battery's status is considered to be UNCERTAIN. In this case, the
battery is blocked for a short time, and it is not recommended to use it unless it is
necessary.
"""

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from frequenz.channels import Receiver, Sender, select, selected_from
from frequenz.channels.timer import SkipMissedAndDrift, Timer
from frequenz.client.microgrid import (
    BatteryComponentState,
    BatteryData,
    BatteryRelayState,
    ComponentCategory,
    ComponentData,
    ErrorLevel,
    InverterComponentState,
    InverterData,
)
from typing_extensions import override

from ....microgrid import connection_manager
from ..._background_service import BackgroundService
from ._blocking_status import BlockingStatus
from ._component_status import (
    ComponentStatus,
    ComponentStatusEnum,
    ComponentStatusTracker,
    SetPowerResult,
)

_logger = logging.getLogger(__name__)


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


class BatteryStatusTracker(ComponentStatusTracker, BackgroundService):
    """Class for tracking if battery is working.

    Status updates are sent out only when there is a status change.
    """

    _battery_valid_relay: set[BatteryRelayState] = {BatteryRelayState.CLOSED}
    """The list of valid relay states of a battery.

    A working battery in any other battery relay state will be reported as failing.
    """

    _battery_valid_state: set[BatteryComponentState] = {
        BatteryComponentState.IDLE,
        BatteryComponentState.CHARGING,
        BatteryComponentState.DISCHARGING,
    }
    """The list of valid states of a battery.

    A working battery in any other battery state will be reported as failing.
    """

    _inverter_valid_state: set[InverterComponentState] = {
        InverterComponentState.STANDBY,
        InverterComponentState.IDLE,
        InverterComponentState.CHARGING,
        InverterComponentState.DISCHARGING,
    }
    """The list of valid states of an inverter.

    A working inverter in any other inverter state will be reported as failing.
    """

    @override
    def __init__(  # pylint: disable=too-many-arguments
        self,
        component_id: int,
        max_data_age: timedelta,
        max_blocking_duration: timedelta,
        status_sender: Sender[ComponentStatus],
        set_power_result_receiver: Receiver[SetPowerResult],
    ) -> None:
        """Create class instance.

        Args:
            component_id: Id of this battery
            max_data_age: If component stopped sending data, then this is the maximum
                time when its last message should be considered as valid. After that
                time, component won't be used until it starts sending data.
            max_blocking_duration: This value tell what should be the maximum
                timeout used for blocking failing component.
            status_sender: Channel to send status updates.
            set_power_result_receiver: Channel to receive results of the requests to the
                components.

        Raises:
            RuntimeError: If battery has no adjacent inverter.
        """
        BackgroundService.__init__(self, name=f"BatteryStatusTracker({component_id})")
        self._max_data_age = max_data_age
        self._status_sender = status_sender
        self._set_power_result_receiver = set_power_result_receiver

        # First battery is considered as not working.
        # Change status after first messages are received.
        self._last_status: ComponentStatusEnum = ComponentStatusEnum.NOT_WORKING
        self._blocking_status: BlockingStatus = BlockingStatus(
            min_duration=timedelta(seconds=1.0), max_duration=max_blocking_duration
        )
        self._timedelta_zero = timedelta(seconds=0.0)

        inverter_id = self._find_adjacent_inverter_id(component_id)
        if inverter_id is None:
            raise RuntimeError(
                f"Can't find inverter adjacent to battery: {component_id}"
            )

        self._battery: _ComponentStreamStatus = _ComponentStreamStatus(
            component_id,
            data_recv_timer=Timer(max_data_age, SkipMissedAndDrift()),
        )
        self._inverter: _ComponentStreamStatus = _ComponentStreamStatus(
            inverter_id,
            data_recv_timer=Timer(max_data_age, SkipMissedAndDrift()),
        )

        # Select needs receivers that can be get in async way only.

    @override
    def start(self) -> None:
        """Start the BatteryStatusTracker instance."""
        self._tasks.add(
            asyncio.create_task(
                self._run(self._status_sender, self._set_power_result_receiver)
            )
        )

    @property
    def battery_id(self) -> int:
        """Get battery id.

        Returns:
            Battery id
        """
        return self._battery.component_id

    def _handle_status_battery(self, bat_data: BatteryData) -> None:
        self._battery.last_msg_correct = (
            self._is_message_reliable(bat_data)
            and self._is_battery_state_correct(bat_data)
            and self._no_critical_error(bat_data)
            and self._is_capacity_present(bat_data)
        )
        self._battery.last_msg_timestamp = bat_data.timestamp
        self._battery.data_recv_timer.reset()

    def _handle_status_inverter(self, inv_data: InverterData) -> None:
        self._inverter.last_msg_correct = (
            self._is_message_reliable(inv_data)
            and self._is_inverter_state_correct(inv_data)
            and self._no_critical_error(inv_data)
        )
        self._inverter.last_msg_timestamp = inv_data.timestamp
        self._inverter.data_recv_timer.reset()

    def _handle_status_set_power_result(self, result: SetPowerResult) -> None:
        if self.battery_id in result.succeeded:
            self._blocking_status.unblock()

        elif (
            self.battery_id in result.failed
            and self._last_status != ComponentStatusEnum.NOT_WORKING
        ):
            duration = self._blocking_status.block()

            if duration > self._timedelta_zero:
                _logger.warning(
                    "battery %d failed last response. block it for %s",
                    self.battery_id,
                    duration,
                )

    def _handle_status_battery_timer(self) -> None:
        if self._battery.last_msg_correct:
            self._battery.last_msg_correct = False
            _logger.warning(
                "Battery %d stopped sending data, last timestamp: %s",
                self._battery.component_id,
                self._battery.last_msg_timestamp,
            )

    def _handle_status_inverter_timer(self) -> None:
        if self._inverter.last_msg_correct:
            self._inverter.last_msg_correct = False
            _logger.warning(
                "Inverter %d stopped sending data, last timestamp: %s",
                self._inverter.component_id,
                self._inverter.last_msg_timestamp,
            )

    def _get_new_status_if_changed(self) -> ComponentStatusEnum | None:
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

    async def _run(
        self,
        status_sender: Sender[ComponentStatus],
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

        battery = battery_receiver
        battery_timer = self._battery.data_recv_timer
        inverter_timer = self._inverter.data_recv_timer
        inverter = inverter_receiver
        set_power_result = set_power_result_receiver

        while True:
            try:
                async for selected in select(
                    battery,
                    battery_timer,
                    inverter_timer,
                    inverter,
                    set_power_result,
                ):
                    new_status = None

                    if selected_from(selected, battery):
                        self._handle_status_battery(selected.message)

                    elif selected_from(selected, inverter):
                        self._handle_status_inverter(selected.message)

                    elif selected_from(selected, set_power_result):
                        self._handle_status_set_power_result(selected.message)

                    elif selected_from(selected, battery_timer):
                        if (
                            datetime.now(tz=timezone.utc)
                            - self._battery.last_msg_timestamp
                        ) < self._max_data_age:
                            # This means that we have received data from the battery
                            # since the timer triggered, but the timer event arrived
                            # late, so we can ignore it.
                            continue
                        self._handle_status_battery_timer()

                    elif selected_from(selected, inverter_timer):
                        if (
                            datetime.now(tz=timezone.utc)
                            - self._inverter.last_msg_timestamp
                        ) < self._max_data_age:
                            # This means that we have received data from the inverter
                            # since the timer triggered, but the timer event arrived
                            # late, so we can ignore it.
                            continue
                        self._handle_status_inverter_timer()

                    else:
                        _logger.error("Unknown message returned from select")

                    new_status = self._get_new_status_if_changed()

                    if new_status is not None:
                        await status_sender.send(
                            ComponentStatus(self.battery_id, new_status)
                        )

            except Exception as err:  # pylint: disable=broad-except
                _logger.exception("BatteryStatusTracker crashed with error: %s", err)

    def _get_current_status(self) -> ComponentStatusEnum:
        """Get current battery status.

        Returns:
            Battery status.
        """
        is_msg_correct = (
            self._battery.last_msg_correct and self._inverter.last_msg_correct
        )

        if not is_msg_correct:
            return ComponentStatusEnum.NOT_WORKING
        if self._last_status == ComponentStatusEnum.NOT_WORKING:
            # If message just become correct, then try to use it
            self._blocking_status.unblock()
            return ComponentStatusEnum.WORKING
        if self._blocking_status.is_blocked():
            return ComponentStatusEnum.UNCERTAIN

        return ComponentStatusEnum.WORKING

    def _is_capacity_present(self, msg: BatteryData) -> bool:
        """Check whether the battery capacity is NaN or not.

        If battery capacity is missing, then we can't work with it.

        Args:
            msg: battery message

        Returns:
            True if battery capacity is present, false otherwise.
        """
        if math.isnan(msg.capacity):
            if self._last_status == ComponentStatusEnum.WORKING:
                _logger.warning(
                    "Battery %d capacity is NaN",
                    msg.component_id,
                )
            return False
        return True

    def _no_critical_error(self, msg: BatteryData | InverterData) -> bool:
        """Check if battery or inverter message has any critical error.

        Args:
            msg: message.

        Returns:
            True if message has no critical error, False otherwise.
        """
        critical = ErrorLevel.CRITICAL
        critical_err = next((err for err in msg.errors if err.level == critical), None)
        if critical_err is not None:
            last_status = self._last_status  # pylint: disable=protected-access
            if last_status == ComponentStatusEnum.WORKING:
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
        state = msg.component_state
        if state not in BatteryStatusTracker._inverter_valid_state:
            if self._last_status == ComponentStatusEnum.WORKING:
                _logger.warning(
                    "Inverter %d has invalid state: %s",
                    msg.component_id,
                    state.name,
                )
            return False
        # pylint: enable=protected-access
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
        state = msg.component_state
        if state not in BatteryStatusTracker._battery_valid_state:
            if self._last_status == ComponentStatusEnum.WORKING:
                _logger.warning(
                    "Battery %d has invalid state: %s",
                    self.battery_id,
                    state.name,
                )
            return False

        # Component state is not exposed to the user.
        relay_state = msg.relay_state
        if relay_state not in BatteryStatusTracker._battery_valid_relay:
            if self._last_status == ComponentStatusEnum.WORKING:
                _logger.warning(
                    "Battery %d has invalid relay state: %s",
                    self.battery_id,
                    relay_state.name,
                )
            return False
        return True
        # pylint: enable=protected-access

    def _is_timestamp_outdated(self, timestamp: datetime) -> bool:
        """Return if timestamp is to old.

        Args:
            timestamp: timestamp

        Returns:
            _True if timestamp is to old, False otherwise
        """
        now = datetime.now(tz=timezone.utc)
        diff = now - timestamp
        return diff > self._max_data_age

    def _is_message_reliable(self, message: ComponentData) -> bool:
        """Check if message is too old to be considered as reliable.

        Args:
            message: message to check

        Returns:
            True if message is reliable, False otherwise.
        """
        is_outdated = self._is_timestamp_outdated(message.timestamp)

        if is_outdated and self._last_status == ComponentStatusEnum.WORKING:
            _logger.warning(
                "Component %d stopped sending data. Last timestamp: %s.",
                message.component_id,
                str(message.timestamp),
            )

        return not is_outdated

    def _find_adjacent_inverter_id(self, battery_id: int) -> int | None:
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
