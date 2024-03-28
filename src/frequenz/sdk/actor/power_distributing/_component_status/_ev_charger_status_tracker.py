# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Background service that tracks the status of an EV charger."""


import asyncio
import logging
from datetime import datetime, timedelta, timezone

from frequenz.channels import Receiver, Sender, select, selected_from
from frequenz.channels.timer import SkipMissedAndDrift, Timer
from frequenz.client.microgrid import (
    EVChargerCableState,
    EVChargerComponentState,
    EVChargerData,
)
from typing_extensions import override

from frequenz.sdk.microgrid import connection_manager

from ..._background_service import BackgroundService
from ._blocking_status import BlockingStatus
from ._component_status import (
    ComponentStatus,
    ComponentStatusEnum,
    ComponentStatusTracker,
    SetPowerResult,
)

_logger = logging.getLogger(__name__)


class EVChargerStatusTracker(ComponentStatusTracker, BackgroundService):
    """Status tracker for EV chargers.

    It reports an EV charger as `WORKING` when an EV is connected to it,
    and power can be allocated to it, and `NOT_WORKING` otherwise.

    If it receives a power assignment failure from the PowerDistributor,
    when the component is expected to be `WORKING`, it is marked as
    `UNCERTAIN` for a specific interval, before being marked `WORKING`
    again.
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
        """Initialize this instance.

        Args:
            component_id: ID of the EV charger to monitor the status of.
            max_data_age: max duration to wait for, before marking a component as
                NOT_WORKING, unless new data arrives.
            max_blocking_duration: duration for which the component status should be
                UNCERTAIN if a request to the component failed unexpectedly.
            status_sender: Channel sender to send status updates to.
            set_power_result_receiver: Receiver to fetch PowerDistributor responses
                from, to get the status of the most recent request made for an EV
                Charger.
        """
        self._component_id = component_id
        self._max_data_age = max_data_age
        self._status_sender = status_sender
        self._set_power_result_receiver = set_power_result_receiver

        self._last_status = ComponentStatusEnum.NOT_WORKING
        self._blocking_status = BlockingStatus(
            min_duration=timedelta(seconds=1.0),
            max_duration=max_blocking_duration,
        )

        BackgroundService.__init__(self, name=f"EVChargerStatusTracker({component_id})")

    @override
    def start(self) -> None:
        """Start the status tracker."""
        self._tasks.add(asyncio.create_task(self._run_forever()))

    def _is_working(self, ev_data: EVChargerData) -> bool:
        """Return whether the given data indicates that the component is working."""
        return ev_data.cable_state in (
            EVChargerCableState.EV_PLUGGED,
            EVChargerCableState.EV_LOCKED,
        ) and ev_data.component_state in (
            EVChargerComponentState.READY,
            EVChargerComponentState.CHARGING,
            EVChargerComponentState.DISCHARGING,
        )

    def _is_stale(self, ev_data: EVChargerData) -> bool:
        """Return whether the given data is stale."""
        now = datetime.now(tz=timezone.utc)
        stale = now - ev_data.timestamp > self._max_data_age
        return stale

    async def _run_forever(self) -> None:
        """Run the status tracker forever."""
        while True:
            try:
                await self._run()
            except Exception:  # pylint: disable=broad-except
                _logger.exception(
                    "Restarting after exception in EVChargerStatusTracker.run()"
                )
                await asyncio.sleep(1.0)

    def _handle_ev_data(self, ev_data: EVChargerData) -> ComponentStatusEnum:
        """Handle new EV charger data."""
        if self._is_stale(ev_data):
            if self._last_status == ComponentStatusEnum.WORKING:
                _logger.warning(
                    "EV charger %s data is stale. Last timestamp: %s",
                    self._component_id,
                    ev_data.timestamp,
                )
            return ComponentStatusEnum.NOT_WORKING

        if self._is_working(ev_data):
            if self._last_status == ComponentStatusEnum.NOT_WORKING:
                _logger.warning(
                    "EV charger %s is in WORKING state.",
                    self._component_id,
                )
            return ComponentStatusEnum.WORKING

        if self._last_status == ComponentStatusEnum.WORKING:
            _logger.warning(
                "EV charger %s is in NOT_WORKING state. "
                "Cable state: %s, component state: %s",
                self._component_id,
                ev_data.cable_state,
                ev_data.component_state,
            )
        return ComponentStatusEnum.NOT_WORKING

    def _handle_set_power_result(
        self, set_power_result: SetPowerResult
    ) -> ComponentStatusEnum:
        """Handle a new set power result."""
        if self._component_id in set_power_result.succeeded:
            return ComponentStatusEnum.WORKING

        self._blocking_status.block()
        if self._last_status == ComponentStatusEnum.WORKING:
            _logger.warning(
                "EV charger %s is in UNCERTAIN state. Set power result: %s",
                self._component_id,
                set_power_result,
            )
        return ComponentStatusEnum.UNCERTAIN

    async def _run(self) -> None:
        """Run the status tracker."""
        api_client = connection_manager.get().api_client
        ev_data_rx = await api_client.ev_charger_data(self._component_id)
        set_power_result_rx = self._set_power_result_receiver
        missing_data_timer = Timer(self._max_data_age, SkipMissedAndDrift())

        # Send initial status
        await self._status_sender.send(
            ComponentStatus(self._component_id, self._last_status)
        )

        async for selected in select(
            ev_data_rx, set_power_result_rx, missing_data_timer
        ):
            new_status = ComponentStatusEnum.NOT_WORKING
            if selected_from(selected, ev_data_rx):
                missing_data_timer.reset()
                new_status = self._handle_ev_data(selected.message)
            elif selected_from(selected, set_power_result_rx):
                new_status = self._handle_set_power_result(selected.message)
            elif selected_from(selected, missing_data_timer):
                _logger.warning(
                    "No EV charger %s data received for %s. Setting status to NOT_WORKING.",
                    self._component_id,
                    self._max_data_age,
                )

            # Send status update if status changed
            if (
                self._blocking_status.is_blocked()
                and new_status != ComponentStatusEnum.NOT_WORKING
            ):
                new_status = ComponentStatusEnum.UNCERTAIN

            if new_status != self._last_status:
                _logger.info(
                    "EV charger %s status changed from %s to %s",
                    self._component_id,
                    self._last_status,
                    new_status,
                )
                self._last_status = new_status
                await self._status_sender.send(
                    ComponentStatus(self._component_id, new_status)
                )
