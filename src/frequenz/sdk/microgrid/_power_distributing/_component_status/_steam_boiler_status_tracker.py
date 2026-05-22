# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Background service that tracks the status of a steam boiler."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from frequenz.channels import Receiver, Sender, select, selected_from
from frequenz.channels.timer import SkipMissedAndDrift, Timer
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    ComponentDataSamples,
    ComponentStateCode,
    SteamBoiler,
)
from frequenz.client.microgrid.metrics import Metric
from typing_extensions import override

from ...._internal._asyncio import run_forever
from ....actor._background_service import BackgroundService
from ... import connection_manager
from ._blocking_status import BlockingStatus
from ._component_status import (
    ComponentStatus,
    ComponentStatusEnum,
    ComponentStatusTracker,
    SetPowerResult,
)

_logger = logging.getLogger(__name__)


class SteamBoilerStatusTracker(ComponentStatusTracker, BackgroundService):
    """Status tracker for steam boilers."""

    @override
    def __init__(  # pylint: disable=too-many-arguments
        self,
        component_id: ComponentId,
        status_sender: Sender[ComponentStatus],
        set_power_result_receiver: Receiver[SetPowerResult],
        *,
        max_data_age: timedelta,
        max_blocking_duration: timedelta,
    ) -> None:
        """Initialize this instance."""
        self._component_id = component_id
        self._max_data_age = max_data_age
        self._status_sender = status_sender
        self._set_power_result_receiver = set_power_result_receiver

        self._last_status = ComponentStatusEnum.NOT_WORKING
        self._blocking_status = BlockingStatus(
            min_duration=timedelta(seconds=1.0),
            max_duration=max_blocking_duration,
        )

        BackgroundService.__init__(
            self, name=f"SteamBoilerStatusTracker({component_id})"
        )

    @override
    def start(self) -> None:
        """Start the status tracker."""
        self._tasks.add(asyncio.create_task(run_forever(self._run)))

    def _is_stale(self, samples: ComponentDataSamples) -> bool:
        """Return whether the given data is stale."""
        if not samples.metric_samples:
            return False

        timestamp = samples.metric_samples[-1].sampled_at
        now = datetime.now(tz=timezone.utc)
        return now - timestamp > self._max_data_age

    def _is_working(self, samples: ComponentDataSamples) -> bool:
        """Return whether the given data indicates that the component is working."""
        if not samples.states:
            return False
        states = samples.states[-1].states
        return bool(
            {
                ComponentStateCode.STANDBY,
                ComponentStateCode.READY,
                ComponentStateCode.CHARGING,
                ComponentStateCode.DISCHARGING,
            }
            & states
        )

    def _handle_component_data(
        self, samples: ComponentDataSamples
    ) -> ComponentStatusEnum:
        """Handle new steam boiler data."""
        if self._is_stale(samples):
            if self._last_status == ComponentStatusEnum.WORKING:
                _logger.warning(
                    "Steam boiler %s data is stale.",
                    self._component_id,
                )
            return ComponentStatusEnum.NOT_WORKING

        if self._is_working(samples):
            if self._last_status == ComponentStatusEnum.NOT_WORKING:
                _logger.info(
                    "Steam boiler %s: state changed to WORKING.",
                    self._component_id,
                )
            return ComponentStatusEnum.WORKING

        if self._last_status == ComponentStatusEnum.WORKING:
            _logger.warning(
                "Steam boiler %s is in NOT_WORKING state.",
                self._component_id,
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
                "Steam boiler %s is in UNCERTAIN state. Set power result: %s",
                self._component_id,
                set_power_result,
            )
        return ComponentStatusEnum.UNCERTAIN

    async def _run(self) -> None:
        """Run the status tracker."""
        components = connection_manager.get().component_graph.components(
            matching_ids={self._component_id}, matching_types={SteamBoiler}
        )
        if not components:
            raise RuntimeError(f"Component {self._component_id} is not a steam boiler")

        component_data_rx = (
            connection_manager.get().api_client.receive_component_data_samples_stream(
                self._component_id,
                metrics={Metric.AC_ACTIVE_POWER},
            )
        )
        set_power_result_rx = self._set_power_result_receiver
        missing_data_timer = Timer(self._max_data_age, SkipMissedAndDrift())

        await self._status_sender.send(
            ComponentStatus(self._component_id, self._last_status)
        )

        async for selected in select(
            component_data_rx, set_power_result_rx, missing_data_timer
        ):
            new_status = ComponentStatusEnum.NOT_WORKING
            if selected_from(selected, component_data_rx):
                missing_data_timer.reset()
                new_status = self._handle_component_data(selected.message)
            elif selected_from(selected, set_power_result_rx):
                new_status = self._handle_set_power_result(selected.message)
            elif selected_from(selected, missing_data_timer):
                _logger.warning(
                    "No steam boiler %s data received for %s. Setting status to NOT_WORKING.",
                    self._component_id,
                    self._max_data_age,
                )

            if (
                self._blocking_status.is_blocked()
                and new_status != ComponentStatusEnum.NOT_WORKING
            ):
                new_status = ComponentStatusEnum.UNCERTAIN

            if new_status != self._last_status:
                _logger.info(
                    "Steam boiler %s status changed from %s to %s",
                    self._component_id,
                    self._last_status,
                    new_status,
                )
                self._last_status = new_status
                await self._status_sender.send(
                    ComponentStatus(self._component_id, new_status)
                )
