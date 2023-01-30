# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Class that stores pool of batteries and manage them."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Set

from frequenz.channels import Broadcast, Receiver
from frequenz.channels.util import MergeNamed

from ..._internal.asyncio import cancel_and_await
from ._battery_status import BatteryStatusTracker, SetPowerResult, Status

_logger = logging.getLogger(__name__)


@dataclass
class BatteryStatus:
    """Status of the batteries."""

    working: Set[int]
    """Set of working battery ids."""

    uncertain: Set[int]
    """Set of batteries that should be used only if there are no working batteries."""

    def get_working_batteries(self, batteries: Set[int]) -> Set[int]:
        """From the given set of batteries return working batteries.

        Args:
            batteries: Set of batteries

        Returns:
            Subset with working batteries.
        """
        working = self.working.intersection(batteries)
        if len(working) > 0:
            return working
        return self.uncertain.intersection(batteries)


@dataclass
class _BatteryStatusChannelHelper:
    """Helper class to create battery status channel.

    Channel has only one receiver.
    Receiver has size 1, because we need only latest status.
    """

    battery_id: int
    """Id of the battery for which we should create channel."""

    def __post_init__(self):
        self.name: str = f"battery-{self.battery_id}-status"
        channel = Broadcast[Status](self.name)

        receiver_name = f"{self.name}-receiver"
        self.receiver = channel.new_receiver(name=receiver_name, maxsize=1)
        self.sender = channel.new_sender()


class BatteryPoolStatus:
    """Track status of the batteries.

    Send set of working and uncertain batteries, when the any battery change status.
    """

    def __init__(
        self,
        battery_ids: Set[int],
        max_data_age_sec: float,
        max_blocking_duration_sec: float,
    ) -> None:
        """Create BatteryPoolStatus instance.

        Args:
            battery_ids: set of batteries ids that should be stored in pool.
            max_data_age_sec: If component stopped sending data, then
                this is the maximum time when its last message should be considered as
                valid. After that time, component won't be used until it starts sending
                data.
            max_blocking_duration_sec: This value tell what should be the maximum
                timeout used for blocking failing component.

        Raises:
            RuntimeError: If any battery has no adjacent inverter.
        """
        # At first no battery is working, we will get notification when they start
        # working.
        self._current_status = BatteryStatus(working=set(), uncertain=set())

        # Channel for sending results of requests to the batteries
        request_result_channel = Broadcast[SetPowerResult]("battery_request_status")
        self._request_result_sender = request_result_channel.new_sender()

        self._batteries: Dict[str, BatteryStatusTracker] = {}

        # Receivers for individual battery statuses are needed to create a `MergeNamed`
        # object.
        receivers: Dict[str, Receiver[Status]] = {}

        for battery_id in battery_ids:
            channel = _BatteryStatusChannelHelper(battery_id)
            receivers[channel.name] = channel.receiver

            self._batteries[channel.name] = BatteryStatusTracker(
                battery_id=battery_id,
                max_data_age_sec=max_data_age_sec,
                max_blocking_duration_sec=max_blocking_duration_sec,
                status_sender=channel.sender,
                request_result_receiver=request_result_channel.new_receiver(
                    f"battery_{battery_id}_request_status"
                ),
            )

        self._battery_status_channel = MergeNamed[Status](
            **receivers,
        )

        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop tracking batteries status."""
        await cancel_and_await(self._task)

        await asyncio.gather(
            *[
                tracker.stop()  # pylint: disable=protected-access
                for tracker in self._batteries.values()
            ],
        )
        await self._battery_status_channel.stop()

    async def _run(self) -> None:
        """Start tracking batteries status."""
        while True:
            try:
                await self._update_status(self._battery_status_channel)
            except Exception as err:  # pylint: disable=broad-except
                _logger.error(
                    "BatteryPoolStatus failed with error: %s. Restarting.", err
                )

    async def _update_status(self, status_channel: MergeNamed[Status]) -> None:
        """Wait for any battery to change status and update status.

        Args:
            status_channel: Receivers packed in Select object.
        """
        async for channel_name, status in status_channel:
            battery_id = self._batteries[channel_name].battery_id
            if status == Status.WORKING:
                self._current_status.working.add(battery_id)
                self._current_status.uncertain.discard(battery_id)
            elif status == Status.UNCERTAIN:
                self._current_status.working.discard(battery_id)
                self._current_status.uncertain.add(battery_id)
            elif status == Status.NOT_WORKING:
                self._current_status.working.discard(battery_id)
                self._current_status.uncertain.discard(battery_id)

            # In the future here we should send status to the subscribed actors

    async def update_status(
        self, succeed_batteries: Set[int], failed_batteries: Set[int]
    ) -> None:
        """Notify which batteries succeed and failed in the request.

        Batteries that failed will be considered as broken and will be blocked for
        some time.
        Batteries that succeed will be unblocked.

        Args:
            succeed_batteries: Batteries that succeed request
            failed_batteries: Batteries that failed request
        """
        await self._request_result_sender.send(
            SetPowerResult(succeed_batteries, failed_batteries)
        )

    def get_working_batteries(self, batteries: Set[int]) -> Set[int]:
        """From the given set of batteries get working.

        Args:
            batteries: Set of batteries

        Returns:
            Subset with working batteries.
        """
        return self._current_status.get_working_batteries(batteries)
