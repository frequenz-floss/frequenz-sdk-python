# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Class that stores pool of batteries and manage them."""

from __future__ import annotations

import asyncio
import logging
from collections import abc
from dataclasses import dataclass
from typing import Dict, Set

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.channels.util import MergeNamed

from ..._internal._asyncio import cancel_and_await
from ._battery_status import BatteryStatusTracker, SetPowerResult, Status

_logger = logging.getLogger(__name__)


@dataclass
class BatteryStatus:
    """Status of the batteries."""

    working: Set[int]
    """Set of working battery ids."""

    uncertain: Set[int]
    """Set of batteries that should be used only if there are no working batteries."""

    def get_working_batteries(self, batteries: abc.Set[int]) -> Set[int]:
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

    def __post_init__(self) -> None:
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
        battery_status_sender: Sender[BatteryStatus],
        max_data_age_sec: float,
        max_blocking_duration_sec: float,
    ) -> None:
        """Create BatteryPoolStatus instance.

        Args:
            battery_ids: set of batteries ids that should be stored in pool.
            battery_status_sender: The sender used for sending the status of the
                batteries in the pool.
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
        set_power_result_channel = Broadcast[SetPowerResult]("battery_request_status")
        self._set_power_result_sender = set_power_result_channel.new_sender()

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
                set_power_result_receiver=set_power_result_channel.new_receiver(
                    f"battery_{battery_id}_request_status"
                ),
            )

        self._battery_status_channel = MergeNamed[Status](
            **receivers,
        )

        self._task = asyncio.create_task(self._run(battery_status_sender))

    async def join(self) -> None:
        """Await for the battery pool, and return when the task completes.

        It will not terminate the program while BatteryPool is working.
        BatteryPool can be stopped with the `stop` method.
        This method is not needed in source code, because BatteryPool is owned
        by the internal code.
        It is needed only when user needs to run his own instance of the BatteryPool.
        """
        await self._task

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

    async def _run(self, battery_status_sender: Sender[BatteryStatus]) -> None:
        """Start tracking batteries status.

        Args:
            battery_status_sender: The sender used for sending the status of the
                batteries in the pool.
        """
        while True:
            try:
                await self._update_status(battery_status_sender)
            except Exception as err:  # pylint: disable=broad-except
                _logger.error(
                    "BatteryPoolStatus failed with error: %s. Restarting.", err
                )

    async def _update_status(
        self, battery_status_sender: Sender[BatteryStatus]
    ) -> None:
        """Wait for any battery to change status and update status.

        Args:
            battery_status_sender: Sender to send the current status of the batteries.
        """
        async for channel_name, status in self._battery_status_channel:
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

            await battery_status_sender.send(self._current_status)

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
        await self._set_power_result_sender.send(
            SetPowerResult(succeed_batteries, failed_batteries)
        )

    def get_working_batteries(self, batteries: abc.Set[int]) -> Set[int]:
        """From the given set of batteries get working.

        Args:
            batteries: Set of batteries

        Returns:
            Subset with working batteries.
        """
        return self._current_status.get_working_batteries(batteries)
