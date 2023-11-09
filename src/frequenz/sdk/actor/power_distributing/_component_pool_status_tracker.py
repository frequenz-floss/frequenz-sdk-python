# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Class that tracks the status of pool of components."""


import asyncio
import logging
from collections import abc

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.channels.util import Merge

from ..._internal._asyncio import cancel_and_await
from ._battery_status import BatteryStatusTracker, SetPowerResult
from ._component_status import ComponentPoolStatus, ComponentStatus, ComponentStatusEnum

_logger = logging.getLogger(__name__)


class ComponentPoolStatusTracker:
    """Track status of components of a given category.

    Send set of working and uncertain components, when the status of any of the tracked
    components changes.
    """

    def __init__(  # noqa: DOC502 (RuntimeError raised from BatteryStatusTracker)
        self,
        component_ids: set[int],
        component_status_sender: Sender[ComponentPoolStatus],
        max_data_age_sec: float,
        max_blocking_duration_sec: float,
    ) -> None:
        """Create ComponentPoolStatusTracker instance.

        Args:
            component_ids: set of component ids whose status is to be tracked.
            component_status_sender: The sender used for sending the status of the
                tracked components.
            max_data_age_sec: If a component stops sending data, then this is the
                maximum time for which its last message should be considered as
                valid. After that time, the component won't be used until it starts
                sending data.
            max_blocking_duration_sec: This value tell what should be the maximum
                timeout used for blocking failing component.

        Raises:
            RuntimeError: When managing batteries, if any battery has no adjacent
                inverter.
        """
        self._component_ids = component_ids
        self._max_data_age_sec = max_data_age_sec
        self._max_blocking_duration_sec = max_blocking_duration_sec
        self._component_status_sender = component_status_sender

        # At first no component is working, we will get notification when they start
        # working.
        self._current_status = ComponentPoolStatus(working=set(), uncertain=set())

        # Channel for sending results of requests to the components.
        self._set_power_result_channel = Broadcast[SetPowerResult](
            "component_request_status"
        )
        self._set_power_result_sender = self._set_power_result_channel.new_sender()
        self._component_status_trackers: list[BatteryStatusTracker] = []
        self._merged_status_receiver = self._make_merged_status_receiver()

        self._task = asyncio.create_task(self._run())

    async def join(self) -> None:
        """Wait and return when the instance's task completes.

        It will not terminate the instance, which can be done with the `stop` method.
        """
        await self._task

    async def stop(self) -> None:
        """Stop tracking batteries status."""
        await cancel_and_await(self._task)
        await asyncio.gather(
            *[tracker.stop() for tracker in self._component_status_trackers],
        )
        await self._merged_status_receiver.stop()

    def _make_merged_status_receiver(
        self,
    ) -> Merge[ComponentStatus]:
        status_receivers: list[Receiver[ComponentStatus]] = []

        for component_id in self._component_ids:
            channel: Broadcast[ComponentStatus] = Broadcast(
                f"component_{component_id}_status"
            )
            tracker = BatteryStatusTracker(
                battery_id=component_id,
                max_data_age_sec=self._max_data_age_sec,
                max_blocking_duration_sec=self._max_blocking_duration_sec,
                status_sender=channel.new_sender(),
                set_power_result_receiver=self._set_power_result_channel.new_receiver(),
            )
            self._component_status_trackers.append(tracker)
            status_receivers.append(channel.new_receiver())
        return Merge(*status_receivers)

    async def _run(self) -> None:
        """Start tracking component status."""
        while True:
            try:
                await self._update_status()
            except Exception as err:  # pylint: disable=broad-except
                _logger.error(
                    "ComponentPoolStatus failed with error: %s. Restarting.", err
                )

    async def _update_status(self) -> None:
        async for status in self._merged_status_receiver:
            component_id = status.component_id
            if status.value == ComponentStatusEnum.WORKING:
                self._current_status.working.add(component_id)
                self._current_status.uncertain.discard(component_id)
            elif status.value == ComponentStatusEnum.UNCERTAIN:
                self._current_status.working.discard(component_id)
                self._current_status.uncertain.add(component_id)
            elif status.value == ComponentStatusEnum.NOT_WORKING:
                self._current_status.working.discard(component_id)
                self._current_status.uncertain.discard(component_id)

            await self._component_status_sender.send(self._current_status)

    async def update_status(
        self, succeed_components: set[int], failed_components: set[int]
    ) -> None:
        """Notify which components succeed and failed in the request.

        Components that failed will be considered as broken and will be temporarily
        blocked for some time.

        Components that succeed will be unblocked.

        Args:
            succeed_components: Components that succeed request
            failed_components: Components that failed request
        """
        await self._set_power_result_sender.send(
            SetPowerResult(succeed_components, failed_components)
        )

    def get_working_components(self, components: abc.Set[int]) -> abc.Set[int]:
        """From the given set of components, return only working ones.

        Args:
            components: Set of component IDs.

        Returns:
            IDs of subset with working components.
        """
        return self._current_status.get_working_components(components)
