# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Class that tracks the status of pool of components."""


import asyncio
import logging
from collections import abc
from dataclasses import dataclass

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.channels.util import MergeNamed

from ..._internal._asyncio import cancel_and_await
from ._battery_status import BatteryStatusTracker, SetPowerResult
from ._component_status import ComponentPoolStatus, ComponentStatusEnum

_logger = logging.getLogger(__name__)


@dataclass
class _ComponentStatusChannelHelper:
    """Helper class to create component status channel.

    Channel has only one receiver.
    Receiver has size 1, because we need only latest status.
    """

    component_id: int
    """Id of the component for which we should create channel."""

    def __post_init__(self) -> None:
        self.name: str = f"component-{self.component_id}-status"
        channel = Broadcast[ComponentStatusEnum](self.name)

        receiver_name = f"{self.name}-receiver"
        self.receiver = channel.new_receiver(name=receiver_name, maxsize=1)
        self.sender = channel.new_sender()


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
        # At first no component is working, we will get notification when they start
        # working.
        self._current_status = ComponentPoolStatus(working=set(), uncertain=set())

        # Channel for sending results of requests to the components.
        set_power_result_channel = Broadcast[SetPowerResult]("component_request_status")
        self._set_power_result_sender = set_power_result_channel.new_sender()

        self._batteries: dict[str, BatteryStatusTracker] = {}

        # Receivers for individual components statuses are needed to create a
        # `MergeNamed` object.
        receivers: dict[str, Receiver[ComponentStatusEnum]] = {}

        for battery_id in component_ids:
            channel = _ComponentStatusChannelHelper(battery_id)
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

        self._component_status_channel = MergeNamed[ComponentStatusEnum](
            **receivers,
        )

        self._task = asyncio.create_task(self._run(component_status_sender))

    async def join(self) -> None:
        """Wait and return when the instance's task completes.

        It will not terminate the instance, which can be done with the `stop` method.
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
        await self._component_status_channel.stop()

    async def _run(self, component_status_sender: Sender[ComponentPoolStatus]) -> None:
        """Start tracking component status.

        Args:
            component_status_sender: The sender used for sending the status of the
                components in the pool.
        """
        while True:
            try:
                await self._update_status(component_status_sender)
            except Exception as err:  # pylint: disable=broad-except
                _logger.error(
                    "ComponentPoolStatus failed with error: %s. Restarting.", err
                )

    async def _update_status(
        self, component_status_sender: Sender[ComponentPoolStatus]
    ) -> None:
        """Wait for any component to change status and update status.

        Args:
            component_status_sender: Sender to send the current status of components.
        """
        async for channel_name, status in self._component_status_channel:
            component_id = self._batteries[channel_name].battery_id
            if status == ComponentStatusEnum.WORKING:
                self._current_status.working.add(component_id)
                self._current_status.uncertain.discard(component_id)
            elif status == ComponentStatusEnum.UNCERTAIN:
                self._current_status.working.discard(component_id)
                self._current_status.uncertain.add(component_id)
            elif status == ComponentStatusEnum.NOT_WORKING:
                self._current_status.working.discard(component_id)
                self._current_status.uncertain.discard(component_id)

            await component_status_sender.send(self._current_status)

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

    def get_working_components(self, components: abc.Set[int]) -> set[int]:
        """From the given set of components, return only working ones.

        Args:
            components: Set of component IDs.

        Returns:
            IDs of subset with working components.
        """
        return self._current_status.get_working_components(components)
