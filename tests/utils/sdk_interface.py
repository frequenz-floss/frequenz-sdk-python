# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Create all needed sdk tools. To be replaced by real sdk interface when ready."""
import asyncio
from typing import Any, List

from frequenz.channels import Broadcast

from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    ComponentMetricsResamplingActor,
    DataSourcingActor,
    ResamplerConfig,
)
from frequenz.sdk.actor.power_distributing import BatteryStatus, PowerDistributingActor


class SdkInterface:
    """Sdk interface.

    To be replaced by true sdk interface when ready.
    """

    def __init__(self, resampling_period_s: float) -> None:
        """Create class instance.

        Args:
            resampling_period_s: resampling period for the
                ComponentMetricsResamplingActor.
        """
        # Any to be replaced with BaseActor when ready
        self._actors: List[Any] = []
        self.channel_registry = ChannelRegistry(name="Microgrid Channel Registry")

        self.data_source_request_channel = Broadcast[ComponentMetricRequest](
            "Data Source Request Channel"
        )

        self._actors.append(
            DataSourcingActor(
                request_receiver=self.data_source_request_channel.new_receiver(),
                registry=self.channel_registry,
            )
        )

        self.resampling_actor_request_channel = Broadcast[ComponentMetricRequest](
            "Resampling Actor Request Channel"
        )

        self._actors.append(
            ComponentMetricsResamplingActor(
                channel_registry=self.channel_registry,
                data_sourcing_request_sender=self.data_source_request_channel.new_sender(),
                resampling_request_receiver=self.resampling_actor_request_channel.new_receiver(),
                config=ResamplerConfig(resampling_period_s=resampling_period_s),
            )
        )

        self.battery_status_channel = Broadcast[BatteryStatus]("batteries-status")
        self._power_distributing_actor = PowerDistributingActor(
            users_channels={},
            battery_status_sender=self.battery_status_channel.new_sender(),
        )
        self._actors.append(self._power_distributing_actor)

    async def stop(self) -> None:
        # pylint: disable=protected-access
        """Stop all async tasks and all actors."""
        await self._power_distributing_actor._stop_actor()
        await asyncio.gather(
            *[actor._stop() for actor in self._actors], return_exceptions=True
        )
