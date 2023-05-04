# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Frequenz Python SDK usage examples.

This example creates two users.
One user sends request with power to apply in PowerDistributingActor.
Second user receives requests and set that power.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from queue import Queue
from typing import List, Optional, Set

from frequenz.channels import Bidirectional, Broadcast, Receiver, Sender

from frequenz.sdk import actor, microgrid
from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    ComponentMetricsResamplingActor,
    DataSourcingActor,
    ResamplerConfig,
)
from frequenz.sdk.actor.power_distributing import (
    BatteryStatus,
    PowerDistributingActor,
    Request,
    Result,
    Success,
)
from frequenz.sdk.microgrid import connection_manager
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.logical_meter import LogicalMeter

_logger = logging.getLogger(__name__)
HOST = "microgrid.sandbox.api.frequenz.io"  # it should be the host name.
PORT = 61060


@actor.actor
class DecisionMakingActor:
    """Actor that receives set receives power for given batteries."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        power_channel: Receiver[List[float]],
        power_distributor_handle: Bidirectional.Handle[Request, Result],
        batteries: Set[int],
    ) -> None:
        """Create actor instance.

        Args:
            power_channel: channel where actor receives requests
            power_distributor_handle: Channel
                for communication with power distributor
            batteries: Batteries to charge/discharge
        """
        self._power_channel = power_channel
        self._power_distributor_handle = power_distributor_handle
        self._batteries = batteries

    async def run(self) -> None:
        """Run actor.

        Raises:
            RuntimeError: If any channel was closed unexpectedly
        """
        while True:
            # wait for request with blocking
            request: Optional[List[float]] = await self._power_channel.receive()

            if request is None:
                raise RuntimeError("Request channel has been closed.")

            avg_power = sum(request) / len(request)
            _logger.debug("Avg power %d", avg_power)
            power_to_set: float
            if avg_power > 30000:
                # Charge
                power_to_set = 10000.0
            else:
                # Discharge
                power_to_set = -10000.0

            await self._power_distributor_handle.send(
                Request(
                    power_to_set,
                    batteries=self._batteries,
                    request_timeout_sec=2.0,
                )
            )
            try:
                result: Optional[Result] = await asyncio.wait_for(
                    self._power_distributor_handle.receive(), timeout=3
                )
            except asyncio.exceptions.TimeoutError:
                _logger.error(
                    "Got timeout error when waiting for response from PowerDistributingActor"
                )
                continue
            if result is None:
                raise RuntimeError("PowerDistributingActor channel has been closed.")
            if not isinstance(result, Success):
                _logger.error(
                    "Could not set %d power. Result: %s", power_to_set, type(result)
                )
            else:
                _logger.info("Set power with %d succeed.", power_to_set)


@actor.actor
class DataCollectingActor:
    """Actor that makes decisions about how much to charge/discharge batteries."""

    def __init__(
        self,
        request_channel: Sender[List[float]],
        active_power_data: Receiver[Sample],
    ) -> None:
        """Create actor instance.

        Args:
            request_channel: channel where actor send requests
            active_power_data: channel to get active power of batteries in microgrid
        """
        self._request_channel = request_channel
        self._active_power_data = active_power_data

    async def run(self) -> None:
        """Run actor.

        Raises:
            RuntimeError: If communication channel has been closed.
        """
        while True:
            queue: Queue[Optional[float]] = Queue(maxsize=50)
            for _ in range(5):
                active_power = await self._active_power_data.receive()
                if active_power is None:
                    _logger.error("No data for active power. Channel closed.")
                    continue

                time_data = active_power.timestamp
                if (datetime.now(timezone.utc) - time_data).total_seconds() > 30:
                    _logger.error("Active power data are stale")
                    continue
                queue.put_nowait(active_power.value)

            await self._request_channel.send(list(queue.queue))


async def run() -> None:
    """Run main functions that initializes and creates everything."""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
    )
    await microgrid.initialize(
        HOST, PORT, ResamplerConfig(resampling_period=timedelta(seconds=1.0))
    )

    channel_registry = ChannelRegistry(name="Microgrid Channel Registry")

    data_source_request_channel = Broadcast[ComponentMetricRequest](
        "Data Source Request Channel"
    )

    resampling_actor_request_channel = Broadcast[ComponentMetricRequest](
        "Resampling Actor Request Channel"
    )

    _ds_actor = DataSourcingActor(
        request_receiver=data_source_request_channel.new_receiver(),
        registry=channel_registry,
    )

    _resampling_actor = ComponentMetricsResamplingActor(
        channel_registry=channel_registry,
        data_sourcing_request_sender=data_source_request_channel.new_sender(),
        resampling_request_receiver=resampling_actor_request_channel.new_receiver(),
        config=ResamplerConfig(resampling_period=timedelta(seconds=1.0)),
    )

    logical_meter = LogicalMeter(
        channel_registry,
        resampling_actor_request_channel.new_sender(),
    )
    sending_actor_id: str = "SendingActor"
    # Bidirectional channel is used for one sender - one receiver communication
    power_distributor_channels = {
        sending_actor_id: Bidirectional[Request, Result](
            client_id=sending_actor_id, service_id="PowerDistributingActor"
        )
    }

    battery_status_channel = Broadcast[BatteryStatus]("battery-status")

    power_distributor = PowerDistributingActor(
        users_channels={
            key: channel.service_handle
            for key, channel in power_distributor_channels.items()
        },
        battery_status_sender=battery_status_channel.new_sender(),
    )

    # Channel to communicate between actors.
    power_dist_req_chan = Broadcast[List[float]](
        "power-distribing-req", resend_latest=True
    )

    # You should get components from ComponentGraph, not from the api.
    # It is faster and and non blocking approach.
    batteries: Set[Component] = connection_manager.get().component_graph.components(
        # component_type=set(ComponentType.BATTERY) in v0.8.0
        component_category={ComponentCategory.BATTERY}
    )

    service_actor = DecisionMakingActor(
        power_channel=power_dist_req_chan.new_receiver(),
        power_distributor_handle=power_distributor_channels[
            sending_actor_id
        ].client_handle,
        batteries={battery.component_id for battery in batteries},
    )

    client_actor = DataCollectingActor(
        request_channel=power_dist_req_chan.new_sender(),
        active_power_data=logical_meter.grid_power.new_receiver(),
    )

    await actor.run(service_actor, client_actor, power_distributor)


asyncio.run(run())
