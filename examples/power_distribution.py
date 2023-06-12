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
from typing import List, Optional

from frequenz.channels import Broadcast, Receiver, Sender

from frequenz.sdk import actor, microgrid
from frequenz.sdk.actor import ResamplerConfig
from frequenz.sdk.actor.power_distributing import Result, Success
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._quantities import Power

_logger = logging.getLogger(__name__)
HOST = "microgrid.sandbox.api.frequenz.io"  # it should be the host name.
PORT = 61060


@actor.actor
class DecisionMakingActor:
    """Actor that receives set receives power for given batteries."""

    def __init__(
        self,
        power_channel: Receiver[List[float]],
    ) -> None:
        """Create actor instance.

        Args:
            power_channel: channel where actor receives requests
        """
        self._power_channel = power_channel

    async def run(self) -> None:
        """Run actor.

        Raises:
            RuntimeError: If any channel was closed unexpectedly
        """
        battery_pool = microgrid.battery_pool()
        result_rx = battery_pool.power_distribution_results()

        while True:
            # wait for request with blocking
            request: Optional[List[float]] = await self._power_channel.receive()

            if request is None:
                raise RuntimeError("Request channel has been closed.")

            avg_power = sum(request) / len(request)
            _logger.debug("Avg power %d", avg_power)
            power_to_set: Power
            if avg_power > 30000:
                # Charge
                power_to_set = Power(10000.0)
            else:
                # Discharge
                power_to_set = Power(-10000.0)

            await battery_pool.set_power(power_to_set)
            try:
                result: Optional[Result] = await asyncio.wait_for(
                    result_rx.receive(),
                    timeout=3,
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
        active_power_data: Receiver[Sample[Power]],
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
                queue.put_nowait(
                    None if not active_power.value else active_power.value.base_value
                )

            await self._request_channel.send(list(queue.queue))


async def run() -> None:
    """Run main functions that initializes and creates everything."""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
    )
    await microgrid.initialize(
        HOST, PORT, ResamplerConfig(resampling_period=timedelta(seconds=1.0))
    )

    logical_meter = microgrid.logical_meter()
    # Channel to communicate between actors.
    power_dist_req_chan = Broadcast[List[float]](
        "power-distribing-req", resend_latest=True
    )

    service_actor = DecisionMakingActor(
        power_channel=power_dist_req_chan.new_receiver(),
    )

    client_actor = DataCollectingActor(
        request_channel=power_dist_req_chan.new_sender(),
        active_power_data=logical_meter.grid_power.new_receiver(),
    )

    await actor.run(service_actor, client_actor)


asyncio.run(run())
