# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Example how to run BatteryPoolStatus as separate instance.

This is not needed for user but simplifies testing and debugging and understanding
this feature.
"""

import asyncio
import logging

from frequenz.channels import Broadcast

from frequenz.sdk import microgrid
from frequenz.sdk.actor.power_distributing._battery_pool_status import (
    BatteryPoolStatus,
    BatteryStatus,
)
from frequenz.sdk.microgrid.component import ComponentCategory

_logger = logging.getLogger(__name__)
HOST = "microgrid.sandbox.api.frequenz.io"  # it should be the host name.
PORT = 61060


async def main() -> None:
    """Start BatteryPoolStatus to see how it works."""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
    )
    await microgrid.initialize(HOST, PORT)
    batteries = {
        bat.component_id
        for bat in microgrid.get().component_graph.components(
            component_category={ComponentCategory.BATTERY}
        )
    }
    battery_status_channel = Broadcast[BatteryStatus]("battery-status-channel")

    batteries_status = BatteryPoolStatus(
        battery_status_sender=battery_status_channel.new_sender(),
        battery_ids=batteries,
        max_data_age_sec=5,
        max_blocking_duration_sec=30,
    )

    battery_status_receiver = battery_status_channel.new_receiver()
    async for status in battery_status_receiver:
        print(f"Received new battery status {status}")

    await batteries_status.join()


asyncio.run(main())
