# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Script with an example how to use BatteryPool."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict

from frequenz.channels import Broadcast, Receiver
from frequenz.channels.util import MergeNamed

from frequenz.sdk import microgrid
from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    DataSourcingActor,
)
from frequenz.sdk.actor.power_distributing import PowerDistributingActor
from frequenz.sdk.actor.power_distributing._battery_pool_status import BatteryStatus
from frequenz.sdk.microgrid import connection_manager
from frequenz.sdk.microgrid.component import ComponentCategory
from frequenz.sdk.timeseries.battery_pool import BatteryPool

HOST = "microgrid.sandbox.api.frequenz.io"  # it should be the host name.
PORT = 61060


def create_battery_pool() -> BatteryPool:
    """Create battery pool.

    It needs many instance to be created before.

    Returns:
        BatteryPool instance ready to use.
    """
    channel_registry = ChannelRegistry(name="data-registry")

    # Create a channels for sending/receiving subscription requests
    data_source_request_channel = Broadcast[ComponentMetricRequest](
        "data-source", resend_latest=True
    )

    # Instantiate a data sourcing actor
    _ = DataSourcingActor(
        request_receiver=data_source_request_channel.new_receiver(
            "data_sourcing_receiver"
        ),
        registry=channel_registry,
    )

    battery_status_channel = Broadcast[BatteryStatus]("batteries-status")
    _ = PowerDistributingActor(
        users_channels={},
        battery_status_sender=battery_status_channel.new_sender(),
    )

    batteries = connection_manager.get().component_graph.components(
        component_category={ComponentCategory.BATTERY}
    )

    return BatteryPool(
        batteries_id=set(battery.component_id for battery in batteries),
        batteries_status_receiver=battery_status_channel.new_receiver(
            name="battery_pool", maxsize=1
        ),
        min_update_interval=timedelta(seconds=0.2),
    )


async def main() -> None:
    """Create the battery pool, activate all formulas and listen for any update."""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
    )
    await microgrid.initialize(host=HOST, port=PORT)

    battery_pool = create_battery_pool()
    receivers: Dict[str, Receiver[Any]] = {
        "soc": await battery_pool.soc(maxsize=1),
        "capacity": await battery_pool.capacity(maxsize=1),
        "power_bounds": await battery_pool.power_bounds(maxsize=1),
    }

    merged_channel = MergeNamed[Any](**receivers)
    async for metric_name, metric in merged_channel:
        print(f"Received new {metric_name}: {metric}")


asyncio.run(main())
