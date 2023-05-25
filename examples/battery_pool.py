# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Script with an example how to use BatteryPool."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict

from frequenz.channels import Receiver
from frequenz.channels.util import MergeNamed

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig

HOST = "microgrid.sandbox.api.frequenz.io"  # it should be the host name.
PORT = 61060


async def main() -> None:
    """Create the battery pool, activate all formulas and listen for any update."""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
    )
    await microgrid.initialize(
        host=HOST,
        port=PORT,
        resampler_config=ResamplerConfig(resampling_period=timedelta(seconds=1.0)),
    )

    battery_pool = microgrid.battery_pool()
    receivers: Dict[str, Receiver[Any]] = {
        "soc": battery_pool.soc.new_receiver(maxsize=1),
        "capacity": battery_pool.capacity.new_receiver(maxsize=1),
        "power_bounds": battery_pool.power_bounds.new_receiver(maxsize=1),
    }

    merged_channel = MergeNamed[Any](**receivers)
    async for metric_name, metric in merged_channel:
        print(f"Received new {metric_name}: {metric}")


asyncio.run(main())
