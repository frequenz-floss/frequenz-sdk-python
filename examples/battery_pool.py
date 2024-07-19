# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Script with an example how to use BatteryPool."""


import asyncio
import logging
from datetime import timedelta

from frequenz.channels import merge

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig

MICROGRID_API_URL = "grpc://microgrid.sandbox.api.frequenz.io:62060"


async def main() -> None:
    """Create the battery pool, activate all formulas and listen for any update."""
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
    )
    # Reduce undesired noise from hpack
    logging.getLogger("hpack.hpack").setLevel(logging.INFO)

    await microgrid.initialize(
        MICROGRID_API_URL,
        resampler_config=ResamplerConfig(resampling_period=timedelta(seconds=1.0)),
    )

    battery_pool = microgrid.new_battery_pool(priority=5)
    receivers = [
        battery_pool.soc.new_receiver(limit=1),
        battery_pool.capacity.new_receiver(limit=1),
        # pylint: disable=protected-access
        battery_pool._system_power_bounds.new_receiver(limit=1),
        # pylint: enable=protected-access
    ]

    async for metric in merge(*receivers):
        print(f"Received new metric: {metric}")


asyncio.run(main())
