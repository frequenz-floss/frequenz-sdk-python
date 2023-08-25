# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Check how long it takes to distribute power."""

import asyncio
import csv
import random
import timeit
from datetime import timedelta
from typing import Any, Coroutine, Dict, List, Set  # pylint: disable=unused-import

from frequenz.channels import Broadcast

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ChannelRegistry, ResamplerConfig
from frequenz.sdk.actor.power_distributing import (
    BatteryStatus,
    Error,
    OutOfBounds,
    PartialFailure,
    PowerDistributingActor,
    Request,
    Result,
    Success,
)
from frequenz.sdk.microgrid import connection_manager
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.timeseries._quantities import Power

HOST = "microgrid.sandbox.api.frequenz.io"
PORT = 61060


async def send_requests(batteries: Set[int], request_num: int) -> List[Result]:
    """Send requests to the PowerDistributingActor and wait for the response.

    Args:
        user: user that should send request
        batteries: set of batteries where the power should be set
        request_num: number of requests that should be send

    Raises:
        SystemError: If the channel was closed.

    Returns:
        List of the results from the PowerDistributingActor.
    """
    battery_pool = microgrid.battery_pool(batteries)
    results_rx = battery_pool.power_distribution_results()
    result: List[Result] = []
    for _ in range(request_num):
        await battery_pool.set_power(Power(float(random.randrange(100000, 1000000))))
        try:
            output = await asyncio.wait_for(results_rx.receive(), timeout=3)
            if output is None:
                raise SystemError(f"Power response channel for {battery_pool} closed!")
            result.append(output)
        except asyncio.exceptions.TimeoutError:
            print("TIMEOUT ERROR")

    return result


def parse_result(result: List[List[Result]]) -> Dict[str, float]:
    """Parse result.

    Args:
        result: Results from finished tasks.

    Returns:
        Number of each result.
    """
    result_counts = {
        Error: 0,
        Success: 0,
        PartialFailure: 0,
        OutOfBounds: 0,
    }

    for result_list in result:
        for item in result_list:
            result_counts[type(item)] += 1

    return {
        "success_num": result_counts[Success],
        "failed_num": result_counts[PartialFailure],
        "error_num": result_counts[Error],
        "out_of_bounds": result_counts[OutOfBounds],
    }


async def run_test(  # pylint: disable=too-many-locals
    num_requests: int,
    batteries: Set[int],
) -> Dict[str, Any]:
    """Run test.

    Args:
        num_requests: Number of requests to send.
        batteries: Set of batteries for each request.

    Returns:
        Dictionary with statistics.
    """
    start = timeit.default_timer()

    power_request_channel = Broadcast[Request]("power-request")
    battery_status_channel = Broadcast[BatteryStatus]("battery-status")
    channel_registry = ChannelRegistry(name="power_distributor")
    async with PowerDistributingActor(
        channel_registry=channel_registry,
        requests_receiver=power_request_channel.new_receiver(),
        battery_status_sender=battery_status_channel.new_sender(),
    ):
        tasks: List[Coroutine[Any, Any, List[Result]]] = []
        tasks.append(send_requests(batteries, num_requests))

        result = await asyncio.gather(*tasks)
        exec_time = timeit.default_timer() - start

    summary = parse_result(result)
    summary["num_requests"] = num_requests
    summary["batteries_num"] = len(batteries)
    summary["exec_time"] = exec_time
    return summary


async def run() -> None:
    """Create microgrid api and run tests."""
    # pylint: disable=protected-access

    await microgrid.initialize(
        HOST, PORT, ResamplerConfig(resampling_period=timedelta(seconds=1.0))
    )

    all_batteries: Set[Component] = connection_manager.get().component_graph.components(
        component_category={ComponentCategory.BATTERY}
    )
    batteries_ids = {c.component_id for c in all_batteries}
    # Take some time to get data from components
    await asyncio.sleep(4)
    with open("/dev/stdout", "w", encoding="utf-8") as csvfile:
        fields = await run_test(0, batteries_ids)
        out = csv.DictWriter(csvfile, fields.keys())
        out.writeheader()
        out.writerow(await run_test(1, batteries_ids))
        out.writerow(await run_test(10, batteries_ids))


async def main() -> None:
    """Run the run() function."""
    await run()


asyncio.run(main())
