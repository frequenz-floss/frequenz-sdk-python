# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Check how long it takes to distribute power."""

import asyncio
import csv
import random
import timeit
from dataclasses import dataclass
from typing import Any, Coroutine, Dict, List, Set  # pylint: disable=unused-import

from frequenz.channels import Bidirectional, Broadcast

from frequenz.sdk import microgrid
from frequenz.sdk.actor.power_distributing import (
    BatteryStatus,
    Error,
    Ignored,
    OutOfBound,
    PartialFailure,
    PowerDistributingActor,
    Request,
    Result,
    Success,
)
from frequenz.sdk.microgrid.component import Component, ComponentCategory

HOST = "microgrid.sandbox.api.frequenz.io"
PORT = 61060


@dataclass
class User:
    """User definition."""

    user_id: str
    channel: Bidirectional.Handle[Request, Result]


async def run_user(user: User, batteries: Set[int], request_num: int) -> List[Result]:
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
    result: List[Result] = []
    for _ in range(request_num):
        await user.channel.send(
            Request(power=random.randrange(100000, 1000000), batteries=batteries)
        )
        try:
            output = await asyncio.wait_for(user.channel.receive(), timeout=3)
            if output is None:
                raise SystemError(f"Channel for {user.user_id} closed!")
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
        Ignored: 0,
        Success: 0,
        PartialFailure: 0,
        OutOfBound: 0,
    }

    for result_list in result:
        for item in result_list:
            result_counts[type(item)] += 1

    return {
        "success_num": result_counts[Success],
        "failed_num": result_counts[PartialFailure],
        "ignore_num": result_counts[Ignored],
        "error_num": result_counts[Error],
        "out_of_bound": result_counts[OutOfBound],
    }


async def run_test(  # pylint: disable=too-many-locals
    users_num: int,
    requests_per_user: int,
    batteries: Set[int],
) -> Dict[str, Any]:
    """Run test.

    Args:
        users_num: Number of users to register
        requests_per_user: How many request user should send.
        batteries: Set of batteries for each request.

    Returns:
        Dictionary with statistics.
    """
    start = timeit.default_timer()

    channels: Dict[str, Bidirectional[Request, Result]] = {
        str(user_id): Bidirectional[Request, Result](str(user_id), "power_distributor")
        for user_id in range(users_num)
    }

    service_channels = {
        user_id: channel.service_handle for user_id, channel in channels.items()
    }

    battery_status_channel = Broadcast[BatteryStatus]("battery-status")

    distributor = PowerDistributingActor(
        service_channels, battery_status_sender=battery_status_channel.new_sender()
    )

    tasks: List[Coroutine[Any, Any, List[Result]]] = []
    for user_id, channel in channels.items():
        user = User(user_id, channel.client_handle)
        tasks.append(run_user(user, batteries, requests_per_user))

    result = await asyncio.gather(*tasks)
    exec_time = timeit.default_timer() - start

    await distributor._stop()  # type: ignore # pylint: disable=no-member, protected-access

    summary = parse_result(result)
    summary["users_num"] = users_num
    summary["requests_per_user"] = requests_per_user
    summary["batteries_num"] = len(batteries)
    summary["exec_time"] = exec_time
    return summary


async def run() -> None:
    """Create microgrid api and run tests."""
    # pylint: disable=protected-access

    await microgrid.initialize(HOST, PORT)

    all_batteries: Set[Component] = microgrid.get().component_graph.components(
        component_category={ComponentCategory.BATTERY}
    )
    batteries_ids = {c.component_id for c in all_batteries}
    # Take some time to get data from components
    await asyncio.sleep(4)
    with open("/dev/stdout", "w", encoding="utf-8") as csvfile:
        fields = await run_test(0, 0, batteries_ids)
        out = csv.DictWriter(csvfile, fields.keys())
        out.writeheader()
        out.writerow(await run_test(1, 1, batteries_ids))
        out.writerow(await run_test(1, 10, batteries_ids))
        out.writerow(await run_test(10, 10, batteries_ids))


async def main() -> None:
    """Run the run() function."""
    await run()


asyncio.run(main())
