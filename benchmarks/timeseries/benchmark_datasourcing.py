# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Benchmark the data sourcing actor.

To be able to access the `tests` package we need to adjust the PYTHONPATH.

Usage:

    PYTHONPATH=. python benchmark_datasourcing.py <num ev chargers> <num messages per battery>
"""
import argparse
import asyncio
import sys
import tracemalloc
from time import perf_counter
from typing import Any, Tuple

from frequenz.channels import Broadcast, Receiver, ReceiverStoppedError

from frequenz.sdk import microgrid
from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    DataSourcingActor,
)
from frequenz.sdk.microgrid.component import ComponentMetricId

try:
    from tests.timeseries.mock_microgrid import MockMicrogrid
    from tests.utils import MockMicrogridClient
except ImportError:
    print(
        "Error: Unable to import the `tests` package. "
        "Please make sure that the PYTHONPATH env variable is set to the root of the repository."
    )
    sys.exit(1)

COMPONENT_METRIC_IDS = [
    ComponentMetricId.CURRENT_PHASE_1,
    ComponentMetricId.CURRENT_PHASE_2,
    ComponentMetricId.CURRENT_PHASE_3,
]


def enable_mock_client(client: MockMicrogridClient) -> None:
    """Enable the mock microgrid client.

    Args:
        client: the mock microgrid client to enable.
    """
    # pylint: disable=protected-access
    microgrid.connection_manager._CONNECTION_MANAGER = client.mock_microgrid


# pylint: disable=too-many-locals
async def benchmark_data_sourcing(
    num_ev_chargers: int, num_msgs_per_battery: int
) -> None:
    """Benchmark the data sourcing actor.

    Benchmark the data sourcing actor by sending out a number of requests and
    printing out the number of samples sent and the time taken.

    Args:
        num_ev_chargers: number of EV Chargers to create for the mock microgrid.
        num_msgs_per_battery: number of messages to send out for each battery.
    """
    num_expected_messages = (
        num_ev_chargers * len(COMPONENT_METRIC_IDS) * num_msgs_per_battery
    )
    mock_grid = MockMicrogrid(
        grid_meter=False, num_values=num_msgs_per_battery, sample_rate_s=0.0
    )

    mock_grid.add_ev_chargers(num_ev_chargers)
    mock_grid.start_mock_client(enable_mock_client)

    request_channel = Broadcast[ComponentMetricRequest](
        "DataSourcingActor Request Channel"
    )

    channel_registry = ChannelRegistry(name="Microgrid Channel Registry")
    request_receiver = request_channel.new_receiver(
        "datasourcing-benchmark", maxsize=(num_ev_chargers * len(COMPONENT_METRIC_IDS))
    )
    request_sender = request_channel.new_sender()

    consume_tasks = []

    start_time = perf_counter()
    samples_sent = 0

    async def consume(channel: Receiver[Any]) -> None:
        while True:
            try:
                await channel.ready()
                channel.consume()
            except ReceiverStoppedError:
                return

            nonlocal samples_sent
            samples_sent += 1

    for evc_id in mock_grid.evc_ids:
        for component_metric_id in COMPONENT_METRIC_IDS:
            request = ComponentMetricRequest(
                "current_phase_requests", evc_id, component_metric_id, None
            )

            recv_channel = channel_registry.new_receiver(request.get_channel_name())

            await request_sender.send(request)
            consume_tasks.append(asyncio.create_task(consume(recv_channel)))

    async with DataSourcingActor(request_receiver, channel_registry):
        await asyncio.gather(*consume_tasks)

        time_taken = perf_counter() - start_time

        await mock_grid.cleanup()

        print(f"Samples Sent: {samples_sent}, time taken: {time_taken}")
        print(f"Samples per second: {samples_sent / time_taken}")
        print(
            "Expected samples: "
            f"{num_expected_messages}, missing: {num_expected_messages - samples_sent}"
        )
        print(
            f"Missing per EVC: {(num_expected_messages - samples_sent) / num_ev_chargers}"
        )


def parse_args() -> Tuple[int, int, bool]:
    """Parse the command line arguments.

    Returns:
        A tuple of (num ev chargers, num messages per battery, record allocations).
    """
    parser = argparse.ArgumentParser(description="Benchmark the data sourcing actor.")
    parser.add_argument(
        "num_ev_chargers",
        type=int,
        help="Number of EV Chargers to create for the mock microgrid.",
    )
    parser.add_argument(
        "num_msgs_per_battery",
        type=int,
        help="Number of messages to send out for each battery.",
    )
    parser.add_argument(
        "--record-allocations",
        action="store_true",
        help="Record memory allocations.",
    )

    args = parser.parse_args()

    return args.num_ev_chargers, args.num_msgs_per_battery, args.record_allocations


def main() -> None:
    """Start everything."""
    (
        num_ev_chargers_pararm,
        num_msgs_per_battery_param,
        record_allocations,
    ) = parse_args()

    if record_allocations:
        tracemalloc.start()

    asyncio.run(
        benchmark_data_sourcing(num_ev_chargers_pararm, num_msgs_per_battery_param)
    )

    if not record_allocations:
        sys.exit(0)

    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics("lineno")

    print("\n[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)


if __name__ == "__main__":
    main()
