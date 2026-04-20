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
from typing import Any

from frequenz.channels import (
    BroadcastChannel,
    BroadcastReceiver,
    OneshotChannel,
    Receiver,
    ReceiverStoppedError,
)
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Quantity

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid._data_sourcing import (
    ComponentMetricRequest,
    DataSourcingActor,
)
from frequenz.sdk.timeseries import Sample

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
    Metric.AC_CURRENT_PHASE_1,
    Metric.AC_CURRENT_PHASE_2,
    Metric.AC_CURRENT_PHASE_3,
]


def enable_mock_client(client: MockMicrogridClient) -> None:
    """Enable the mock microgrid client.

    Args:
        client: the mock microgrid client to enable.
    """
    microgrid.connection_manager._CONNECTION_MANAGER = (  # pylint: disable=protected-access
        client.mock_microgrid
    )


async def benchmark_data_sourcing(  # pylint: disable=too-many-locals
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

    request_sender, request_receiver = BroadcastChannel[ComponentMetricRequest](
        name="DataSourcingActor Request Channel",
        limit=(num_ev_chargers * len(COMPONENT_METRIC_IDS)),
    )

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
            telem_stream_sender, telem_stream_receiver = OneshotChannel[
                BroadcastReceiver[Sample[Quantity]]
            ]()
            request = ComponentMetricRequest(
                namespace="current_phase_requests",
                component_id=evc_id,
                metric=component_metric_id,
                start_time=None,
                telem_stream_sender=telem_stream_sender,
            )
            await request_sender.send(request)
            stream_receiver = await telem_stream_receiver.receive()
            consume_tasks.append(asyncio.create_task(consume(stream_receiver)))

    async with DataSourcingActor(request_receiver):
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


def parse_args() -> tuple[int, int, bool]:
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
