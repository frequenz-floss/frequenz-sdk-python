# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Memory allocation benchmark for the ringbuffer."""

from __future__ import annotations

import argparse
import tracemalloc
from datetime import datetime, timedelta, timezone

import numpy as np

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._quantities import Quantity
from frequenz.sdk.timeseries._ringbuffer import OrderedRingBuffer

FIVE_MINUTES = timedelta(minutes=5)

RINGBUFFER_LENGTH = 4_000
# Number of iterations to run the benchmark
ITERATIONS = 100


def parse_args() -> tuple[int, int, int]:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--size",
        type=int,
        default=RINGBUFFER_LENGTH,
        help="Size of the ringbuffer to memory benchmark",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=ITERATIONS,
        help="Number of iterations to run the benchmark",
    )
    parser.add_argument(
        "--gap-size",
        type=int,
        default=0,
        help="Number of samples to skip between each update",
    )
    args = parser.parse_args()
    return args.size, args.iterations, args.gap_size


def main(ringbuffer_len: int, iterations: int, gap_size: int) -> None:
    """Run the benchmark.

    Args:
        ringbuffer_len: Size of the ringbuffer to dump/load
        iterations: Number of iterations to run the benchmark
        gap_size: Number of samples to skip between each update

    """
    # Trace memory allocations
    tracemalloc.start()

    ringbuffer = OrderedRingBuffer(
        np.arange(0, ringbuffer_len, dtype=np.float64), FIVE_MINUTES
    )

    # Snapshot memory allocations after ringbuffer creation
    snapshot_init = tracemalloc.take_snapshot()

    print(f"size: {ringbuffer_len}")
    print(f"iterations: {iterations}")
    print(f"gap_size: {gap_size}")

    for i in range(0, ringbuffer_len * iterations, gap_size + 1):
        ringbuffer.update(
            Sample(
                datetime.fromtimestamp(
                    200 + i * FIVE_MINUTES.total_seconds(), tz=timezone.utc
                ),
                Quantity(i),
            )
        )

    # Snapshot memory allocations after ringbuffer update
    snapshot_update = tracemalloc.take_snapshot()

    # Allocation diff between ringbuffer creation and update
    top_stats = snapshot_update.compare_to(snapshot_init, "lineno")

    # Print the top 10 memory allocations
    print("Allocations since ringbuffer creation:")
    print("[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)


if __name__ == "__main__":
    main(*parse_args())
