# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Benchmarks the serialization of the `OrderedRingBuffer` class."""

from __future__ import annotations

import fnmatch
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np

import frequenz.sdk.timeseries._ringbuffer as rb
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._quantities import Quantity

FILE_NAME = "ringbuffer.pkl"
FIVE_MINUTES = timedelta(minutes=5)

# Size of the ringbuffer to dump/load
SIZE = 4000_000
# Number of iterations to run the benchmark
ITERATIONS = 100


def delete_files_with_prefix(prefix: str) -> None:
    """Delete all files starting with the given prefix.

    Args:
        prefix: Prefix of the files to delete
    """
    for file in os.listdir():
        if fnmatch.fnmatch(file, prefix + "*"):
            os.remove(file)


def benchmark_serialization(
    ringbuffer: rb.OrderedRingBuffer[Any], iterations: int
) -> float:
    """Benchmark the given buffer `iteration` times.

    Args:
        ringbuffer: Ringbuffer to benchmark to serialize.
        iterations: amount of iterations to run.
    """
    total = 0.0
    for _ in range(iterations):
        start = time.time()
        rb.dump(ringbuffer, FILE_NAME)
        rb.load(FILE_NAME)
        end = time.time()
        total += end - start
        delete_files_with_prefix(FILE_NAME)

    return total / iterations


def main() -> None:
    """Run Benchmark."""
    ringbuffer = rb.OrderedRingBuffer(
        np.arange(0, SIZE, dtype=np.float64), timedelta(minutes=5)
    )

    print("size:", SIZE)
    print("iterations:", ITERATIONS)

    for i in range(0, SIZE, 10000):
        ringbuffer.update(
            Sample(
                datetime.fromtimestamp(
                    200 + i * FIVE_MINUTES.total_seconds(), tz=timezone.utc
                ),
                Quantity(i),
            )
        )

    print(
        "Avg time for Pickle dump/load:  "
        f"{benchmark_serialization(ringbuffer, ITERATIONS)}s"
    )


if __name__ == "__main__":
    main()
