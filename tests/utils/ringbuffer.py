# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `OrderedRingbuffer` class."""

from __future__ import annotations

import random
from datetime import datetime
from itertools import cycle, islice
from typing import Any

import numpy as np
import pytest

from frequenz.sdk.util.ringbuffer import OrderedRingBuffer

RESOLUTION_IN_SECONDS = 300


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer([0] * 24 * RESOLUTION_IN_SECONDS, RESOLUTION_IN_SECONDS),
        OrderedRingBuffer(
            np.empty(shape=(24 * RESOLUTION_IN_SECONDS,), dtype=np.float64),
            RESOLUTION_IN_SECONDS,
        ),
    ],
)
def test_timestamp_ringbuffer(buffer: OrderedRingBuffer[Any, Any]) -> None:
    """Test ordered ring buffer."""
    size = buffer.maxlen

    random.seed(0)

    # import pdb; pdb.set_trace()

    # Push in random order
    for i in random.sample(range(size), size):
        assert buffer.update(datetime.fromtimestamp(200 + i * RESOLUTION_IN_SECONDS), i)

    # Check all possible window sizes and start positions
    for i in range(0, size, 100):
        for j in range(1, size - i, 100):
            assert i + j < size
            start = datetime.fromtimestamp(200 + i * RESOLUTION_IN_SECONDS)
            end = datetime.fromtimestamp(200 + (j + i) * RESOLUTION_IN_SECONDS)

            tmp = list(islice(cycle(range(0, size)), i, i + j))
            assert list(buffer.window(start, end)) == list(tmp)


@pytest.mark.parametrize(
    "buffer",
    [
        (OrderedRingBuffer([0] * 24, 1)),
        (OrderedRingBuffer(np.empty(shape=(24,), dtype=np.float64), 1)),
    ],
)
def test_timestamp_ringbuffer_overwrite(buffer: OrderedRingBuffer[Any, Any]) -> None:
    """Test overwrite behavior and correctness."""
    size = buffer.maxlen

    random.seed(0)

    # Push in random order
    for i in random.sample(range(size), size):
        buffer.update(datetime.fromtimestamp(200 + i), i)

    # Push the same amount twice
    for i in random.sample(range(size), size):
        buffer.update(datetime.fromtimestamp(200 + i), i * 2)

    # Check all possible window sizes and start positions
    for i in range(size):
        for j in range(1, size - i):
            start = datetime.fromtimestamp(200 + i)
            end = datetime.fromtimestamp(200 + j + i)

            tmp = islice(cycle(range(0, size * 2, 2)), i, i + j)
            actual: float
            for actual, expectation in zip(buffer.window(start, end), tmp):
                assert actual == expectation

            assert j == len(buffer.window(start, end))


@pytest.mark.parametrize(
    "buffer",
    [
        (OrderedRingBuffer([0] * 24, 1)),
        (OrderedRingBuffer(np.empty(shape=(24,), dtype=np.float64), 1)),
    ],
)
def test_timestamp_ringbuffer_missing_windows(
    buffer: OrderedRingBuffer[Any, Any],
) -> None:
    """Test force_copy command for window()."""
    size = buffer.maxlen
    random.seed(0)

    # Add initial data
    for i in random.sample(range(size), size):
        buffer.update(datetime.fromtimestamp(200 + i), i)

    # Request window of the data
    buffer.window(
        datetime.fromtimestamp(200),
        datetime.fromtimestamp(202),
    )

    # Add entry far in the future
    buffer.update(datetime.fromtimestamp(500 + size), 9999)

    # Expect exception for the same window
    with pytest.raises(IndexError):
        buffer.window(
            datetime.fromtimestamp(200),
            datetime.fromtimestamp(202),
        )

    # Receive new window without exception
    buffer.window(
        datetime.fromtimestamp(501),
        datetime.fromtimestamp(500 + size),
    )
