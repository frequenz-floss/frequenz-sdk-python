# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the `Ringbuffer` class."""

from __future__ import annotations

import random
from datetime import datetime
from itertools import cycle, islice
from typing import TypeVar

import numpy as np
import pytest

from frequenz.sdk.util.ringbuffer import OrderedRingBuffer, RingBuffer

T = TypeVar("T")


@pytest.mark.parametrize(
    "buffer",
    [
        RingBuffer[int]([0] * 50000),
        RingBuffer[int](np.empty(shape=(50000,), dtype=np.float64)),
    ],
)
def test_simple_push_pop(buffer: RingBuffer[int]) -> None:
    """Test simple pushing/popping of RingBuffer."""
    for i in range(buffer.maxlen):
        buffer.push(i)

    assert len(buffer) == buffer.maxlen

    for i in range(buffer.maxlen):
        assert i == buffer.pop()


@pytest.mark.parametrize(
    "buffer",
    [
        RingBuffer[int]([0] * 50000),
        RingBuffer[int](np.empty(shape=(50000,), dtype=np.float64)),
    ],
)
def test_push_pop_over_limit(buffer: RingBuffer[int]) -> None:
    """Test pushing over the limit and the expected loss of data."""
    over_limit_pushes = 1000

    for i in range(buffer.maxlen + over_limit_pushes):
        buffer.push(i)

    assert len(buffer) == buffer.maxlen

    for i in range(buffer.maxlen):
        assert i + over_limit_pushes == buffer.pop()

    assert len(buffer) == 0


@pytest.mark.parametrize(
    "buffer, element_type",
    [
        (RingBuffer[int]([0] * 5000), int),
        (RingBuffer[float](np.empty(shape=(5000,), dtype=np.float64)), float),
    ],
)
def test_slicing(buffer: RingBuffer[T], element_type: type) -> None:
    """Test slicing method."""
    for i in range(buffer.maxlen):
        buffer.push(element_type(i))

    # Wrap in extra list() otherwise pytest complains about numpy arrays
    # pylint: disable=protected-access
    assert list(buffer._container) == list(range(buffer.maxlen))


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer[int]([0] * 24, 1),
        OrderedRingBuffer[float](np.empty(shape=(24,), dtype=np.float64), 1),
    ],
)
def test_timestamp_ringbuffer(buffer: OrderedRingBuffer[float | int]) -> None:
    """Test ordered ring buffer."""
    size = buffer.maxlen

    #    import pdb; pdb.set_trace()
    random.seed(0)

    # Push in random order
    for i in random.sample(range(size), size):
        buffer.update(datetime.fromtimestamp(200 + i), i)

    # Check all possible window sizes and start positions
    for i in range(size):
        for j in range(1, size):
            start = datetime.fromtimestamp(200 + i)
            end = datetime.fromtimestamp(200 + j + i)

            tmp = list(islice(cycle(range(0, size)), i, i + j))
            assert list(buffer.window(start, end)) == list(tmp)


@pytest.mark.parametrize(
    "buffer",
    [
        (OrderedRingBuffer[float]([0] * 24, 1)),
        (OrderedRingBuffer[float](np.empty(shape=(24,), dtype=np.float64), 1)),
    ],
)
def test_timestamp_ringbuffer_overwrite(buffer: OrderedRingBuffer[float | int]) -> None:
    """Test overwrite behavior and correctness."""
    size = buffer.maxlen

    #    import pdb; pdb.set_trace()
    random.seed(0)

    # Push in random order
    for i in random.sample(range(size), size):
        buffer.update(datetime.fromtimestamp(200 + i), i)

    # Push the same amount twice
    for i in random.sample(range(size), size):
        buffer.update(datetime.fromtimestamp(200 + i), i * 2)

    # Check all possible window sizes and start positions
    for i in range(size):
        for j in range(1, size):
            start = datetime.fromtimestamp(200 + i)
            end = datetime.fromtimestamp(200 + j + i)

            tmp = islice(cycle(range(0, size * 2, 2)), i, i + j)
            # assert list(buffer.window(start, end)) == list(tmp)
            actual: float
            for actual, expectation in zip(buffer.window(start, end), tmp):
                assert actual == expectation

            assert j == len(buffer.window(start, end))
