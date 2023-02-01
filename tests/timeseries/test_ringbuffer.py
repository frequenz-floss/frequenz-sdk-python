# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `OrderedRingbuffer` class."""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from itertools import cycle, islice
from typing import Any

import numpy as np
import pytest

from frequenz.sdk.timeseries._ringbuffer import Gap, OrderedRingBuffer

FIVE_MINUTES = timedelta(minutes=5)
ONE_MINUTE = timedelta(minutes=1)
ONE_SECOND = timedelta(seconds=1)
TWO_HUNDRED_MS = timedelta(milliseconds=200)


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer([0] * 1800, TWO_HUNDRED_MS),
        OrderedRingBuffer(
            np.empty(shape=(24 * 1800,), dtype=np.float64),
            TWO_HUNDRED_MS,
        ),
        OrderedRingBuffer([0] * 1800, TWO_HUNDRED_MS, datetime(2000, 1, 1)),
    ],
)
def test_timestamp_ringbuffer(buffer: OrderedRingBuffer[Any, Any]) -> None:
    """Test ordered ring buffer."""
    size = buffer.maxlen

    random.seed(0)

    resolution = buffer.sampling_period.total_seconds()

    # import pdb; pdb.set_trace()

    # Push in random order
    # for i in random.sample(range(size), size):
    for i in range(size):
        buffer.update(datetime.fromtimestamp(200 + i * resolution), i)

    # Check all possible window sizes and start positions
    for i in range(0, size, 1000):
        for j in range(1, size - i, 987):
            assert i + j < size
            start = datetime.fromtimestamp(200 + i * resolution)
            end = datetime.fromtimestamp(200 + (j + i) * resolution)

            tmp = list(islice(cycle(range(0, size)), i, i + j))
            assert list(buffer.window(start, end)) == list(tmp)


@pytest.mark.parametrize(
    "buffer",
    [
        (OrderedRingBuffer([0] * 24, ONE_SECOND)),
        (OrderedRingBuffer(np.empty(shape=(24,), dtype=np.float64), ONE_SECOND)),
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
        (OrderedRingBuffer([0] * 24, ONE_SECOND)),
        (OrderedRingBuffer(np.empty(shape=(24,), dtype=np.float64), ONE_SECOND)),
    ],
)
def test_timestamp_ringbuffer_gaps(
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


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer([0] * 24 * int(FIVE_MINUTES.total_seconds()), FIVE_MINUTES),
        OrderedRingBuffer(
            np.empty(shape=(24 * int(FIVE_MINUTES.total_seconds())), dtype=np.float64),
            FIVE_MINUTES,
        ),
    ],
)
def test_timestamp_ringbuffer_missing_parameter(
    buffer: OrderedRingBuffer[Any, Any],
) -> None:
    """Test ordered ring buffer."""
    buffer.update(datetime(2, 2, 2, 0, 0), 0)

    # pylint: disable=protected-access
    assert buffer._normalize_timestamp(buffer.gaps[0].start) == buffer.gaps[0].start

    # Expecting one gap now, made of all the previous entries of the one just
    # added.
    assert len(buffer.gaps) == 1
    assert buffer.gaps[0].end == datetime(2, 2, 2)

    # Add entry so that a second gap appears
    # pylint: disable=protected-access
    assert buffer._normalize_timestamp(datetime(2, 2, 2, 0, 7, 31)) == datetime(
        2, 2, 2, 0, 10
    )
    buffer.update(datetime(2, 2, 2, 0, 7, 31), 0)

    assert buffer.datetime_to_index(
        datetime(2, 2, 2, 0, 7, 31)
    ) == buffer.datetime_to_index(datetime(2, 2, 2, 0, 10))
    assert len(buffer.gaps) == 2

    # import pdb; pdb.set_trace()
    buffer.update(datetime(2, 2, 2, 0, 5), 0)
    assert len(buffer.gaps) == 1


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer([0] * 24 * int(ONE_MINUTE.total_seconds()), ONE_MINUTE),
        OrderedRingBuffer(
            np.empty(shape=(24 * int(ONE_MINUTE.total_seconds()),), dtype=np.float64),
            ONE_MINUTE,
        ),
        OrderedRingBuffer([0] * 24 * int(FIVE_MINUTES.total_seconds()), FIVE_MINUTES),
        OrderedRingBuffer(
            np.empty(shape=(24 * int(FIVE_MINUTES.total_seconds())), dtype=np.float64),
            FIVE_MINUTES,
        ),
    ],
)
def test_timestamp_ringbuffer_missing_parameter_smoke(
    buffer: OrderedRingBuffer[Any, Any]
) -> None:
    """Test ordered ring buffer."""
    size = buffer.maxlen
    resolution = int(buffer.sampling_period.total_seconds())

    random.seed(0)

    for _ in range(0, 10):
        expected_gaps_abstract = [(0.1, 0.2), (0.35, 0.4), (0.86, 1.0)]
        expected_gaps_concrete = list(
            map(lambda x: (int(size * x[0]), int(size * x[1])), expected_gaps_abstract)
        )

        # Push in random order
        # pylint: disable=cell-var-from-loop
        for j in random.sample(range(size), size):
            missing = any(map(lambda x: x[0] <= j < x[1], expected_gaps_concrete))
            buffer.update(
                datetime.fromtimestamp(
                    200 + j * buffer.sampling_period.total_seconds()
                ),
                math.nan if missing else j,
                missing,
            )

        expected_gaps = list(
            map(
                lambda x: Gap(
                    # pylint: disable=protected-access
                    start=buffer._normalize_timestamp(
                        datetime.fromtimestamp(200 + x[0] * resolution)
                    ),
                    # pylint: disable=protected-access
                    end=buffer._normalize_timestamp(
                        datetime.fromtimestamp(200 + x[1] * resolution)
                    ),
                ),
                expected_gaps_concrete,
            )
        )

        def window_to_timestamp(window: Any) -> Any:
            return window.start.timestamp()

        assert len(expected_gaps) == len(buffer.gaps)
        assert expected_gaps == sorted(
            buffer.gaps,
            key=window_to_timestamp,
        )
