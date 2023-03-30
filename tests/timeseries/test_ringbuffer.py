# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `OrderedRingbuffer` class."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from itertools import cycle, islice
from typing import Any

import numpy as np
import pytest

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._ringbuffer import Gap, OrderedRingBuffer

FIVE_MINUTES = timedelta(minutes=5)
ONE_MINUTE = timedelta(minutes=1)
ONE_SECOND = timedelta(seconds=1)
TWO_HUNDRED_MS = timedelta(milliseconds=200)


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer([0.0] * 1800, TWO_HUNDRED_MS),
        OrderedRingBuffer(
            np.empty(shape=(24 * 1800,), dtype=np.float64),
            TWO_HUNDRED_MS,
        ),
        OrderedRingBuffer(
            [0.0] * 1800, TWO_HUNDRED_MS, datetime(2000, 1, 1, tzinfo=timezone.utc)
        ),
    ],
)
def test_timestamp_ringbuffer(buffer: OrderedRingBuffer[Any]) -> None:
    """Test ordered ring buffer."""
    size = buffer.maxlen

    random.seed(0)

    resolution = buffer.sampling_period.total_seconds()

    # import pdb; pdb.set_trace()

    # Push in random order
    # for i in random.sample(range(size), size):
    for i in range(size):
        buffer.update(
            Sample(datetime.fromtimestamp(200 + i * resolution, tz=timezone.utc), i)
        )

    # Check all possible window sizes and start positions
    for i in range(0, size, 1000):
        for j in range(1, size - i, 987):
            assert i + j < size
            start = datetime.fromtimestamp(200 + i * resolution, tz=timezone.utc)
            end = datetime.fromtimestamp(200 + (j + i) * resolution, tz=timezone.utc)

            tmp = list(islice(cycle(range(0, size)), i, i + j))
            assert list(buffer.window(start, end)) == list(tmp)


@pytest.mark.parametrize(
    "buffer",
    [
        (OrderedRingBuffer([0.0] * 24, ONE_SECOND)),
        (OrderedRingBuffer(np.empty(shape=(24,), dtype=np.float64), ONE_SECOND)),
    ],
)
def test_timestamp_ringbuffer_overwrite(buffer: OrderedRingBuffer[Any]) -> None:
    """Test overwrite behavior and correctness."""
    size = buffer.maxlen

    random.seed(0)

    # Push in random order
    for i in random.sample(range(size), size):
        buffer.update(Sample(datetime.fromtimestamp(200 + i, tz=timezone.utc), i))

    # Push the same amount twice
    for i in random.sample(range(size), size):
        buffer.update(Sample(datetime.fromtimestamp(200 + i, tz=timezone.utc), i * 2))

    # Check all possible window sizes and start positions
    for i in range(size):
        for j in range(1, size - i):
            start = datetime.fromtimestamp(200 + i, tz=timezone.utc)
            end = datetime.fromtimestamp(200 + j + i, tz=timezone.utc)

            tmp = islice(cycle(range(0, size * 2, 2)), i, i + j)
            actual: float
            for actual, expectation in zip(buffer.window(start, end), tmp):
                assert actual == expectation

            assert j == len(buffer.window(start, end))


@pytest.mark.parametrize(
    "buffer",
    [
        (OrderedRingBuffer([0.0] * 24, ONE_SECOND)),
        (OrderedRingBuffer(np.empty(shape=(24,), dtype=np.float64), ONE_SECOND)),
    ],
)
def test_timestamp_ringbuffer_gaps(
    buffer: OrderedRingBuffer[Any],
) -> None:
    """Test force_copy command for window()."""
    size = buffer.maxlen
    random.seed(0)

    # Add initial data
    for i in random.sample(range(size), size):
        buffer.update(Sample(datetime.fromtimestamp(200 + i, tz=timezone.utc), i))

    # Request window of the data
    buffer.window(
        datetime.fromtimestamp(200, tz=timezone.utc),
        datetime.fromtimestamp(202, tz=timezone.utc),
    )

    # Add entry far in the future
    buffer.update(Sample(datetime.fromtimestamp(500 + size, tz=timezone.utc), 9999))

    # Expect exception for the same window
    with pytest.raises(IndexError):
        buffer.window(
            datetime.fromtimestamp(200, tz=timezone.utc),
            datetime.fromtimestamp(202, tz=timezone.utc),
        )

    # Receive new window without exception
    buffer.window(
        datetime.fromtimestamp(501, tz=timezone.utc),
        datetime.fromtimestamp(500 + size, tz=timezone.utc),
    )


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer([0.0] * 24 * int(FIVE_MINUTES.total_seconds()), FIVE_MINUTES),
        OrderedRingBuffer(
            np.empty(shape=(24 * int(FIVE_MINUTES.total_seconds())), dtype=np.float64),
            FIVE_MINUTES,
        ),
    ],
)
def test_timestamp_ringbuffer_missing_parameter(
    buffer: OrderedRingBuffer[Any],
) -> None:
    """Test ordered ring buffer."""
    buffer.update(Sample(datetime(2, 2, 2, 0, 0, tzinfo=timezone.utc), 0))

    # pylint: disable=protected-access
    assert buffer._normalize_timestamp(buffer.gaps[0].start) == buffer.gaps[0].start

    # Expecting one gap now, made of all the previous entries of the one just
    # added.
    assert len(buffer.gaps) == 1
    assert buffer.gaps[0].end == datetime(2, 2, 2, tzinfo=timezone.utc)

    # Add entry so that a second gap appears
    # pylint: disable=protected-access
    assert buffer._normalize_timestamp(
        datetime(2, 2, 2, 0, 7, 31, tzinfo=timezone.utc)
    ) == datetime(2, 2, 2, 0, 10, tzinfo=timezone.utc)
    buffer.update(Sample(datetime(2, 2, 2, 0, 7, 31, tzinfo=timezone.utc), 0))

    assert buffer.datetime_to_index(
        datetime(2, 2, 2, 0, 7, 31, tzinfo=timezone.utc)
    ) == buffer.datetime_to_index(datetime(2, 2, 2, 0, 10, tzinfo=timezone.utc))
    assert len(buffer.gaps) == 2

    # import pdb; pdb.set_trace()
    buffer.update(Sample(datetime(2, 2, 2, 0, 5, tzinfo=timezone.utc), 0))
    assert len(buffer.gaps) == 1


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer([0.0] * 24 * int(ONE_MINUTE.total_seconds()), ONE_MINUTE),
        OrderedRingBuffer(
            np.empty(shape=(24 * int(ONE_MINUTE.total_seconds()),), dtype=np.float64),
            ONE_MINUTE,
        ),
        OrderedRingBuffer([0.0] * 24 * int(FIVE_MINUTES.total_seconds()), FIVE_MINUTES),
        OrderedRingBuffer(
            np.empty(shape=(24 * int(FIVE_MINUTES.total_seconds())), dtype=np.float64),
            FIVE_MINUTES,
        ),
    ],
)
def test_timestamp_ringbuffer_missing_parameter_smoke(
    buffer: OrderedRingBuffer[Any],
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
                Sample(
                    datetime.fromtimestamp(
                        200 + j * buffer.sampling_period.total_seconds(),
                        tz=timezone.utc,
                    ),
                    None if missing else j,
                )
            )

        expected_gaps = list(
            map(
                lambda x: Gap(
                    # pylint: disable=protected-access
                    start=buffer._normalize_timestamp(
                        datetime.fromtimestamp(200 + x[0] * resolution, tz=timezone.utc)
                    ),
                    # pylint: disable=protected-access
                    end=buffer._normalize_timestamp(
                        datetime.fromtimestamp(200 + x[1] * resolution, tz=timezone.utc)
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


def test_len_ringbuffer_samples_fit_buffer_size() -> None:
    """Test the length of ordered ring buffer.

    The number of samples fits the ordered ring buffer size.
    """
    min_samples = 1
    max_samples = 100

    for num_samples in range(
        min_samples, max_samples + 1
    ):  # Include max_samples in the range
        test_samples = range(num_samples)

        buffer = OrderedRingBuffer(
            np.empty(shape=len(test_samples), dtype=float),
            sampling_period=timedelta(seconds=1),
            time_index_alignment=datetime(1, 1, 1, tzinfo=timezone.utc),
        )

        start_ts: datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        for index, sample_value in enumerate(test_samples):
            timestamp = start_ts + timedelta(seconds=index)
            buffer.update(Sample(timestamp, float(sample_value)))

        assert len(buffer) == len(test_samples)


def test_len_ringbuffer_samples_overwrite_buffer() -> None:
    """Test the length of ordered ring buffer.

    The number of samples overwrites the ordered ring buffer.
    """
    min_samples = 2
    max_samples = 100

    for num_samples in range(
        min_samples, max_samples + 1
    ):  # Include max_samples in the range
        test_samples = range(num_samples)
        half_buffer_size = len(test_samples) // 2

        buffer = OrderedRingBuffer(
            np.empty(shape=half_buffer_size, dtype=float),
            sampling_period=timedelta(seconds=1),
            time_index_alignment=datetime(1, 1, 1, tzinfo=timezone.utc),
        )

        start_ts: datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        for index, sample_value in enumerate(test_samples):
            timestamp = start_ts + timedelta(seconds=index)
            buffer.update(Sample(timestamp, float(sample_value)))

        assert len(buffer) == half_buffer_size


def test_ringbuffer_empty_buffer() -> None:
    """Test capacity ordered ring buffer."""
    empty_np_buffer = np.empty(shape=0, dtype=float)
    empty_list_buffer: list[float] = []

    assert len(empty_np_buffer) == len(empty_list_buffer) == 0

    with pytest.raises(AssertionError):
        OrderedRingBuffer(
            empty_np_buffer,
            sampling_period=timedelta(seconds=1),
            time_index_alignment=datetime(1, 1, 1),
        )
        OrderedRingBuffer(
            empty_list_buffer,
            sampling_period=timedelta(seconds=1),
            time_index_alignment=datetime(1, 1, 1),
        )
