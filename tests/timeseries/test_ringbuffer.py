# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `OrderedRingbuffer` class."""


import random
from datetime import datetime, timedelta, timezone
from itertools import cycle, islice
from typing import Any

import numpy as np
import pytest

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._quantities import Quantity
from frequenz.sdk.timeseries._ringbuffer import Gap, OrderedRingBuffer
from frequenz.sdk.timeseries._ringbuffer.buffer import FloatArray

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
            Sample(
                datetime.fromtimestamp(200 + i * resolution, tz=timezone.utc),
                Quantity(i),
            )
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
        buffer.update(
            Sample(datetime.fromtimestamp(200 + i, tz=timezone.utc), Quantity(i))
        )

    # Push the same amount twice
    for i in random.sample(range(size), size):
        buffer.update(
            Sample(datetime.fromtimestamp(200 + i, tz=timezone.utc), Quantity(i * 2))
        )

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
        buffer.update(
            Sample(datetime.fromtimestamp(200 + i, tz=timezone.utc), Quantity(i))
        )

    # Request window of the data
    buffer.window(
        datetime.fromtimestamp(200, tz=timezone.utc),
        datetime.fromtimestamp(202, tz=timezone.utc),
    )

    # Add entry far in the future
    buffer.update(
        Sample(datetime.fromtimestamp(500 + size, tz=timezone.utc), Quantity(9999))
    )

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
    buffer.update(Sample(datetime(2, 2, 2, 0, 0, tzinfo=timezone.utc), Quantity(0)))

    # pylint: disable=protected-access
    assert buffer.normalize_timestamp(buffer.gaps[0].start) == buffer.gaps[0].start

    # Expecting one gap now, made of all the previous entries of the one just
    # added.
    assert len(buffer.gaps) == 1
    assert buffer.gaps[0].end == datetime(2, 2, 2, tzinfo=timezone.utc)

    # Add entry so that a second gap appears
    # pylint: disable=protected-access
    assert buffer.normalize_timestamp(
        datetime(2, 2, 2, 0, 7, 31, tzinfo=timezone.utc)
    ) == datetime(2, 2, 2, 0, 10, tzinfo=timezone.utc)
    buffer.update(Sample(datetime(2, 2, 2, 0, 7, 31, tzinfo=timezone.utc), Quantity(0)))

    assert buffer.to_internal_index(
        datetime(2, 2, 2, 0, 7, 31, tzinfo=timezone.utc)
    ) == buffer.to_internal_index(datetime(2, 2, 2, 0, 10, tzinfo=timezone.utc))
    assert len(buffer.gaps) == 2

    # import pdb; pdb.set_trace()
    buffer.update(Sample(datetime(2, 2, 2, 0, 5, tzinfo=timezone.utc), Quantity(0)))
    assert len(buffer.gaps) == 1


def dt(i: int) -> datetime:  # pylint: disable=invalid-name
    """Create datetime objects from indices.

    Args:
        i: Index to create datetime from.

    Returns:
        Datetime object.
    """
    return datetime.fromtimestamp(i, tz=timezone.utc)


def test_gaps() -> None:  # pylint: disable=too-many-statements
    """Test gap treatment in ordered ring buffer."""
    buffer = OrderedRingBuffer([0.0] * 5, ONE_SECOND)
    assert buffer.oldest_timestamp is None
    assert buffer.newest_timestamp is None
    assert buffer.count_valid() == 0
    assert buffer.count_covered() == 0
    assert len(buffer.gaps) == 0

    buffer.update(Sample(dt(0), Quantity(0)))
    assert buffer.oldest_timestamp == dt(0)
    assert buffer.newest_timestamp == dt(0)
    assert buffer.count_valid() == 1
    assert buffer.count_covered() == 1
    assert len(buffer.gaps) == 1

    buffer.update(Sample(dt(6), Quantity(0)))
    assert buffer.oldest_timestamp == dt(6)
    assert buffer.newest_timestamp == dt(6)
    assert buffer.count_valid() == 1
    assert buffer.count_covered() == 1
    assert len(buffer.gaps) == 1

    buffer.update(Sample(dt(2), Quantity(2)))
    buffer.update(Sample(dt(3), Quantity(3)))
    buffer.update(Sample(dt(4), Quantity(4)))
    assert buffer.oldest_timestamp == dt(2)
    assert buffer.newest_timestamp == dt(6)
    assert buffer.count_valid() == 4
    assert buffer.count_covered() == 5
    assert len(buffer.gaps) == 1

    buffer.update(Sample(dt(3), None))
    assert buffer.oldest_timestamp == dt(2)
    assert buffer.newest_timestamp == dt(6)
    assert buffer.count_valid() == 3
    assert buffer.count_covered() == 5
    assert len(buffer.gaps) == 2

    buffer.update(Sample(dt(3), Quantity(np.nan)))
    assert buffer.oldest_timestamp == dt(2)
    assert buffer.newest_timestamp == dt(6)
    assert buffer.count_valid() == 3
    assert buffer.count_covered() == 5
    assert len(buffer.gaps) == 2

    buffer.update(Sample(dt(2), Quantity(np.nan)))
    assert buffer.oldest_timestamp == dt(4)
    assert buffer.newest_timestamp == dt(6)
    assert buffer.count_valid() == 2
    assert buffer.count_covered() == 3
    assert len(buffer.gaps) == 2

    buffer.update(Sample(dt(3), Quantity(3)))
    assert buffer.oldest_timestamp == dt(3)
    assert buffer.newest_timestamp == dt(6)
    assert buffer.count_valid() == 3
    assert buffer.count_covered() == 4
    assert len(buffer.gaps) == 2

    buffer.update(Sample(dt(2), Quantity(2)))
    assert buffer.oldest_timestamp == dt(2)
    assert buffer.newest_timestamp == dt(6)
    assert buffer.count_valid() == 4
    assert buffer.count_covered() == 5
    assert len(buffer.gaps) == 1

    buffer.update(Sample(dt(5), Quantity(5)))
    assert buffer.oldest_timestamp == dt(2)
    assert buffer.newest_timestamp == dt(6)
    assert buffer.count_valid() == 5
    assert buffer.count_covered() == 5
    assert len(buffer.gaps) == 0

    # whole range gap suffers from sdk#646
    buffer.update(Sample(dt(99), None))
    assert buffer.oldest_timestamp == dt(95)  # bug: should be None
    assert buffer.newest_timestamp == dt(99)  # bug: should be None
    assert buffer.count_valid() == 4  # bug: should be 0 (whole range gap)
    assert buffer.count_covered() == 5  # bug: should be 0
    assert len(buffer.gaps) == 1


@pytest.mark.parametrize(
    "buffer",
    [
        OrderedRingBuffer([0.0] * 10 * int(ONE_MINUTE.total_seconds()), ONE_MINUTE),
        OrderedRingBuffer(
            np.empty(shape=(12 * int(ONE_MINUTE.total_seconds()),), dtype=np.float64),
            ONE_MINUTE,
        ),
        OrderedRingBuffer([0.0] * 5 * int(FIVE_MINUTES.total_seconds()), FIVE_MINUTES),
        OrderedRingBuffer(
            np.empty(shape=(5 * int(FIVE_MINUTES.total_seconds())), dtype=np.float64),
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
                    None if missing else Quantity(j),
                )
            )

        expected_gaps = list(
            map(
                lambda x: Gap(
                    # pylint: disable=protected-access
                    start=buffer.normalize_timestamp(
                        datetime.fromtimestamp(200 + x[0] * resolution, tz=timezone.utc)
                    ),
                    # pylint: disable=protected-access
                    end=buffer.normalize_timestamp(
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
            align_to=datetime(1, 1, 1, tzinfo=timezone.utc),
        )

        start_ts: datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        for index, sample_value in enumerate(test_samples):
            timestamp = start_ts + timedelta(seconds=index)
            buffer.update(Sample(timestamp, Quantity(float(sample_value))))

        assert buffer.count_valid() == len(test_samples)


def test_len_with_gaps() -> None:
    """Test the length when there are gaps in the buffer."""
    buffer = OrderedRingBuffer(
        np.empty(shape=10, dtype=float),
        sampling_period=timedelta(seconds=1),
        align_to=datetime(1, 1, 1, tzinfo=timezone.utc),
    )

    for i in range(10):
        buffer.update(
            Sample(datetime(2, 2, 2, 0, 0, i, tzinfo=timezone.utc), Quantity(float(i)))
        )
        assert buffer.count_valid() == i + 1


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
            align_to=datetime(1, 1, 1, tzinfo=timezone.utc),
        )

        start_ts: datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        for index, sample_value in enumerate(test_samples):
            timestamp = start_ts + timedelta(seconds=index)
            buffer.update(Sample(timestamp, Quantity(float(sample_value))))

        assert buffer.count_valid() == half_buffer_size


def test_ringbuffer_empty_buffer() -> None:
    """Test capacity ordered ring buffer."""
    empty_np_buffer = np.empty(shape=0, dtype=float)
    empty_list_buffer: list[float] = []

    assert len(empty_np_buffer) == len(empty_list_buffer) == 0

    with pytest.raises(AssertionError):
        OrderedRingBuffer(
            empty_np_buffer,
            sampling_period=timedelta(seconds=1),
            align_to=datetime(1, 1, 1),
        )
        OrderedRingBuffer(
            empty_list_buffer,
            sampling_period=timedelta(seconds=1),
            align_to=datetime(1, 1, 1),
        )


def test_off_by_one_gap_logic_bug() -> None:
    """Test off by one bug in the gap calculation."""
    buffer = OrderedRingBuffer(
        np.empty(shape=2, dtype=float),
        sampling_period=timedelta(seconds=1),
        align_to=datetime(1, 1, 1, tzinfo=timezone.utc),
    )

    base_time = datetime(2023, 1, 1, tzinfo=timezone.utc)

    times = [base_time, base_time + timedelta(seconds=1)]

    buffer.update(Sample(times[0], Quantity(1.0)))
    buffer.update(Sample(times[1], Quantity(2.0)))

    assert buffer.is_missing(times[0]) is False
    assert buffer.is_missing(times[1]) is False


def test_cleanup_oldest_gap_timestamp() -> None:
    """Test that gaps are updated such that they are fully contained in the buffer."""
    buffer = OrderedRingBuffer(
        np.empty(shape=15, dtype=float),
        sampling_period=timedelta(seconds=1),
        align_to=datetime(1, 1, 1, tzinfo=timezone.utc),
    )

    for i in range(10):
        buffer.update(
            Sample(datetime.fromtimestamp(200 + i, tz=timezone.utc), Quantity(i))
        )

    gap = Gap(
        datetime.fromtimestamp(195, tz=timezone.utc),
        datetime.fromtimestamp(200, tz=timezone.utc),
    )

    assert gap == buffer.gaps[0]


def test_delete_oudated_gap() -> None:
    """Test updating the buffer such that the gap is no longer valid.

    We introduce two gaps and check that the oldest is removed.
    """
    buffer = OrderedRingBuffer(
        np.empty(shape=3, dtype=float),
        sampling_period=timedelta(seconds=1),
        align_to=datetime(1, 1, 1, tzinfo=timezone.utc),
    )

    for i in range(2):
        buffer.update(
            Sample(datetime.fromtimestamp(200 + i, tz=timezone.utc), Quantity(i))
        )
    assert len(buffer.gaps) == 1

    buffer.update(Sample(datetime.fromtimestamp(202, tz=timezone.utc), Quantity(2)))

    assert len(buffer.gaps) == 0


def get_orb(data: FloatArray) -> OrderedRingBuffer[FloatArray]:
    """Get OrderedRingBuffer with data.

    Args:
        data: Data to fill the buffer with.

    Returns:
        OrderedRingBuffer with data.
    """
    buffer = OrderedRingBuffer(data, ONE_SECOND)
    for i, d in enumerate(data):  # pylint: disable=invalid-name
        buffer.update(Sample(dt(i), Quantity(d) if d is not None else None))
    return buffer


def test_window() -> None:
    """Test the window function."""
    buffer = get_orb(np.array([0, None, 2, 3, 4]))
    win = buffer.window(dt(0), dt(3), force_copy=False)
    assert [0, np.nan, 2] == list(win)
    buffer._buffer[1] = 1  # pylint: disable=protected-access
    # Test whether the window is a view or a copy
    assert [0, 1, 2] == list(win)
    win = buffer.window(dt(0), dt(3), force_copy=False)
    assert [0, 1, 2] == list(win)
    # Empty array
    assert 0 == buffer.window(dt(1), dt(1)).size

    buffer = get_orb([0.0, 1.0, 2.0, 3.0, 4.0])  # type: ignore
    assert [0, 1, 2] == buffer.window(dt(0), dt(3))
    assert [] == buffer.window(dt(0), dt(0))
    assert [] == buffer.window(dt(1), dt(1))


def test_wrapped_buffer_window() -> None:
    """Test the wrapped buffer window function."""
    wbw = OrderedRingBuffer._wrapped_buffer_window  # pylint: disable=protected-access

    #
    # Tests for list buffer
    #
    buffer = [0.0, 1.0, 2.0, 3.0, 4.0]
    # start = end
    assert [0, 1, 2, 3, 4] == wbw(buffer, 0, 0, force_copy=False)
    assert [4, 0, 1, 2, 3] == wbw(buffer, 4, 4, force_copy=False)
    # start < end
    assert [0] == wbw(buffer, 0, 1, force_copy=False)
    assert [0, 1, 2, 3, 4] == wbw(buffer, 0, 5, force_copy=False)
    # start > end, end = 0
    assert [4] == wbw(buffer, 4, 0, force_copy=False)
    # start > end, end > 0
    assert [4, 0, 1] == wbw(buffer, 4, 2, force_copy=False)

    # Lists are always shallow copies
    res_copy = wbw(buffer, 0, 5, force_copy=False)
    assert [0, 1, 2, 3, 4] == res_copy
    buffer[0] = 9
    assert [0, 1, 2, 3, 4] == res_copy

    #
    # Tests for array buffer
    #
    buffer = np.array([0, 1, 2, 3, 4])  # type: ignore
    # start = end
    assert [0, 1, 2, 3, 4] == list(wbw(buffer, 0, 0, force_copy=False))
    assert [4, 0, 1, 2, 3] == list(wbw(buffer, 4, 4, force_copy=False))
    # start < end
    assert [0] == list(wbw(buffer, 0, 1, force_copy=False))
    assert [0, 1, 2, 3, 4] == list(wbw(buffer, 0, 5, force_copy=False))
    # start > end, end = 0
    assert [4] == list(wbw(buffer, 4, 0, force_copy=False))
    # start > end, end > 0
    assert [4, 0, 1] == list(wbw(buffer, 4, 2, force_copy=False))

    # Get a view and a copy before modifying the buffer
    res1_view = wbw(buffer, 3, 5, force_copy=False)
    res1_copy = wbw(buffer, 3, 5, force_copy=True)
    res2_view = wbw(buffer, 3, 0, force_copy=False)
    res2_copy = wbw(buffer, 3, 0, force_copy=True)
    res3_copy = wbw(buffer, 4, 1, force_copy=False)
    assert [3, 4] == list(res1_view)
    assert [3, 4] == list(res1_copy)
    assert [3, 4] == list(res2_view)
    assert [3, 4] == list(res2_copy)
    assert [4, 0] == list(res3_copy)

    # Modify the buffer and check that only the view is updated
    buffer[4] = 9
    assert [3, 9] == list(res1_view)
    assert [3, 4] == list(res1_copy)
    assert [3, 9] == list(res2_view)
    assert [3, 4] == list(res2_copy)
    assert [4, 0] == list(res3_copy)


def test_get_timestamp() -> None:
    """Test the get_timestamp function."""
    buffer = OrderedRingBuffer(
        np.empty(shape=5, dtype=float),
        sampling_period=timedelta(seconds=1),
    )
    for i in range(5):
        buffer.update(Sample(dt(i), Quantity(i)))
    assert dt(4) == buffer.get_timestamp(-1)
    assert dt(0) == buffer.get_timestamp(-5)
    assert dt(-1) == buffer.get_timestamp(-6)
    assert dt(0) == buffer.get_timestamp(0)
    assert dt(5) == buffer.get_timestamp(5)
    assert dt(6) == buffer.get_timestamp(6)

    for i in range(10, 15):
        buffer.update(Sample(dt(i), Quantity(i)))
    assert dt(14) == buffer.get_timestamp(-1)
    assert dt(10) == buffer.get_timestamp(-5)
    assert dt(9) == buffer.get_timestamp(-6)
    assert dt(10) == buffer.get_timestamp(0)
    assert dt(15) == buffer.get_timestamp(5)
    assert dt(16) == buffer.get_timestamp(6)
