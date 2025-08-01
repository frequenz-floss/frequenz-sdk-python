# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the moving window."""

import asyncio
import re
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

import async_solipsism
import numpy as np
import pytest
import time_machine
from frequenz.channels import Broadcast, Sender
from frequenz.core.datetime import UNIX_EPOCH
from frequenz.quantities import Quantity

from frequenz.sdk.timeseries import (
    MovingWindow,
    ResamplerConfig,
    ResamplerConfig2,
    Sample,
)


@pytest.fixture(autouse=True)
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


async def push_logical_meter_data(
    sender: Sender[Sample[Quantity]],
    test_seq: Sequence[float | None],
    start_ts: datetime = UNIX_EPOCH,
    fake_time: time_machine.Coordinates | None = None,
) -> None:
    """Push data in the passed sender to mock `LogicalMeter` behaviour.

    Starting with UNIX_EPOCH.

    Args:
        sender: Sender for pushing resampled samples to the `MovingWindow`.
        test_seq: The Sequence that is pushed into the `MovingWindow`.
        start_ts: The start timestamp of the `MovingWindow`.
        fake_time: The fake time object to shift the time.
    """
    for i, j in zip(test_seq, range(0, len(test_seq))):
        timestamp = start_ts + timedelta(seconds=j)
        await sender.send(
            Sample(timestamp, Quantity(float(i)) if i is not None else None)
        )
        if fake_time is not None:
            # For the wall clock timer we need to make sure that the wall clock is
            # adjusted just before the timer wakes up from sleeping, and then we need to
            # make sure this function returns *after* the timer has woken up.
            await asyncio.sleep(0.999)
            fake_time.shift(1.0)
            await asyncio.sleep(0.001)

    await asyncio.sleep(0.0)


def init_moving_window(
    size: timedelta,
    resampler_config: ResamplerConfig | None = None,
) -> tuple[MovingWindow, Sender[Sample[Quantity]]]:
    """Initialize the moving window with given shape.

    Args:
        size: The size of the `MovingWindow`
        resampler_config: The resampler configuration.

    Returns:
        tuple[MovingWindow, Sender[Sample]]: A pair of sender and `MovingWindow`.
    """
    lm_chan = Broadcast[Sample[Quantity]](name="lm_net_power")
    lm_tx = lm_chan.new_sender()
    window = MovingWindow(
        size=size,
        resampled_data_recv=lm_chan.new_receiver(),
        input_sampling_period=timedelta(seconds=1),
        resampler_config=resampler_config,
    )
    return window, lm_tx


def dt(i: int) -> datetime:  # pylint: disable=invalid-name
    """Create a timestamp from the given index.

    Args:
        i: The index to create a timestamp from.

    Returns:
        The timestamp created from the index.
    """
    return datetime.fromtimestamp(i, tz=timezone.utc)


async def test_access_window_by_index() -> None:
    """Test indexing a window by integer index."""
    window, sender = init_moving_window(timedelta(seconds=2))
    async with window:
        await push_logical_meter_data(sender, [1, 2, 3])
        assert np.array_equal(window[0], 2.0)
        assert np.array_equal(window[1], 3.0)
        assert np.array_equal(window[-1], 3.0)
        assert np.array_equal(window[-2], 2.0)
        with pytest.raises(IndexError):
            _ = window[3]
        with pytest.raises(IndexError):
            _ = window[-3]


async def test_access_window_by_timestamp() -> None:
    """Test indexing a window by timestamp."""
    window, sender = init_moving_window(timedelta(seconds=2))
    async with window:
        await push_logical_meter_data(sender, [0, 1, 2])
        assert np.array_equal(window[dt(1)], 1.0)
        assert np.array_equal(window.at(dt(1)), 1.0)
        assert np.array_equal(window[dt(2)], 2.0)
        assert np.array_equal(window.at(dt(2)), 2.0)
        with pytest.raises(IndexError):
            _ = window[dt(0)]
        with pytest.raises(IndexError):
            _ = window.at(dt(0))
        with pytest.raises(IndexError):
            _ = window[dt(3)]
        with pytest.raises(IndexError):
            _ = window.at(dt(3))


async def test_access_window_by_int_slice() -> None:
    """Test accessing a subwindow with an integer slice.

    Note that the second test is overwriting the data of the first test.
    since the push_lm_data function is starting with the same initial timestamp.
    """
    window, sender = init_moving_window(timedelta(seconds=14))
    async with window:
        await push_logical_meter_data(sender, range(0, 5))
        assert np.array_equal(window[3:5], np.array([3.0, 4.0]))
        assert np.array_equal(window.window(3, 5), np.array([3.0, 4.0]))

        data = [1, 2, 2.5, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1]
        await push_logical_meter_data(sender, data)
        assert np.array_equal(window[5:14], np.array(data[5:14]))
        assert np.array_equal(window.window(5, 14), np.array(data[5:14]))

        # Test with step size (other than 1 not supported)
        assert np.array_equal(window[5:14:1], np.array(data[5:14]))
        assert np.array_equal(window[5:14:None], np.array(data[5:14]))
        with pytest.raises(ValueError):
            _ = window[5:14:2]
        with pytest.raises(ValueError):
            _ = window[14:5:-1]

    window, sender = init_moving_window(timedelta(seconds=5))

    def test_eq(expected: list[float], start: int | None, end: int | None) -> None:
        assert np.allclose(
            window.window(start, end), np.array(expected), equal_nan=True
        )
        assert np.allclose(window[start:end], np.array(expected), equal_nan=True)

    async with window:
        test_eq([], 0, 1)

        # Incomplete window
        await push_logical_meter_data(sender, [0.0, 1.0])
        test_eq([0.0, 1.0], 0, 2)
        test_eq([0.0, 1.0], 0, 9)
        test_eq([0.0, 1.0], 0, None)
        test_eq([0.0, 1.0], -9, None)
        test_eq([0.0, 1.0], None, None)
        test_eq([0.0], -2, -1)
        test_eq([1.0], -1, None)

        # Incomplete window with gap
        await push_logical_meter_data(
            sender, [3.0], start_ts=UNIX_EPOCH + timedelta(seconds=3)
        )
        test_eq([0.0, 1.0], 0, 2)
        # test gaps to be NaN
        test_eq([0.0, 1.0, np.nan, 3.0], 0, None)
        test_eq([np.nan, 3.0], -2, None)

        # Test fill_value
        assert np.allclose(
            np.array([0.0, 1.0, 2.0, 3.0]),
            window.window(0, None, fill_value=2.0),
            equal_nan=True,
        )

        # Complete window
        await push_logical_meter_data(sender, [0.0, 1.0, 2.0, 3.0, 4.0])
        test_eq([0.0, 1.0], 0, 2)
        test_eq([3.0, 4.0], -2, None)

        # Complete window with nan
        await push_logical_meter_data(sender, [0.0, 1.0, np.nan])
        test_eq([0.0, 1.0, np.nan], 0, 3)
        test_eq([np.nan, 3.0, 4.0], -3, None)


async def test_access_window_by_ts_slice() -> None:
    """Test accessing a subwindow with a timestamp slice."""
    window, sender = init_moving_window(timedelta(seconds=5))
    async with window:
        await push_logical_meter_data(sender, range(0, 5))
        time_start = UNIX_EPOCH + timedelta(seconds=3)
        time_end = time_start + timedelta(seconds=2)
        assert np.array_equal(window[time_start:time_end], np.array([3.0, 4.0]))  # type: ignore
        assert np.array_equal(window.window(dt(3), dt(5)), np.array([3.0, 4.0]))
        assert np.array_equal(window.window(dt(3), dt(3)), np.array([]))
        # Window also supports slicing with indices outside allowed range
        assert np.array_equal(window.window(dt(3), dt(1)), np.array([]))
        assert np.array_equal(window.window(dt(3), dt(6)), np.array([3, 4]))
        assert np.array_equal(window.window(dt(-1), dt(5)), np.array([0, 1, 2, 3, 4]))


async def test_access_empty_window() -> None:
    """Test accessing an empty window, should throw IndexError."""
    window, _ = init_moving_window(timedelta(seconds=5))
    async with window:
        with pytest.raises(IndexError, match=r"^The buffer is empty\.$"):
            _ = window[42]


async def test_window_size() -> None:  # pylint: disable=too-many-statements
    """Test the size of the window."""
    window, sender = init_moving_window(timedelta(seconds=10))
    async with window:

        def assert_valid_and_covered_counts(
            *,
            since: datetime | None = None,
            until: datetime | None = None,
            expected: int | None = None,
            expected_valid: int | None = None,
            expected_covered: int | None = None,
        ) -> None:
            if expected is not None:
                assert window.count_valid(since=since, until=until) == expected
                assert window.count_covered(since=since, until=until) == expected
                return

            assert window.count_valid(since=since, until=until) == expected_valid
            assert window.count_covered(since=since, until=until) == expected_covered

        assert window.capacity == 10, "Wrong window capacity"
        assert window.count_valid() == 0, "Window should be empty"
        assert window.count_covered() == 0, "Window should be empty"

        await push_logical_meter_data(sender, range(0, 2))
        assert window.capacity == 10, "Wrong window capacity"
        assert window.count_valid() == 2, "Window should be partially full"
        assert window.count_covered() == 2, "Window should be partially full"

        newest_ts = window.newest_timestamp
        assert newest_ts is not None and newest_ts == UNIX_EPOCH + timedelta(seconds=1)

        await push_logical_meter_data(sender, range(2, 5), start_ts=newest_ts)
        assert window.capacity == 10, "Wrong window capacity"
        assert window.count_valid() == 4, "Window should be partially full"
        assert window.count_covered() == 4, "Window should be partially full"

        newest_ts = window.newest_timestamp
        assert newest_ts is not None and newest_ts == UNIX_EPOCH + timedelta(seconds=3)

        await push_logical_meter_data(sender, range(5, 12), start_ts=newest_ts)
        assert window.capacity == 10, "Wrong window capacity"
        assert window.count_valid() == 10, "Window should be full"
        assert window.count_covered() == 10, "Window should be full"

        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=1), expected=9
        )
        assert_valid_and_covered_counts(
            until=UNIX_EPOCH + timedelta(seconds=2), expected=3
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=1),
            until=UNIX_EPOCH + timedelta(seconds=1),
            expected=1,
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=3),
            until=UNIX_EPOCH + timedelta(seconds=8),
            expected=6,
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=8),
            until=UNIX_EPOCH + timedelta(seconds=3),
            expected=0,
        )

        newest_ts = window.newest_timestamp
        assert newest_ts is not None and newest_ts == UNIX_EPOCH + timedelta(seconds=9)
        assert window.oldest_timestamp == UNIX_EPOCH

        await push_logical_meter_data(sender, range(5, 12), start_ts=newest_ts)
        assert window.capacity == 10, "Wrong window capacity"
        assert_valid_and_covered_counts(expected=10)

        newest_ts = window.newest_timestamp
        assert newest_ts is not None and newest_ts == UNIX_EPOCH + timedelta(seconds=15)
        assert window.oldest_timestamp == UNIX_EPOCH + timedelta(seconds=6)

        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=1),
            until=UNIX_EPOCH + timedelta(seconds=5),
            expected=0,
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=3),
            until=UNIX_EPOCH + timedelta(seconds=8),
            expected=3,
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=6),
            until=UNIX_EPOCH + timedelta(seconds=20),
            expected=10,
        )

        newest_ts = window.newest_timestamp
        assert newest_ts is not None and newest_ts == UNIX_EPOCH + timedelta(seconds=15)
        assert window.oldest_timestamp == UNIX_EPOCH + timedelta(seconds=6)

        await push_logical_meter_data(
            sender, [3, 4, None, None, 10, 12, None], start_ts=newest_ts
        )

        # After the last insertion, the moving window would look like this:
        #
        # +------------------------+----+----+-----+----+----+-----+-----+-----+-----+-----+
        # | MovingWindow timestamp |    |    |     |    |    |     |     |     |     |     |
        # | (seconds after EPOCH)  | 12 | 13 | 14  | 15 | 16 | 17  | 18  | 19  | 20  | 21  |
        # |------------------------+----+----+-----+----+----+-----+-----+-----+-----+-----|
        # | value in buffer        | 8. | 9. | 10. | 3. | 4. | nan | nan | 10. | 12. | nan |
        # +------------------------+----+----+-----+----+----+-----+-----+-----+-----+-----+

        newest_ts = window.newest_timestamp
        assert newest_ts is not None and newest_ts == UNIX_EPOCH + timedelta(seconds=21)
        assert window.oldest_timestamp == UNIX_EPOCH + timedelta(seconds=12)

        assert_valid_and_covered_counts(
            expected_valid=7,
            expected_covered=10,
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=15),
            expected_valid=4,
            expected_covered=7,
        )
        assert_valid_and_covered_counts(
            until=UNIX_EPOCH + timedelta(seconds=19),
            expected_valid=6,
            expected_covered=8,
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=12),
            until=UNIX_EPOCH + timedelta(seconds=15),
            expected_valid=4,
            expected_covered=4,
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=17),
            until=UNIX_EPOCH + timedelta(seconds=18),
            expected_valid=0,
            expected_covered=2,
        )
        assert_valid_and_covered_counts(
            since=UNIX_EPOCH + timedelta(seconds=16),
            until=UNIX_EPOCH + timedelta(seconds=20),
            expected_valid=3,
            expected_covered=5,
        )


async def test_wait_for_samples() -> None:
    """Test waiting for samples in the window."""
    window, sender = init_moving_window(timedelta(seconds=10))
    async with window:
        task = asyncio.create_task(window.wait_for_samples(5))
        await asyncio.sleep(0)
        assert not task.done()
        await push_logical_meter_data(sender, range(0, 5))
        await asyncio.sleep(0)
        # After pushing 5 values, the `wait_for_samples` task should be done.
        assert task.done()

        task = asyncio.create_task(window.wait_for_samples(5))
        await asyncio.sleep(0)
        await push_logical_meter_data(
            sender, [1, 2, 3, 4], start_ts=UNIX_EPOCH + timedelta(seconds=5)
        )
        await asyncio.sleep(0)
        # The task should not be done yet, since we have only pushed 4 values.
        assert not task.done()

        await push_logical_meter_data(
            sender, [1], start_ts=UNIX_EPOCH + timedelta(seconds=9)
        )
        await asyncio.sleep(0)
        # After pushing the last value, the task should be done.
        assert task.done()

        task = asyncio.create_task(window.wait_for_samples(-1))
        with pytest.raises(
            ValueError,
            match=re.escape("The number of samples to wait for must be 0 or greater."),
        ):
            await task

        task = asyncio.create_task(window.wait_for_samples(20))
        with pytest.raises(
            ValueError,
            match=re.escape(
                "The number of samples to wait for must be less than or equal to the "
                + "capacity of the MovingWindow (10)."
            ),
        ):
            await task

        task = asyncio.create_task(window.wait_for_samples(4))
        await asyncio.sleep(0)
        await push_logical_meter_data(
            sender, range(0, 10), start_ts=UNIX_EPOCH + timedelta(seconds=10)
        )
        await asyncio.sleep(0)
        assert task.done()

        task = asyncio.create_task(window.wait_for_samples(10))
        await asyncio.sleep(0)
        await push_logical_meter_data(
            sender, range(0, 5), start_ts=UNIX_EPOCH + timedelta(seconds=20)
        )
        await asyncio.sleep(0)
        assert not task.done()

        await push_logical_meter_data(
            sender, range(10, 15), start_ts=UNIX_EPOCH + timedelta(seconds=25)
        )
        await asyncio.sleep(0)
        assert task.done()

        task = asyncio.create_task(window.wait_for_samples(5))
        await asyncio.sleep(0)
        await push_logical_meter_data(
            sender, [1, 2, None, 4, None], start_ts=UNIX_EPOCH + timedelta(seconds=30)
        )
        await asyncio.sleep(0)
        # `None` values *are* counted towards the number of samples to wait for.
        assert task.done()


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_wait_for_samples_with_resampling(
    config_class: type[ResamplerConfig], fake_time: time_machine.Coordinates
) -> None:
    """Test waiting for samples in a moving window with resampling."""
    window, sender = init_moving_window(
        timedelta(seconds=20), config_class(resampling_period=timedelta(seconds=2))
    )
    async with window:
        task = asyncio.create_task(window.wait_for_samples(3))
        await asyncio.sleep(0)
        assert not task.done()
        await push_logical_meter_data(sender, range(0, 7), fake_time=fake_time)
        assert task.done()

        task = asyncio.create_task(window.wait_for_samples(10))
        await push_logical_meter_data(
            sender,
            range(0, 10),
            fake_time=fake_time,
            start_ts=UNIX_EPOCH + timedelta(seconds=7),
        )
        assert window.count_covered() == 8
        assert not task.done()

        await push_logical_meter_data(
            sender,
            range(0, 6),
            fake_time=fake_time,
            start_ts=UNIX_EPOCH + timedelta(seconds=17),
        )
        assert window.count_covered() == 10
        assert not task.done()

        await push_logical_meter_data(
            sender,
            range(0, 6),
            fake_time=fake_time,
            start_ts=UNIX_EPOCH + timedelta(seconds=23),
        )
        assert window.count_covered() == 10
        assert window.count_valid() == 10
        assert task.done()

        task = asyncio.create_task(window.wait_for_samples(5))
        await push_logical_meter_data(
            sender,
            [1, 2, None, None, None, None, None, None, None, None],
            fake_time=fake_time,
            start_ts=UNIX_EPOCH + timedelta(seconds=29),
        )
        assert window.count_covered() == 10
        assert window.count_valid() == 8
        assert task.done()

        task = asyncio.create_task(window.wait_for_samples(5))
        await push_logical_meter_data(
            sender,
            [None, 4, None, None, None, None, None, None, None, 5],
            fake_time=fake_time,
            start_ts=UNIX_EPOCH + timedelta(seconds=39),
        )
        assert window.count_covered() == 10
        # There is also an inconsistency here between the monotonic and wall clock,
        # the wall clock timer have less samples in the buffer at the end. This is
        # probably some border condition because of the way we fake time
        is_wall_clock = config_class is ResamplerConfig2
        assert window.count_valid() == (5 if is_wall_clock else 7)
        assert task.done()


# pylint: disable=redefined-outer-name
async def test_resampling_window(fake_time: time_machine.Coordinates) -> None:
    """Test resampling in MovingWindow."""
    channel = Broadcast[Sample[Quantity]](name="net_power")
    sender = channel.new_sender()

    window_size = timedelta(seconds=16)
    input_sampling = timedelta(seconds=1)
    output_sampling = timedelta(seconds=2)
    resampler_config = ResamplerConfig(resampling_period=output_sampling)

    async with MovingWindow(
        size=window_size,
        resampled_data_recv=channel.new_receiver(),
        input_sampling_period=input_sampling,
        resampler_config=resampler_config,
    ) as window:
        assert window.capacity == window_size / output_sampling, "Wrong window capacity"
        assert window.count_valid() == 0, "Window should be empty at the beginning"
        stream_values = [4.0, 8.0, 2.0, 6.0, 5.0] * 100
        for value in stream_values:
            timestamp = datetime.now(tz=timezone.utc)
            sample = Sample(timestamp, Quantity(float(value)))
            await sender.send(sample)
            await asyncio.sleep(0.1)
            fake_time.shift(0.1)

        assert window.count_valid() == window_size / output_sampling
        for value in window:  # type: ignore
            assert 4.9 < value < 5.1


async def test_timestamps() -> None:
    """Test indexing a window by timestamp."""
    window, sender = init_moving_window(timedelta(seconds=5))
    async with window:
        await push_logical_meter_data(
            sender, [1, 2], start_ts=UNIX_EPOCH + timedelta(seconds=1)
        )
        assert window.oldest_timestamp == UNIX_EPOCH + timedelta(seconds=1)
        assert window.newest_timestamp == UNIX_EPOCH + timedelta(seconds=2)
