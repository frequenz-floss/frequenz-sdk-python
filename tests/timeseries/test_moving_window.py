# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the moving window."""

import asyncio
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone

import async_solipsism
import numpy as np
import pytest
import time_machine
from frequenz.channels import Broadcast, Sender

from frequenz.sdk.timeseries import UNIX_EPOCH, Sample
from frequenz.sdk.timeseries._moving_window import MovingWindow
from frequenz.sdk.timeseries._quantities import Quantity
from frequenz.sdk.timeseries._resampling import ResamplerConfig


# Setting 'autouse' has no effect as this method replaces the event loop for all tests in the file.
@pytest.fixture()
def event_loop() -> Iterator[async_solipsism.EventLoop]:
    """Replace the loop with one that doesn't interact with the outside world."""
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


async def push_logical_meter_data(
    sender: Sender[Sample[Quantity]],
    test_seq: Sequence[float],
    start_ts: datetime = UNIX_EPOCH,
) -> None:
    """Push data in the passed sender to mock `LogicalMeter` behaviour.

    Starting with UNIX_EPOCH.

    Args:
        sender: Sender for pushing resampled samples to the `MovingWindow`.
        test_seq: The Sequence that is pushed into the `MovingWindow`.
        start_ts: The start timestamp of the `MovingWindow`.
    """
    for i, j in zip(test_seq, range(0, len(test_seq))):
        timestamp = start_ts + timedelta(seconds=j)
        await sender.send(Sample(timestamp, Quantity(float(i))))

    await asyncio.sleep(0.0)


def init_moving_window(
    size: timedelta,
) -> tuple[MovingWindow, Sender[Sample[Quantity]]]:
    """Initialize the moving window with given shape.

    Args:
        size: The size of the `MovingWindow`

    Returns:
        tuple[MovingWindow, Sender[Sample]]: A pair of sender and `MovingWindow`.
    """
    lm_chan = Broadcast[Sample[Quantity]]("lm_net_power")
    lm_tx = lm_chan.new_sender()
    window = MovingWindow(size, lm_chan.new_receiver(), timedelta(seconds=1))
    return window, lm_tx


def dt(i: int) -> datetime:  # pylint: disable=invalid-name
    """Create datetime objects from indices.

    Args:
        i: Index to create datetime from.

    Returns:
        Datetime object.
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
        # gap fill not supported yet:
        # test_eq([0.0, 1.0, np.nan, 3.0], 0, None)
        # test_eq([0.0, 1.0, np.nan, 3.0], -9, None)
        # test_eq([np.nan, 3.0], -2, None)

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


async def test_window_size() -> None:
    """Test the size of the window."""
    window, sender = init_moving_window(timedelta(seconds=5))
    async with window:
        assert window.capacity == 5, "Wrong window capacity"
        assert window.count_valid() == 0, "Window should be empty"
        assert window.count_covered() == 0, "Window should be empty"
        await push_logical_meter_data(sender, range(0, 2))
        assert window.capacity == 5, "Wrong window capacity"
        assert window.count_valid() == 2, "Window should be partially full"
        assert window.count_covered() == 2, "Window should be partially full"
        await push_logical_meter_data(sender, range(2, 20))
        assert window.capacity == 5, "Wrong window capacity"
        assert window.count_valid() == 5, "Window should be full"
        assert window.count_covered() == 5, "Window should be full"


# pylint: disable=redefined-outer-name
async def test_resampling_window(fake_time: time_machine.Coordinates) -> None:
    """Test resampling in MovingWindow."""
    channel = Broadcast[Sample[Quantity]]("net_power")
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
