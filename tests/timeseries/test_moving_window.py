# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the moving window."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Iterator, Sequence, Tuple

import async_solipsism
import numpy as np
import pytest
import time_machine
from frequenz.channels import Broadcast, Sender

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._moving_window import MovingWindow
from frequenz.sdk.timeseries._resampling import ResamplerConfig


# Setting 'autouse' has no effect as this method replaces the event loop for all tests in the file.
@pytest.fixture()
def event_loop() -> Iterator[async_solipsism.EventLoop]:
    """Replace the loop with one that doesn't interact with the outside world."""
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


@pytest.fixture
def fake_time() -> Iterator[time_machine.Coordinates]:
    """Replace real time with a time machine that doesn't automatically tick."""
    with time_machine.travel(0, tick=False) as traveller:
        yield traveller


async def push_lm_data(sender: Sender[Sample], test_seq: Sequence[float]) -> None:
    """
    Push data in the passed sender to mock `LogicalMeter` behaviour.
    Starting with the First of January 2023.

    Args:
        sender: Sender for pushing resampled samples to the `MovingWindow`.
        test_seq: The Sequence that is pushed into the `MovingWindow`.
    """
    start_ts: datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
    for i, j in zip(test_seq, range(0, len(test_seq))):
        timestamp = start_ts + timedelta(seconds=j)
        await sender.send(Sample(timestamp, float(i)))

    await asyncio.sleep(0.0)


def init_moving_window(size: timedelta) -> Tuple[MovingWindow, Sender[Sample]]:
    """
    Initialize the moving window with given shape

    Args:
        size: The size of the `MovingWindow`

    Returns:
        tuple[MovingWindow, Sender[Sample]]: A pair of sender and `MovingWindow`.
    """
    lm_chan = Broadcast[Sample]("lm_net_power")
    lm_tx = lm_chan.new_sender()
    window = MovingWindow(size, lm_chan.new_receiver(), timedelta(seconds=1))
    return window, lm_tx


async def test_access_window_by_index() -> None:
    """Test indexing a window by integer index"""
    window, sender = init_moving_window(timedelta(seconds=1))
    await push_lm_data(sender, [1])
    assert np.array_equal(window[0], 1.0)


async def test_access_window_by_timestamp() -> None:
    """Test indexing a window by timestamp"""
    window, sender = init_moving_window(timedelta(seconds=1))
    await push_lm_data(sender, [1])
    assert np.array_equal(window[datetime(2023, 1, 1, tzinfo=timezone.utc)], 1.0)


async def test_access_window_by_int_slice() -> None:
    """Test accessing a subwindow with an integer slice"""
    window, sender = init_moving_window(timedelta(seconds=5))
    await push_lm_data(sender, range(0, 5))
    assert np.array_equal(window[3:5], np.array([3.0, 4.0]))


async def test_access_window_by_ts_slice() -> None:
    """Test accessing a subwindow with a timestamp slice"""
    window, sender = init_moving_window(timedelta(seconds=5))
    await push_lm_data(sender, range(0, 5))
    time_start = datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=3)
    time_end = time_start + timedelta(seconds=2)
    assert np.array_equal(window[time_start:time_end], np.array([3.0, 4.0]))  # type: ignore


async def test_window_size() -> None:
    """Test the size of the window."""
    window, sender = init_moving_window(timedelta(seconds=5))
    await push_lm_data(sender, range(0, 20))
    assert len(window) == 5


# pylint: disable=redefined-outer-name
async def test_resampling_window(fake_time: time_machine.Coordinates) -> None:
    """Test resampling in MovingWindow."""
    channel = Broadcast[Sample]("net_power")
    sender = channel.new_sender()

    window_size = timedelta(seconds=16)
    input_sampling = timedelta(seconds=1)
    output_sampling = timedelta(seconds=2)
    resampler_config = ResamplerConfig(resampling_period=output_sampling)

    window = MovingWindow(
        size=window_size,
        resampled_data_recv=channel.new_receiver(),
        input_sampling_period=input_sampling,
        resampler_config=resampler_config,
    )

    stream_values = [4.0, 8.0, 2.0, 6.0, 5.0] * 100
    for value in stream_values:
        timestamp = datetime.now(tz=timezone.utc)
        sample = Sample(timestamp, float(value))
        await sender.send(sample)
        await asyncio.sleep(0.1)
        fake_time.shift(0.1)

    assert len(window) == window_size / output_sampling
    for value in window:  # type: ignore
        assert 4.9 < value < 5.1
