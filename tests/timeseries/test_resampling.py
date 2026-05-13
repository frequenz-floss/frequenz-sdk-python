# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the `TimeSeriesResampler` class."""

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Broadcast, Receiver, Sender, SenderError
from frequenz.quantities import Quantity

from frequenz.sdk.timeseries import (
    DEFAULT_BUFFER_LEN_MAX,
    DEFAULT_BUFFER_LEN_WARN,
    ResamplerConfig,
    ResamplerConfig2,
    ResamplingFunction,
    Sample,
    Sink,
    Source,
    SourceProperties,
    TickInfo,
    WindowSide,
)
from frequenz.sdk.timeseries._resampling._exceptions import (
    ResamplingError,
    SourceStoppedError,
)
from frequenz.sdk.timeseries._resampling._resampler import Resampler, _ResamplingHelper

from ..utils import a_sequence

# We relax some pylint checks as for tests they don't make a lot of sense for this test.
# pylint: disable=too-many-lines,disable=too-many-locals
# pylint: disable=too-many-arguments,disable=too-many-positional-arguments


@pytest.fixture(autouse=True)
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


@pytest.fixture
async def source_chan() -> AsyncIterator[Broadcast[Sample[Quantity]]]:
    """Create a broadcast channel of samples."""
    chan = Broadcast[Sample[Quantity]](name="test")
    yield chan
    await chan.close()


def as_float_tuple(sample: Sample[Quantity]) -> tuple[datetime, float]:
    """Convert a sample to a tuple of datetime and float value."""
    assert sample.value is not None, "Sample value should not be None"
    return (sample.timestamp, sample.value.base_value)


async def _advance_time(fake_time: time_machine.Coordinates, seconds: float) -> None:
    """Advance the time by the given number of seconds.

    This advances both the wall clock and the time machine fake time.

    Args:
        fake_time: The time machine fake time.
        seconds: The number of seconds to advance the time by.
    """
    await asyncio.sleep(seconds)
    fake_time.shift(seconds)


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
async def _assert_no_more_samples(
    resampler: Resampler,
    initial_time: datetime,
    sink_mock: AsyncMock,
    resampling_fun_mock: MagicMock,
    fake_time: time_machine.Coordinates,
    resampling_period_s: float,
    current_iteration: int,
) -> None:
    """Assert that no more samples are received, so resampling emits None."""
    # Resample 3 more times making sure no more valid samples are used
    for i in range(3):
        # Third resampling run (no more samples)
        await _advance_time(fake_time, resampling_period_s)
        await resampler.resample(one_shot=True)

        iteration_delta = resampling_period_s * (current_iteration + i)
        iteration_time = initial_time + timedelta(seconds=iteration_delta)
        assert datetime.now(timezone.utc) == iteration_time
        sink_mock.assert_called_once_with(Sample(iteration_time, None))
        resampling_fun_mock.assert_not_called()
        sink_mock.reset_mock()
        resampling_fun_mock.reset_mock()


@pytest.mark.parametrize("init_len", list(range(1, DEFAULT_BUFFER_LEN_WARN + 1, 16)))
@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampler_config_len_ok(
    init_len: int,
    config_class: type[ResamplerConfig],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test checks on the resampling buffer."""
    config = config_class(
        resampling_period=timedelta(seconds=1.0),
        initial_buffer_len=init_len,
    )
    assert config.initial_buffer_len == init_len
    # Ignore errors produced by wrongly finalized gRPC server in unrelated tests
    assert _filter_logs(caplog.record_tuples, logger_name="") == []


@pytest.mark.parametrize(
    "init_len",
    range(DEFAULT_BUFFER_LEN_WARN + 1, DEFAULT_BUFFER_LEN_MAX + 1, 64),
)
@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampler_config_len_warn(
    init_len: int, config_class: type[ResamplerConfig], caplog: pytest.LogCaptureFixture
) -> None:
    """Test checks on the resampling buffer."""
    config = config_class(
        resampling_period=timedelta(seconds=1.0),
        initial_buffer_len=init_len,
    )
    assert config.initial_buffer_len == init_len
    # Ignore errors produced by wrongly finalized gRPC server in unrelated tests
    assert _filter_logs(
        caplog.record_tuples, logger_name="frequenz.sdk.timeseries._resampling._config"
    ) == [
        (
            "frequenz.sdk.timeseries._resampling._config",
            logging.WARNING,
            f"initial_buffer_len ({init_len}) is bigger than "
            f"warn_buffer_len ({DEFAULT_BUFFER_LEN_WARN})",
        )
    ]


@pytest.mark.parametrize(
    "init_len",
    list(range(-2, 1)) + [DEFAULT_BUFFER_LEN_MAX + 1, DEFAULT_BUFFER_LEN_MAX + 2],
)
@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampler_config_len_error(
    init_len: int, config_class: type[ResamplerConfig]
) -> None:
    """Test checks on the resampling buffer."""
    with pytest.raises(ValueError):
        _ = config_class(
            resampling_period=timedelta(seconds=1.0),
            initial_buffer_len=init_len,
        )


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampler_config_tick_delay_negative_error(
    config_class: type[ResamplerConfig],
) -> None:
    """Test that negative tick_delay values are rejected."""
    with pytest.raises(ValueError, match="tick_delay"):
        _ = config_class(
            resampling_period=timedelta(seconds=1.0),
            tick_delay=timedelta(milliseconds=-1),
        )


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
@pytest.mark.parametrize("tick_delay", [timedelta(seconds=1.0), timedelta(seconds=1.1)])
async def test_resampler_config_tick_delay_too_big_error(
    config_class: type[ResamplerConfig], tick_delay: timedelta
) -> None:
    """Test that tick_delay must be smaller than resampling_period."""
    with pytest.raises(ValueError, match="smaller than resampling_period"):
        _ = config_class(
            resampling_period=timedelta(seconds=1.0),
            tick_delay=tick_delay,
        )


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_helper_buffer_too_big(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test checks on the resampling buffer."""
    config = config_class(
        resampling_period=timedelta(seconds=DEFAULT_BUFFER_LEN_MAX + 1),
        max_data_age_in_periods=1,
    )
    helper = _ResamplingHelper("test", config)

    for i in range(DEFAULT_BUFFER_LEN_MAX + 1):
        sample = (datetime.now(timezone.utc), i)
        helper.add_sample(sample)
        await _advance_time(fake_time, 1)

    _ = helper.resample(datetime.now(timezone.utc))
    # Ignore errors produced by wrongly finalized gRPC server in unrelated tests
    assert (
        "frequenz.sdk.timeseries._resampling._resampler",
        logging.ERROR,
        f"The new buffer length ({DEFAULT_BUFFER_LEN_MAX + 1}) "
        f"for timeseries test is too big, using {DEFAULT_BUFFER_LEN_MAX} instead",
    ) in _filter_logs(
        caplog.record_tuples,
    )
    # pylint: disable=protected-access
    assert helper._buffer.maxlen == DEFAULT_BUFFER_LEN_MAX


# Tick-delay test fixtures: time constants
@pytest.fixture
def window_end() -> datetime:
    """Define the logical end of the resampling window."""
    return datetime(2020, 1, 1, 0, 0, 10, tzinfo=timezone.utc)


@pytest.fixture
def resampling_period() -> timedelta:
    """Define the resampling period used by the test resampler."""
    return timedelta(seconds=10)


@pytest.fixture
def tick_delay() -> timedelta:
    """Define the delay between timer tick and resampling processing."""
    return timedelta(milliseconds=200)


@pytest.fixture
def after_tick_delay(tick_delay: timedelta) -> timedelta:
    """Define a deterministic delay that happens after tick_delay has elapsed."""
    return tick_delay + timedelta(milliseconds=10)


# Tick-delay test fixtures: channels and mocks
@pytest.fixture
def source_receiver(
    source_chan: Broadcast[Sample[Quantity]],
) -> Receiver[Sample[Quantity]]:
    """Define a receiver for samples sent to the test source channel."""
    return source_chan.new_receiver()


@pytest.fixture
def source_sender(
    source_chan: Broadcast[Sample[Quantity]],
) -> Sender[Sample[Quantity]]:
    """Define a sender for the test source channel."""
    return source_chan.new_sender()


@pytest.fixture
def sink_mock() -> AsyncMock:
    """Define a sink mock used to collect resampled output samples."""
    return AsyncMock(spec=Sink, return_value=True)


@pytest.fixture
def resampling_fun_mock() -> MagicMock:
    """Define a resampling function mock returning a fixed value."""
    return MagicMock(spec=ResamplingFunction, return_value=42.0)


# Tick-delay test fixtures: resampler setup
@pytest.fixture
async def tick_delay_resampler(
    window_end: datetime,
    source_receiver: Receiver[Sample[Quantity]],
    sink_mock: AsyncMock,
    resampling_fun_mock: MagicMock,
    resampling_period: timedelta,
    tick_delay: timedelta,
) -> AsyncIterator[Resampler]:
    """Create a resampler configured with tick_delay and one deterministic tick."""

    async def timer() -> AsyncIterator[TickInfo]:
        yield TickInfo(expected_tick_time=window_end, sleep_infos=[])

    config = ResamplerConfig2(
        resampling_period=resampling_period,
        max_data_age_in_periods=1.0,
        resampling_function=resampling_fun_mock,
        closed=WindowSide.LEFT,
        tick_delay=tick_delay,
    )
    resampler = Resampler(config)

    # Use a deterministic timer tick so tests can control sample arrival time
    # independently from the logical window boundary.
    # pylint: disable=protected-access
    resampler._timer = cast(Any, timer())

    resampler.add_timeseries("test", source_receiver, sink_mock)

    try:
        yield resampler
    finally:
        await resampler.stop()


# Tick-delay test fixtures: sample timestamps
@pytest.fixture
def sample_at_window_start(
    window_end: datetime,
    resampling_period: timedelta,
) -> Sample[Quantity]:
    """Define a sample exactly at the included left boundary of the window."""
    return Sample(window_end - resampling_period, value=Quantity(1.0))


@pytest.fixture
def sample_inside_window(window_end: datetime) -> Sample[Quantity]:
    """Define a sample inside the selected resampling window."""
    return Sample(window_end - timedelta(milliseconds=100), value=Quantity(2.0))


@pytest.fixture
def sample_at_window_end(window_end: datetime) -> Sample[Quantity]:
    """Define a sample exactly at the excluded right boundary of the window."""
    return Sample(window_end, value=Quantity(3.0))


@pytest.fixture
def sample_inside_tick_delay(
    window_end: datetime,
    tick_delay: timedelta,
) -> Sample[Quantity]:
    """Define a sample timestamped after the window end but before tick_delay ends."""
    return Sample(window_end + (tick_delay / 2), value=Quantity(4.0))


@pytest.fixture
def sample_at_tick_delay_end(
    window_end: datetime,
    tick_delay: timedelta,
) -> Sample[Quantity]:
    """Define a sample timestamped exactly at window_end + tick_delay."""
    return Sample(window_end + tick_delay, value=Quantity(5.0))


@pytest.fixture
def sample_after_tick_delay_end(
    window_end: datetime,
    after_tick_delay: timedelta,
) -> Sample[Quantity]:
    """Define a sample timestamped after window_end + tick_delay."""
    return Sample(window_end + after_tick_delay, value=Quantity(6.0))


async def test_tick_delay_prebuffered_samples_follow_timestamp_boundaries(
    tick_delay_resampler: Resampler,
    source_receiver: Receiver[Sample[Quantity]],
    source_sender: Sender[Sample[Quantity]],
    sink_mock: AsyncMock,
    resampling_fun_mock: MagicMock,
    window_end: datetime,
    sample_at_window_start: Sample[Quantity],
    sample_inside_window: Sample[Quantity],
    sample_at_window_end: Sample[Quantity],
    sample_inside_tick_delay: Sample[Quantity],
    sample_at_tick_delay_end: Sample[Quantity],
) -> None:
    """Prebuffered samples are selected strictly by timestamp window boundaries.

    All samples are received before tick handling starts, so arrival time is
    intentionally not a factor in this scenario. The test verifies that only
    timestamps inside the configured [start, end) window are used, while
    boundary and post-window timestamps are excluded.
    """
    await source_sender.send(sample_at_window_start)
    await source_sender.send(sample_inside_window)
    await source_sender.send(sample_at_window_end)
    await source_sender.send(sample_inside_tick_delay)
    await source_sender.send(sample_at_tick_delay_end)

    # Let the resampler's background receiving task buffer the samples before
    # the timer tick is processed.
    await asyncio.sleep(0)

    await tick_delay_resampler.resample(one_shot=True)

    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample_at_window_start),
            as_float_tuple(sample_inside_window),
        ),
        tick_delay_resampler.config,
        tick_delay_resampler.get_source_properties(source_receiver),
    )
    sink_mock.assert_called_once_with(Sample(window_end, Quantity(42.0)))


async def test_without_tick_delay_late_window_samples_are_missed(
    window_end: datetime,
    source_receiver: Receiver[Sample[Quantity]],
    source_sender: Sender[Sample[Quantity]],
    sink_mock: AsyncMock,
    resampling_fun_mock: MagicMock,
    resampling_period: timedelta,
    sample_at_window_start: Sample[Quantity],
    sample_inside_window: Sample[Quantity],
) -> None:
    """Regression test: without `tick_delay`, late in-window samples are missed.

    The samples have timestamps inside the selected window, but they arrive
    after the timer tick. With zero delay, processing happens immediately, so
    they are not buffered in time for this resampling run.
    """

    async def timer() -> AsyncIterator[TickInfo]:
        yield TickInfo(expected_tick_time=window_end, sleep_infos=[])

    config = ResamplerConfig2(
        resampling_period=resampling_period,
        max_data_age_in_periods=1.0,
        resampling_function=resampling_fun_mock,
        closed=WindowSide.LEFT,
        tick_delay=timedelta(0),
    )
    resampler = Resampler(config)
    # pylint: disable=protected-access
    resampler._timer = cast(Any, timer())
    resampler.add_timeseries("test", source_receiver, sink_mock)

    async def send_samples_after_tick() -> None:
        await asyncio.sleep(0.05)

        await source_sender.send(sample_at_window_start)
        await source_sender.send(sample_inside_window)

        await asyncio.sleep(0)

    try:
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(send_samples_after_tick())
            await resampler.resample(one_shot=True)
    finally:
        await resampler.stop()

    resampling_fun_mock.assert_not_called()
    sink_mock.assert_called_once_with(Sample(window_end, None))


async def test_tick_delay_includes_only_window_samples_arriving_during_delay(
    tick_delay_resampler: Resampler,
    source_receiver: Receiver[Sample[Quantity]],
    source_sender: Sender[Sample[Quantity]],
    sink_mock: AsyncMock,
    resampling_fun_mock: MagicMock,
    window_end: datetime,
    tick_delay: timedelta,
    sample_at_window_start: Sample[Quantity],
    sample_inside_window: Sample[Quantity],
    sample_at_window_end: Sample[Quantity],
    sample_inside_tick_delay: Sample[Quantity],
    sample_at_tick_delay_end: Sample[Quantity],
    sample_after_tick_delay_end: Sample[Quantity],
) -> None:
    """Late arrivals are included only when their timestamps are in-window.

    Samples arrive after the timer tick but before delayed processing happens.
    The in-window samples should be included; timestamps at or after the window
    end should still be excluded.
    """

    async def send_samples_during_tick_delay() -> None:
        await asyncio.sleep((tick_delay / 2).total_seconds())

        await source_sender.send(sample_at_window_start)
        await source_sender.send(sample_inside_window)
        await source_sender.send(sample_at_window_end)
        await source_sender.send(sample_inside_tick_delay)
        await source_sender.send(sample_at_tick_delay_end)
        await source_sender.send(sample_after_tick_delay_end)

        await asyncio.sleep(0)

    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(send_samples_during_tick_delay())
        await tick_delay_resampler.resample(one_shot=True)

    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample_at_window_start),
            as_float_tuple(sample_inside_window),
        ),
        tick_delay_resampler.config,
        tick_delay_resampler.get_source_properties(source_receiver),
    )
    sink_mock.assert_called_once_with(Sample(window_end, Quantity(42.0)))


async def test_tick_delay_excludes_window_samples_arriving_after_delay(
    tick_delay_resampler: Resampler,
    source_sender: Sender[Sample[Quantity]],
    sink_mock: AsyncMock,
    resampling_fun_mock: MagicMock,
    window_end: datetime,
    after_tick_delay: timedelta,
    sample_at_window_start: Sample[Quantity],
    sample_inside_window: Sample[Quantity],
) -> None:
    """In-window samples arriving after tick_delay are not considered.

    The sample timestamps belong to the selected window, but their arrival time
    is after delayed processing has already started.
    """

    async def send_samples_after_tick_delay() -> None:
        await asyncio.sleep(after_tick_delay.total_seconds())

        await source_sender.send(sample_at_window_start)
        await source_sender.send(sample_inside_window)

        await asyncio.sleep(0)

    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(send_samples_after_tick_delay())
        await tick_delay_resampler.resample(one_shot=True)

    resampling_fun_mock.assert_not_called()
    sink_mock.assert_called_once_with(Sample(window_end, None))


@pytest.mark.parametrize(
    "resampling_period_s,now,align_to,result",
    (
        (
            1.0,
            datetime(2020, 1, 1, 2, 3, 5, 300000, tzinfo=timezone.utc),
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            (
                datetime(2020, 1, 1, 2, 3, 7, tzinfo=timezone.utc),
                timedelta(seconds=0.7),
            ),
        ),
        (
            3.0,
            datetime(2020, 1, 1, 2, 3, 5, 300000, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
            (
                datetime(2020, 1, 1, 2, 3, 11, tzinfo=timezone.utc),
                timedelta(seconds=2.7),
            ),
        ),
        (
            10.0,
            datetime(2020, 1, 1, 2, 3, 5, 300000, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
            (
                datetime(2020, 1, 1, 2, 3, 25, tzinfo=timezone.utc),
                timedelta(seconds=9.7),
            ),
        ),
        # Future align_to
        (
            10.0,
            datetime(2020, 1, 1, 2, 3, 5, 300000, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 2, 3, 18, tzinfo=timezone.utc),
            (
                datetime(2020, 1, 1, 2, 3, 18, tzinfo=timezone.utc),
                timedelta(seconds=2.7),
            ),
        ),
    ),
)
async def test_calculate_window_end_trivial_cases(
    fake_time: time_machine.Coordinates,
    resampling_period_s: float,
    now: datetime,
    align_to: datetime,
    result: tuple[datetime, timedelta],
) -> None:
    """Test the calculation of the resampling window end for simple cases."""
    resampling_period = timedelta(seconds=resampling_period_s)
    resampler = Resampler(
        ResamplerConfig(
            resampling_period=resampling_period,
            align_to=align_to,
        )
    )
    fake_time.move_to(now)
    # pylint: disable-next=protected-access
    assert resampler._calculate_window_end() == result

    # Repeat the test with align_to=None, so the result should be align to now
    # instead
    resampler_now = Resampler(
        ResamplerConfig(
            resampling_period=resampling_period,
            align_to=now,
        )
    )
    resampler_none = Resampler(
        ResamplerConfig(
            resampling_period=resampling_period,
            align_to=None,
        )
    )
    fake_time.move_to(now)
    # pylint: disable=protected-access
    none_result = resampler_none._calculate_window_end()
    assert resampler_now._calculate_window_end() == none_result
    assert none_result[0] == now + resampling_period


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampling_window_size_is_constant(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test resampling window size is consistent."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = config_class(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=1.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=4,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4
    #        |----------|----------R----|-----|----------R-----> (no more samples)
    # value  5.0       12.0            2.0   4.0        5.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=Quantity(5.0))
    sample1s = Sample(timestamp + timedelta(seconds=1), value=Quantity(12.0))
    await source_sender.send(sample0s)
    await source_sender.send(sample1s)
    await _advance_time(
        fake_time, resampling_period_s
    )  # timer matches resampling period
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    assert asyncio.get_event_loop().time() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(as_float_tuple(sample1s)), config, source_props
    )
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=Quantity(2.0))
    sample3s = Sample(timestamp + timedelta(seconds=3), value=Quantity(4.0))
    sample4s = Sample(timestamp + timedelta(seconds=4), value=Quantity(5.0))
    await source_sender.send(sample2_5s)
    await source_sender.send(sample3s)
    await source_sender.send(sample4s)
    await _advance_time(
        fake_time, resampling_period_s + 0.5
    )  # Timer fired with some delay
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4.5
    sink_mock.assert_called_once_with(
        Sample(
            # But the sample still gets 4s as timestamp, because we are keeping
            # the window size constant, not dependent on when the timer fired
            timestamp + timedelta(seconds=resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample2_5s),
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
        ),
        config,
        source_props,
    )
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()


# Not parametrized because now warnings are handled by the wall clock timer when the
# wall clock timer is used, not the resampler, so it should be tested in the wall clock
# timer tests.
async def test_timer_errors_are_logged(  # pylint: disable=too-many-statements
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that big differences between the expected window end and the fired timer are logged."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = ResamplerConfig(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=2.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=4,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    #
    # trigger            T = 2.0      T = 4.1998     T = 6.3998
    # t(s)   0     1     2 2.5 3     4|4.5 5     6   |
    #        |-----|-----R--|--|-----R+-|--|-----R---+---> (no more samples)
    # value  5.0  12.0    2.0  4.0 5.0 2.0 4.0   5.0
    #
    # R = resampling is done
    # T = timer tick

    # Send a few samples and run a resample tick, advancing the fake time by one period
    # No log message should be produced
    sample0s = Sample(timestamp, value=Quantity(5.0))
    sample1s = Sample(timestamp + timedelta(seconds=1.0), value=Quantity(12.0))
    await source_sender.send(sample0s)
    await source_sender.send(sample1s)
    # Here we need to advance only the wall clock because the resampler timer is not yet
    # started, otherwise the loop time will be advanced twice
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == pytest.approx(2)
    assert asyncio.get_running_loop().time() == pytest.approx(2)
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample0s),
            as_float_tuple(sample1s),
        ),
        config,
        source_props,
    )
    assert not [
        *_filter_logs(
            caplog.record_tuples,
            logger_level=logging.WARNING,
        )
    ]
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run, now with 9.99% delay
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=Quantity(2.0))
    sample3s = Sample(timestamp + timedelta(seconds=3), value=Quantity(4.0))
    sample4s = Sample(timestamp + timedelta(seconds=4), value=Quantity(5.0))
    await source_sender.send(sample2_5s)
    await source_sender.send(sample3s)
    await source_sender.send(sample4s)
    await _advance_time(
        fake_time, resampling_period_s * 1.0999
    )  # Timer is delayed 9.99%
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == pytest.approx(4.1998)
    assert asyncio.get_running_loop().time() == pytest.approx(4.1998)
    sink_mock.assert_called_once_with(
        Sample(
            # But the sample still gets 4s as timestamp, because we are keeping
            # the window size constant, not dependent on when the timer fired
            timestamp + timedelta(seconds=resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample1s),
            as_float_tuple(sample2_5s),
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
        ),
        config,
        source_props,
    )
    assert not [
        *_filter_logs(
            caplog.record_tuples,
            logger_level=logging.WARNING,
        )
    ]
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Third resampling run, now with 10% delay
    sample4_5s = Sample(timestamp + timedelta(seconds=4.5), value=Quantity(2.0))
    sample5s = Sample(timestamp + timedelta(seconds=5), value=Quantity(4.0))
    sample6s = Sample(timestamp + timedelta(seconds=6), value=Quantity(5.0))
    await source_sender.send(sample4_5s)
    await source_sender.send(sample5s)
    await source_sender.send(sample6s)
    await _advance_time(fake_time, resampling_period_s * 1.10)  # Timer delayed 10%
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == pytest.approx(6.3998)
    assert asyncio.get_running_loop().time() == pytest.approx(6.3998)
    sink_mock.assert_called_once_with(
        Sample(
            # But the sample still gets 4s as timestamp, because we are keeping
            # the window size constant, not dependent on when the timer fired
            timestamp + timedelta(seconds=resampling_period_s * 3),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
            as_float_tuple(sample4_5s),
            as_float_tuple(sample5s),
            as_float_tuple(sample6s),
        ),
        config,
        source_props,
    )
    assert (
        "frequenz.sdk.timeseries._resampling._resampler",
        logging.WARNING,
        "The resampling task woke up too late. Resampling should have started at "
        "1970-01-01 00:00:06+00:00, but it started at 1970-01-01 "
        "00:00:06.399800+00:00 (tolerance: 0:00:00.200000, difference: "
        "0:00:00.399800; resampling period: 0:00:02)",
    ) in _filter_logs(caplog.record_tuples, logger_level=logging.WARNING)
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_future_samples_not_included(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test that future samples are not included in the resampling."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = config_class(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=2.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=4,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    #
    # t(s)   0          1      1.9  2          3          4  4.1 4.2
    #        |----------|--------|--R----------|----------R--|---|------------>
    # value  5.0                7.0           4.0            3.0 timer fires
    #                       (with ts=2.1)
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=Quantity(5.0))
    sample1s = Sample(timestamp + timedelta(seconds=1), value=Quantity(12.0))
    sample2_1s = Sample(timestamp + timedelta(seconds=2.1), value=Quantity(7.0))
    await source_sender.send(sample0s)
    await source_sender.send(sample1s)
    await source_sender.send(sample2_1s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample0s),
            as_float_tuple(sample1s),
        ),
        config,
        source_props,  # sample2_1s is not here
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=3, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample3s = Sample(timestamp + timedelta(seconds=3), value=Quantity(4.0))
    sample4_1s = Sample(timestamp + timedelta(seconds=4.1), value=Quantity(3.0))
    await source_sender.send(sample3s)
    await source_sender.send(sample4_1s)
    await _advance_time(fake_time, resampling_period_s + 0.2)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4.2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample1s),
            as_float_tuple(sample2_1s),
            as_float_tuple(sample3s),
        ),
        config,
        source_props,  # sample4_1s is not here
    )


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampling_with_one_window(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test resampling with one resampling window (saving samples of the last period only)."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = config_class(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=1.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=4,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4
    #        |----------|----------R----|-----|----------R-----> (no more samples)
    # value  5.0       12.0            0.0   4.0        5.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=Quantity(5.0))
    sample1s = Sample(timestamp + timedelta(seconds=1), value=Quantity(12.0))
    await source_sender.send(sample0s)
    await source_sender.send(sample1s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample1s),
        ),
        config,
        source_props,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=2, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=Quantity.zero())
    sample3s = Sample(timestamp + timedelta(seconds=3), value=Quantity(4.0))
    sample4s = Sample(timestamp + timedelta(seconds=4), value=Quantity(5.0))
    await source_sender.send(sample2_5s)
    await source_sender.send(sample3s)
    await source_sender.send(sample4s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample2_5s),
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
        ),
        config,
        source_props,
    )
    # By now we have a full buffer (5 samples and a buffer of length 4), which
    # we received in 4 seconds, so we have an input period of 0.8s.
    assert source_props == SourceProperties(
        sampling_start=timestamp,
        received_samples=5,
        sampling_period=timedelta(seconds=0.8),
    )
    # The buffer should be able to hold 2 seconds of data, and data is coming
    # every 0.8 seconds, so we should be able to store 3 samples.
    assert _get_buffer_len(resampler, source_receiver) == 3
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    await _assert_no_more_samples(
        resampler,
        timestamp,
        sink_mock,
        resampling_fun_mock,
        fake_time,
        resampling_period_s,
        current_iteration=3,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp,
        received_samples=5,
        sampling_period=timedelta(seconds=0.8),
    )
    assert _get_buffer_len(resampler, source_receiver) == 3


# Even when a lot could be refactored to use smaller functions, I'm allowing
# too many statements because it makes following failures in tests more easy
# when the code is very flat.
@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampling_with_one_and_a_half_windows(  # pylint: disable=too-many-statements
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test resampling with 1.5 resampling windows."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = config_class(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=1.5,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=7,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4          5          6
    #        |----------|----------R----|-----|----------R----------|----------R-----> (no more)
    # value  5.0       12.0            2.0   4.0        5.0        1.0        3.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=Quantity(5.0))
    sample1s = Sample(timestamp + timedelta(seconds=1), value=Quantity(12.0))
    await source_sender.send(sample0s)
    await source_sender.send(sample1s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample0s),
            as_float_tuple(sample1s),
        ),
        config,
        source_props,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=2, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=Quantity(2.0))
    sample3s = Sample(timestamp + timedelta(seconds=3), value=Quantity(4.0))
    sample4s = Sample(timestamp + timedelta(seconds=4), value=Quantity(5.0))
    await source_sender.send(sample2_5s)
    await source_sender.send(sample3s)
    await source_sender.send(sample4s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    # It should include samples in the interval (1, 4] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample2_5s),
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
        ),
        config,
        source_props,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=5, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Third resampling run
    sample5s = Sample(timestamp + timedelta(seconds=5), value=Quantity(1.0))
    sample6s = Sample(timestamp + timedelta(seconds=6), value=Quantity(3.0))
    await source_sender.send(sample5s)
    await source_sender.send(sample6s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 6
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 3),
            Quantity(expected_resampled_value),
        )
    )
    # It should include samples in the interval (3, 6] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample4s),
            as_float_tuple(sample5s),
            as_float_tuple(sample6s),
        ),
        config,
        source_props,
    )
    # By now we have a full buffer (7 samples and a buffer of length 6), which
    # we received in 4 seconds, so we have an input period of 6/7s.
    assert source_props == SourceProperties(
        sampling_start=timestamp,
        received_samples=7,
        sampling_period=timedelta(seconds=6 / 7),
    )
    # The buffer should be able to hold 2 * 1.5 (3) seconds of data, and data
    # is coming every 6/7 seconds (~0.857s), so we should be able to store
    # 4 samples.
    assert _get_buffer_len(resampler, source_receiver) == 4
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Fourth resampling run
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 8
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 4),
            Quantity(expected_resampled_value),
        )
    )
    # It should include samples in the interval (5, 8] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(as_float_tuple(sample6s)),
        config,
        source_props,
    )
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    await _assert_no_more_samples(
        resampler,
        timestamp,
        sink_mock,
        resampling_fun_mock,
        fake_time,
        resampling_period_s,
        current_iteration=5,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp,
        received_samples=7,
        sampling_period=timedelta(seconds=6 / 7),
    )
    assert _get_buffer_len(resampler, source_receiver) == 4


# Even when a lot could be refactored to use smaller functions, I'm allowing
# too many statements because it makes following failures in tests more easy
# when the code is very flat.
@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampling_with_two_windows(  # pylint: disable=too-many-statements
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test resampling with 2 resampling windows."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = config_class(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=2.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=16,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4          5          6
    #        |----------|----------R----|-----|----------R----------|----------R-----> (no more)
    # value  5.0       12.0            2.0   4.0        5.0        1.0        3.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=Quantity(5.0))
    sample1s = Sample(timestamp + timedelta(seconds=1), value=Quantity(12.0))
    await source_sender.send(sample0s)
    await source_sender.send(sample1s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample0s),
            as_float_tuple(sample1s),
        ),
        config,
        source_props,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=2, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=Quantity(2.0))
    sample3s = Sample(timestamp + timedelta(seconds=3), value=Quantity(4.0))
    sample4s = Sample(timestamp + timedelta(seconds=4), value=Quantity(5.0))
    await source_sender.send(sample2_5s)
    await source_sender.send(sample3s)
    await source_sender.send(sample4s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    # It should include samples in the interval (0, 4] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample1s),
            as_float_tuple(sample2_5s),
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
        ),
        config,
        source_props,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=5, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Third resampling run
    sample5s = Sample(timestamp + timedelta(seconds=5), value=Quantity(1.0))
    sample6s = Sample(timestamp + timedelta(seconds=6), value=Quantity(3.0))
    await source_sender.send(sample5s)
    await source_sender.send(sample6s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 6
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 3),
            Quantity(expected_resampled_value),
        )
    )
    # It should include samples in the interval (2, 6] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample2_5s),
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
            as_float_tuple(sample5s),
            as_float_tuple(sample6s),
        ),
        config,
        source_props,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=7, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Fourth resampling run
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 8
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 4),
            Quantity(expected_resampled_value),
        )
    )
    # It should include samples in the interval (4, 8] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample5s),
            as_float_tuple(sample6s),
        ),
        config,
        source_props,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=7, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    await _assert_no_more_samples(
        resampler,
        timestamp,
        sink_mock,
        resampling_fun_mock,
        fake_time,
        resampling_period_s,
        current_iteration=5,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=7, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_receiving_stopped_resampling_error(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test resampling errors if a receiver stops."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = config_class(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=2.0,
        resampling_function=resampling_fun_mock,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Send a sample and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=Quantity(5.0))
    await source_sender.send(sample0s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(as_float_tuple(sample0s)), config, source_props
    )
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Close channel, try to resample again
    await source_chan.close()
    with pytest.raises(SenderError):
        await source_sender.send(sample0s)
    await _advance_time(fake_time, resampling_period_s)
    with pytest.raises(ResamplingError) as excinfo:
        await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    exceptions = excinfo.value.exceptions
    assert len(exceptions) == 1
    assert source_receiver in exceptions
    timeseries_error = exceptions[source_receiver]
    assert isinstance(timeseries_error, SourceStoppedError)
    assert timeseries_error.source is source_receiver


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_receiving_resampling_error(
    config_class: type[ResamplerConfig], fake_time: time_machine.Coordinates
) -> None:
    """Test resampling stops if there is an unknown error."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    resampler = Resampler(
        config_class(
            resampling_period=timedelta(seconds=resampling_period_s),
            max_data_age_in_periods=2.0,
            resampling_function=resampling_fun_mock,
        )
    )

    class TestException(Exception):
        """Test exception."""

    sample0s = Sample(timestamp, value=Quantity(5.0))

    async def make_fake_source() -> Source:
        yield sample0s
        raise TestException("Test error")

    fake_source = make_fake_source()
    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", fake_source, sink_mock)

    # Try to resample
    await _advance_time(fake_time, resampling_period_s)
    with pytest.raises(ResamplingError) as excinfo:
        await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    exceptions = excinfo.value.exceptions
    assert len(exceptions) == 1
    assert fake_source in exceptions
    timeseries_error = exceptions[fake_source]
    assert isinstance(timeseries_error, TestException)


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_timer_is_aligned(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that the resampling timer is aligned to the resampling period."""
    resampling_period_s = 2
    expected_resampled_value = 42.0

    # There is a small difference in behaviour between the (monotonic) Timer and the
    # WallClockTimer: the former will wait until it has at least one full period filled
    # with data, while the later fires at the next aligned period after it started.
    is_wall_clock = issubclass(config_class, ResamplerConfig2)
    start_offset_s = 0 if is_wall_clock else resampling_period_s
    timestamp = datetime.now(timezone.utc)

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = config_class(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=2.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=5,
    )

    # Advance the time a bit so that the resampler is not aligned to the resampling
    # period
    await _advance_time(fake_time, resampling_period_s / 3)
    # t = 0.667s

    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    # Timer           start delay  timer start           first resampling
    #                ,-------------|---------------------|
    #             start = 0.667    |                     |
    # t(s)   0       |  1    1.5   2   2.5    3          4
    #        |-------+--|-----|----|----|-----|----------R-----> (no more samples)
    # input sample   | 5.0   12.0      2.0   4.0        5.0
    #                `-------------|---------------------|
    # WallClockTimer timer start   first resampling      second resampling
    #    (no extra start delay)
    #
    # R = resampling is done

    # Send samples and resample
    sample1s = Sample(timestamp + timedelta(seconds=1.0), value=Quantity(5.0))
    sample1_5s = Sample(timestamp + timedelta(seconds=1.5), value=Quantity(12.0))
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=Quantity(2.0))
    sample3s = Sample(timestamp + timedelta(seconds=3), value=Quantity(4.0))
    sample4s = Sample(timestamp + timedelta(seconds=4), value=Quantity(5.0))
    await source_sender.send(sample1s)
    await source_sender.send(sample1_5s)
    await source_sender.send(sample2_5s)
    await source_sender.send(sample3s)
    await source_sender.send(sample4s)
    await _advance_time(fake_time, start_offset_s + resampling_period_s * 2 / 3)
    # t = 2 (wall clock) / 4 (mono)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == pytest.approx(
        2 if is_wall_clock else 4
    )
    assert asyncio.get_running_loop().time() == pytest.approx(2 if is_wall_clock else 4)
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=start_offset_s + resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    # We are using a buffer of 2 windows, so when the monotonic timer is used, which
    # fires for the first time one period later, we will use all samples received so far
    # (including the ones for the first period where it didn't fired), for the wall clock
    # timer, we will use only the samples received in the first period.
    expected_samples: list[tuple[datetime, float]] = [
        as_float_tuple(sample1s),
        as_float_tuple(sample1_5s),
    ]
    if not is_wall_clock:
        expected_samples.extend(
            [
                as_float_tuple(sample2_5s),
                as_float_tuple(sample3s),
                as_float_tuple(sample4s),
            ]
        )
    resampling_fun_mock.assert_called_once_with(expected_samples, config, source_props)
    assert not [
        *_filter_logs(
            caplog.record_tuples,
            logger_level=logging.WARNING,
        )
    ]
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    await _advance_time(fake_time, resampling_period_s)
    # t = 4 (wall clock) / 6 (mono)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == pytest.approx(
        4 if is_wall_clock else 6
    )
    assert asyncio.get_running_loop().time() == pytest.approx(4 if is_wall_clock else 6)
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=start_offset_s + resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    # We are using a buffer of 2 windows, so when the monotonic timer is used so it is
    # fired at 6 seconds, we only have in the buffer the samples for the window 2-4
    # seconds. For the wall clock timer, which fires at 4 seconds, we have
    # the samples for the last 2 periods, the time window 0-4 seconds.
    expected_samples = []
    if is_wall_clock:
        expected_samples.extend(
            [
                as_float_tuple(sample1s),
                as_float_tuple(sample1_5s),
            ]
        )
    expected_samples.extend(
        [
            as_float_tuple(sample2_5s),
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
        ]
    )
    resampling_fun_mock.assert_called_once_with(expected_samples, config, source_props)
    assert not [
        *_filter_logs(
            caplog.record_tuples,
            logger_level=logging.WARNING,
        )
    ]
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampling_all_zeros(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test resampling with one resampling window full of zeros."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 0.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = config_class(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=1.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=4,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4
    #        |----------|----------R----|-----|----------R-----> (no more samples)
    # value  0.0       0.0             0.0   0.0        0.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=Quantity.zero())
    sample1s = Sample(timestamp + timedelta(seconds=1), value=Quantity.zero())
    await source_sender.send(sample0s)
    await source_sender.send(sample1s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(as_float_tuple(sample1s)), config, source_props
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=2, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=Quantity.zero())
    sample3s = Sample(timestamp + timedelta(seconds=3), value=Quantity.zero())
    sample4s = Sample(timestamp + timedelta(seconds=4), value=Quantity.zero())
    await source_sender.send(sample2_5s)
    await source_sender.send(sample3s)
    await source_sender.send(sample4s)
    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(
            as_float_tuple(sample2_5s),
            as_float_tuple(sample3s),
            as_float_tuple(sample4s),
        ),
        config,
        source_props,
    )
    # By now we have a full buffer (5 samples and a buffer of length 4), which
    # we received in 4 seconds, so we have an input period of 0.8s.
    assert source_props == SourceProperties(
        sampling_start=timestamp,
        received_samples=5,
        sampling_period=timedelta(seconds=0.8),
    )
    # The buffer should be able to hold 2 seconds of data, and data is coming
    # every 0.8 seconds, so we should be able to store 3 samples.
    assert _get_buffer_len(resampler, source_receiver) == 3
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    await _assert_no_more_samples(
        resampler,
        timestamp,
        sink_mock,
        resampling_fun_mock,
        fake_time,
        resampling_period_s,
        current_iteration=3,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp,
        received_samples=5,
        sampling_period=timedelta(seconds=0.8),
    )
    assert _get_buffer_len(resampler, source_receiver) == 3


@pytest.mark.parametrize("closed", [WindowSide.RIGHT, WindowSide.LEFT])
async def test_resampler_closed_option(
    closed: WindowSide,
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test the `closed` option in ResamplerConfig."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = ResamplerConfig(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=1.0,
        resampling_function=resampling_fun_mock,
        closed=closed,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)
    source_props = resampler.get_source_properties(source_receiver)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4
    #        |----------|----------R----|-----|----------R-----> (no more samples)
    # value  5.0       10.0      15.0  1.0   4.0        5.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample1 = Sample(timestamp, value=Quantity(5.0))
    sample2 = Sample(timestamp + timedelta(seconds=1), value=Quantity(10.0))
    sample3 = Sample(timestamp + timedelta(seconds=2), value=Quantity(15.0))
    await source_sender.send(sample1)
    await source_sender.send(sample2)
    await source_sender.send(sample3)

    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s),
            Quantity(expected_resampled_value),
        )
    )
    # Assert the behavior based on the `closed` option
    if closed == WindowSide.RIGHT:
        resampling_fun_mock.assert_called_once_with(
            a_sequence(as_float_tuple(sample2), as_float_tuple(sample3)),
            config,
            source_props,
        )
    elif closed == WindowSide.LEFT:
        resampling_fun_mock.assert_called_once_with(
            a_sequence(as_float_tuple(sample1), as_float_tuple(sample2)),
            config,
            source_props,
        )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=3, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Additional samples at 2.5, 3, and 4 seconds
    sample4 = Sample(timestamp + timedelta(seconds=2.5), value=Quantity(1.0))
    sample5 = Sample(timestamp + timedelta(seconds=3), value=Quantity(4.0))
    sample6 = Sample(timestamp + timedelta(seconds=4), value=Quantity(5.0))
    await source_sender.send(sample4)
    await source_sender.send(sample5)
    await source_sender.send(sample6)

    # Advance time to 4 seconds and resample again
    await _advance_time(fake_time, resampling_period_s * 2)
    await resampler.resample(one_shot=True)

    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            Quantity(expected_resampled_value),
        )
    )
    if closed == WindowSide.RIGHT:
        resampling_fun_mock.assert_called_once_with(
            a_sequence(
                as_float_tuple(sample4),
                as_float_tuple(sample5),
                as_float_tuple(sample6),
            ),
            config,
            source_props,
        )
    elif closed == WindowSide.LEFT:
        resampling_fun_mock.assert_called_once_with(
            a_sequence(
                as_float_tuple(sample3),
                as_float_tuple(sample4),
                as_float_tuple(sample5),
            ),
            config,
            source_props,
        )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=6, sampling_period=None
    )
    assert _get_buffer_len(resampler, source_receiver) == config.initial_buffer_len


@pytest.mark.parametrize("label", [WindowSide.LEFT, WindowSide.RIGHT])
async def test_resampler_label_option(
    label: WindowSide,
    fake_time: time_machine.Coordinates,
    source_chan: Broadcast[Sample[Quantity]],
) -> None:
    """Test the `label` option in ResamplerConfig."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = ResamplerConfig(
        resampling_period=timedelta(seconds=resampling_period_s),
        max_data_age_in_periods=1.0,
        resampling_function=resampling_fun_mock,
        label=label,
    )
    resampler = Resampler(config)

    source_receiver = source_chan.new_receiver()
    source_sender = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_receiver, sink_mock)

    # Send samples and resample
    sample1 = Sample(timestamp, value=Quantity(5.0))
    sample2 = Sample(timestamp + timedelta(seconds=1), value=Quantity(10.0))
    await source_sender.send(sample1)
    await source_sender.send(sample2)

    await _advance_time(fake_time, resampling_period_s)
    await resampler.resample(one_shot=True)

    # Assert the timestamp of the resampled sample
    expected_timestamp = (
        timestamp
        if label == WindowSide.LEFT
        else timestamp + timedelta(seconds=resampling_period_s)
    )
    sink_mock.assert_called_once_with(
        Sample(expected_timestamp, Quantity(expected_resampled_value))
    )


def _get_buffer_len(resampler: Resampler, source_receiver: Source) -> int:
    # pylint: disable-next=protected-access
    blen = resampler._resamplers[source_receiver]._helper._buffer.maxlen
    assert blen is not None
    return blen


def _filter_logs(
    record_tuples: list[tuple[str, int, str]],
    *,
    logger_name: str = "frequenz.sdk.timeseries._resampling._resampler",
    logger_level: int | None = None,
) -> list[tuple[str, int, str]]:
    return [
        t
        for t in record_tuples
        if t[0] == logger_name and (logger_level is None or logger_level == t[1])
    ]
