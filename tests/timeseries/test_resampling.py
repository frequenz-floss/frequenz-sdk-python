# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""
Tests for the `TimeSeriesResampler`
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Iterator
from unittest.mock import AsyncMock, MagicMock

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Broadcast

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._resampling import (
    DEFAULT_BUFFER_LEN_MAX,
    DEFAULT_BUFFER_LEN_WARN,
    Resampler,
    ResamplerConfig,
    ResamplingError,
    ResamplingFunction,
    Sink,
    Source,
    SourceProperties,
    SourceStoppedError,
    _ResamplingHelper,
)

from ..utils import a_sequence

# pylint: disable=too-many-locals,redefined-outer-name


@pytest.fixture(autouse=True)
def fake_loop() -> Iterator[async_solipsism.EventLoop]:
    """Replace the loop with one that doesn't interact with the outside world."""
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


@pytest.fixture
def fake_time() -> Iterator[time_machine.Coordinates]:
    """Replace real time with a time machine that doesn't automatically tick."""
    with time_machine.travel(0, tick=False) as traveller:
        yield traveller


@pytest.fixture
async def source_chan() -> AsyncIterator[Broadcast[Sample]]:
    """Create a broadcast channel of samples."""
    chan = Broadcast[Sample]("test")
    yield chan
    await chan.close()


async def _assert_no_more_samples(  # pylint: disable=too-many-arguments
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
        fake_time.shift(resampling_period_s)
        await resampler.resample(one_shot=True)

        iteration_delta = resampling_period_s * (current_iteration + i)
        iteration_time = initial_time + timedelta(seconds=iteration_delta)
        assert datetime.now(timezone.utc) == iteration_time
        sink_mock.assert_called_once_with(Sample(iteration_time, None))
        resampling_fun_mock.assert_not_called()
        sink_mock.reset_mock()
        resampling_fun_mock.reset_mock()


@pytest.mark.parametrize("init_len", list(range(1, DEFAULT_BUFFER_LEN_WARN + 1, 16)))
async def test_resampler_config_len_ok(
    init_len: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test checks on the resampling buffer."""
    config = ResamplerConfig(
        resampling_period_s=1.0,
        initial_buffer_len=init_len,
    )
    assert config.initial_buffer_len == init_len
    # Ignore errors produced by wrongly finalized gRPC server in unrelated tests
    assert _filter_logs(caplog.record_tuples, logger_name="") == []


@pytest.mark.parametrize(
    "init_len",
    range(DEFAULT_BUFFER_LEN_WARN + 1, DEFAULT_BUFFER_LEN_MAX + 1, 64),
)
async def test_resampler_config_len_warn(
    init_len: int, caplog: pytest.LogCaptureFixture
) -> None:
    """Test checks on the resampling buffer."""
    config = ResamplerConfig(
        resampling_period_s=1.0,
        initial_buffer_len=init_len,
    )
    assert config.initial_buffer_len == init_len
    # Ignore errors produced by wrongly finalized gRPC server in unrelated tests
    assert _filter_logs(
        caplog.record_tuples,
        logger_name="frequenz.sdk.timeseries._resampling",
    ) == [
        (
            "frequenz.sdk.timeseries._resampling",
            logging.WARNING,
            f"initial_buffer_len ({init_len}) is bigger than "
            f"warn_buffer_len ({DEFAULT_BUFFER_LEN_WARN})",
        )
    ]


@pytest.mark.parametrize(
    "init_len",
    list(range(-2, 1)) + [DEFAULT_BUFFER_LEN_MAX + 1, DEFAULT_BUFFER_LEN_MAX + 2],
)
async def test_resampler_config_len_error(init_len: int) -> None:
    """Test checks on the resampling buffer."""
    with pytest.raises(ValueError):
        _ = ResamplerConfig(
            resampling_period_s=1.0,
            initial_buffer_len=init_len,
        )


async def test_helper_buffer_too_big(
    fake_time: time_machine.Coordinates,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test checks on the resampling buffer."""
    config = ResamplerConfig(
        resampling_period_s=DEFAULT_BUFFER_LEN_MAX + 1,
        max_data_age_in_periods=1,
    )
    helper = _ResamplingHelper("test", config)

    for i in range(DEFAULT_BUFFER_LEN_MAX + 1):
        sample = Sample(datetime.now(timezone.utc), i)
        helper.add_sample(sample)
        fake_time.shift(1)

    _ = helper.resample(datetime.now(timezone.utc))
    # Ignore errors produced by wrongly finalized gRPC server in unrelated tests
    assert _filter_logs(
        caplog.record_tuples,
        logger_name="frequenz.sdk.timeseries._resampling",
    ) == [
        (
            "frequenz.sdk.timeseries._resampling",
            logging.ERROR,
            f"The new buffer length ({DEFAULT_BUFFER_LEN_MAX + 1}) "
            f"for timeseries test is too big, using {DEFAULT_BUFFER_LEN_MAX} instead",
        )
    ]
    # pylint: disable=protected-access
    assert helper._buffer.maxlen == DEFAULT_BUFFER_LEN_MAX


async def test_resampling_with_one_window(
    fake_time: time_machine.Coordinates, source_chan: Broadcast[Sample]
) -> None:
    """Test resampling with one resampling window (saving samples of the last period only)."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = ResamplerConfig(
        resampling_period_s=resampling_period_s,
        max_data_age_in_periods=1.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=4,
    )
    resampler = Resampler(config)

    source_recvr = source_chan.new_receiver()
    source_sendr = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_recvr, sink_mock)
    source_props = resampler.get_source_properties(source_recvr)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4
    #        |----------|----------R----|-----|----------R-----> (no more samples)
    # value  5.0       12.0            2.0   4.0        5.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=5.0)
    sample1s = Sample(timestamp + timedelta(seconds=1), value=12.0)
    await source_sendr.send(sample0s)
    await source_sendr.send(sample1s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s), expected_resampled_value
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample1s), config, source_props
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=2, sampling_period_s=None
    )
    assert _get_buffer_len(resampler, source_recvr) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=2.0)
    sample3s = Sample(timestamp + timedelta(seconds=3), value=4.0)
    sample4s = Sample(timestamp + timedelta(seconds=4), value=5.0)
    await source_sendr.send(sample2_5s)
    await source_sendr.send(sample3s)
    await source_sendr.send(sample4s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            expected_resampled_value,
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample2_5s, sample3s, sample4s), config, source_props
    )
    # By now we have a full buffer (5 samples and a buffer of length 4), which
    # we received in 4 seconds, so we have an input period of 0.8s.
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=5, sampling_period_s=0.8
    )
    # The buffer should be able to hold 2 seconds of data, and data is coming
    # every 0.8 seconds, so we should be able to store 3 samples.
    assert _get_buffer_len(resampler, source_recvr) == 3
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
        sampling_start=timestamp, received_samples=5, sampling_period_s=0.8
    )
    assert _get_buffer_len(resampler, source_recvr) == 3


# Even when a lot could be refactored to use smaller functions, I'm allowing
# too many statements because it makes following failures in tests more easy
# when the code is very flat.
async def test_resampling_with_one_and_a_half_windows(  # pylint: disable=too-many-statements
    fake_time: time_machine.Coordinates, source_chan: Broadcast[Sample]
) -> None:
    """Test resampling with 1.5 resampling windows."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = ResamplerConfig(
        resampling_period_s=resampling_period_s,
        max_data_age_in_periods=1.5,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=7,
    )
    resampler = Resampler(config)

    source_recvr = source_chan.new_receiver()
    source_sendr = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_recvr, sink_mock)
    source_props = resampler.get_source_properties(source_recvr)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4          5          6
    #        |----------|----------R----|-----|----------R----------|----------R-----> (no more)
    # value  5.0       12.0            2.0   4.0        5.0        1.0        3.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=5.0)
    sample1s = Sample(timestamp + timedelta(seconds=1), value=12.0)
    await source_sendr.send(sample0s)
    await source_sendr.send(sample1s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s), expected_resampled_value
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample0s, sample1s), config, source_props
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=2, sampling_period_s=None
    )
    assert _get_buffer_len(resampler, source_recvr) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=2.0)
    sample3s = Sample(timestamp + timedelta(seconds=3), value=4.0)
    sample4s = Sample(timestamp + timedelta(seconds=4), value=5.0)
    await source_sendr.send(sample2_5s)
    await source_sendr.send(sample3s)
    await source_sendr.send(sample4s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            expected_resampled_value,
        )
    )
    # It should include samples in the interval (1, 4] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample2_5s, sample3s, sample4s), config, source_props
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=5, sampling_period_s=None
    )
    assert _get_buffer_len(resampler, source_recvr) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Third resampling run
    sample5s = Sample(timestamp + timedelta(seconds=5), value=1.0)
    sample6s = Sample(timestamp + timedelta(seconds=6), value=3.0)
    await source_sendr.send(sample5s)
    await source_sendr.send(sample6s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 6
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 3),
            expected_resampled_value,
        )
    )
    # It should include samples in the interval (3, 6] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample4s, sample5s, sample6s), config, source_props
    )
    # By now we have a full buffer (7 samples and a buffer of length 6), which
    # we received in 4 seconds, so we have an input period of 6/7s.
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=7, sampling_period_s=6 / 7
    )
    # The buffer should be able to hold 2 * 1.5 (3) seconds of data, and data
    # is coming every 6/7 seconds (~0.857s), so we should be able to store
    # 4 samples.
    assert _get_buffer_len(resampler, source_recvr) == 4
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Fourth resampling run
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 8
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 4),
            expected_resampled_value,
        )
    )
    # It should include samples in the interval (5, 8] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample6s),
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
        sampling_start=timestamp, received_samples=7, sampling_period_s=6 / 7
    )
    assert _get_buffer_len(resampler, source_recvr) == 4


# Even when a lot could be refactored to use smaller functions, I'm allowing
# too many statements because it makes following failures in tests more easy
# when the code is very flat.
async def test_resampling_with_two_windows(  # pylint: disable=too-many-statements
    fake_time: time_machine.Coordinates, source_chan: Broadcast[Sample]
) -> None:
    """Test resampling with 2 resampling windows."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = ResamplerConfig(
        resampling_period_s=resampling_period_s,
        max_data_age_in_periods=2.0,
        resampling_function=resampling_fun_mock,
        initial_buffer_len=16,
    )
    resampler = Resampler(config)

    source_recvr = source_chan.new_receiver()
    source_sendr = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_recvr, sink_mock)
    source_props = resampler.get_source_properties(source_recvr)

    # Test timeline
    #
    # t(s)   0          1          2   2.5    3          4          5          6
    #        |----------|----------R----|-----|----------R----------|----------R-----> (no more)
    # value  5.0       12.0            2.0   4.0        5.0        1.0        3.0
    #
    # R = resampling is done

    # Send a few samples and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=5.0)
    sample1s = Sample(timestamp + timedelta(seconds=1), value=12.0)
    await source_sendr.send(sample0s)
    await source_sendr.send(sample1s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s), expected_resampled_value
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample0s, sample1s), config, source_props
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=2, sampling_period_s=None
    )
    assert _get_buffer_len(resampler, source_recvr) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Second resampling run
    sample2_5s = Sample(timestamp + timedelta(seconds=2.5), value=2.0)
    sample3s = Sample(timestamp + timedelta(seconds=3), value=4.0)
    sample4s = Sample(timestamp + timedelta(seconds=4), value=5.0)
    await source_sendr.send(sample2_5s)
    await source_sendr.send(sample3s)
    await source_sendr.send(sample4s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 2),
            expected_resampled_value,
        )
    )
    # It should include samples in the interval (0, 4] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample1s, sample2_5s, sample3s, sample4s), config, source_props
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=5, sampling_period_s=None
    )
    assert _get_buffer_len(resampler, source_recvr) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Third resampling run
    sample5s = Sample(timestamp + timedelta(seconds=5), value=1.0)
    sample6s = Sample(timestamp + timedelta(seconds=6), value=3.0)
    await source_sendr.send(sample5s)
    await source_sendr.send(sample6s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 6
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 3),
            expected_resampled_value,
        )
    )
    # It should include samples in the interval (2, 6] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample2_5s, sample3s, sample4s, sample5s, sample6s),
        config,
        source_props,
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=7, sampling_period_s=None
    )
    assert _get_buffer_len(resampler, source_recvr) == config.initial_buffer_len
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Fourth resampling run
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 8
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s * 4),
            expected_resampled_value,
        )
    )
    # It should include samples in the interval (4, 8] seconds
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample5s, sample6s), config, source_props
    )
    assert source_props == SourceProperties(
        sampling_start=timestamp, received_samples=7, sampling_period_s=None
    )
    assert _get_buffer_len(resampler, source_recvr) == config.initial_buffer_len
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
        sampling_start=timestamp, received_samples=7, sampling_period_s=None
    )
    assert _get_buffer_len(resampler, source_recvr) == config.initial_buffer_len


async def test_receiving_stopped_resampling_error(
    fake_time: time_machine.Coordinates, source_chan: Broadcast[Sample]
) -> None:
    """Test resampling errors if a receiver stops."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    config = ResamplerConfig(
        resampling_period_s=resampling_period_s,
        max_data_age_in_periods=2.0,
        resampling_function=resampling_fun_mock,
    )
    resampler = Resampler(config)

    source_recvr = source_chan.new_receiver()
    source_sendr = source_chan.new_sender()

    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", source_recvr, sink_mock)
    source_props = resampler.get_source_properties(source_recvr)

    # Send a sample and run a resample tick, advancing the fake time by one period
    sample0s = Sample(timestamp, value=5.0)
    await source_sendr.send(sample0s)
    fake_time.shift(resampling_period_s)
    await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    sink_mock.assert_called_once_with(
        Sample(
            timestamp + timedelta(seconds=resampling_period_s), expected_resampled_value
        )
    )
    resampling_fun_mock.assert_called_once_with(
        a_sequence(sample0s), config, source_props
    )
    sink_mock.reset_mock()
    resampling_fun_mock.reset_mock()

    # Close channel, try to resample again
    await source_chan.close()
    assert await source_sendr.send(sample0s) is False
    fake_time.shift(resampling_period_s)
    with pytest.raises(ResamplingError) as excinfo:
        await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 4
    exceptions = excinfo.value.exceptions
    assert len(exceptions) == 1
    assert source_recvr in exceptions
    timeseries_error = exceptions[source_recvr]
    assert isinstance(timeseries_error, SourceStoppedError)
    assert timeseries_error.source is source_recvr


async def test_receiving_resampling_error(fake_time: time_machine.Coordinates) -> None:
    """Test resampling stops if there is an unknown error."""
    timestamp = datetime.now(timezone.utc)

    resampling_period_s = 2
    expected_resampled_value = 42.0

    resampling_fun_mock = MagicMock(
        spec=ResamplingFunction, return_value=expected_resampled_value
    )
    resampler = Resampler(
        ResamplerConfig(
            resampling_period_s=resampling_period_s,
            max_data_age_in_periods=2.0,
            resampling_function=resampling_fun_mock,
        )
    )

    class TestException(Exception):
        """Test exception."""

    sample0s = Sample(timestamp, value=5.0)

    async def make_fake_source() -> Source:
        yield sample0s
        raise TestException("Test error")

    fake_source = make_fake_source()
    sink_mock = AsyncMock(spec=Sink, return_value=True)

    resampler.add_timeseries("test", fake_source, sink_mock)

    # Try to resample
    fake_time.shift(resampling_period_s)
    with pytest.raises(ResamplingError) as excinfo:
        await resampler.resample(one_shot=True)

    assert datetime.now(timezone.utc).timestamp() == 2
    exceptions = excinfo.value.exceptions
    assert len(exceptions) == 1
    assert fake_source in exceptions
    timeseries_error = exceptions[fake_source]
    assert isinstance(timeseries_error, TestException)


def _get_buffer_len(resampler: Resampler, source_recvr: Source) -> int:
    # pylint: disable=protected-access
    blen = resampler._resamplers[source_recvr]._helper._buffer.maxlen
    assert blen is not None
    return blen


def _filter_logs(
    record_tuples: list[tuple[str, int, str]], *, logger_name: str
) -> list[tuple[str, int, str]]:
    return [t for t in record_tuples if t[0] == logger_name]
