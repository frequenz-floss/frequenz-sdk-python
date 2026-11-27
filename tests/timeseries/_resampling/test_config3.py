# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for `ResamplerConfig3` and `ResamplingContext`."""

import dataclasses
import logging
import math
from datetime import datetime, timedelta, timezone

import pytest

from frequenz.sdk.timeseries import (
    DEFAULT_BUFFER_LEN_INIT,
    DEFAULT_BUFFER_LEN_MAX,
    DEFAULT_BUFFER_LEN_WARN,
    ResamplerConfig3,
    ResamplingContext,
    SourceProperties,
    WallClockTimerConfig,
    WindowSide,
)

_PERIOD = timedelta(seconds=10)
_START = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _context(config: ResamplerConfig3) -> ResamplingContext:
    """Create a context for the window starting at `_START`."""
    return ResamplingContext(
        window_start=_START, config=config, source_properties=SourceProperties()
    )


def test_defaults() -> None:
    """Test the default values."""
    config = ResamplerConfig3(_PERIOD)

    assert config.resampling_period == _PERIOD
    assert config.max_data_age_in_periods == 1.0
    assert config.closed == WindowSide.LEFT
    assert config.label == WindowSide.LEFT
    assert config.input_period_estimation_samples == DEFAULT_BUFFER_LEN_INIT
    assert config.warn_buffer_len == DEFAULT_BUFFER_LEN_WARN
    assert config.max_buffer_len == DEFAULT_BUFFER_LEN_MAX
    assert config.timer_config is None
    assert config.tick_delay == timedelta(0)


def test_default_resampling_function_is_mean() -> None:
    """Test the default resampling function averages all samples it gets."""
    config = ResamplerConfig3(_PERIOD)
    context = ResamplingContext(
        window_start=_START, config=config, source_properties=SourceProperties()
    )
    samples = [
        (_START - timedelta(seconds=5), 1.0),
        (_START + timedelta(seconds=1), 2.0),
        (_START + timedelta(seconds=2), 6.0),
    ]

    assert config.resampling_function(samples, context) == 3.0


def test_only_resampling_period_is_positional() -> None:
    """Test only the resampling period can be passed by position."""
    with pytest.raises(TypeError):
        ResamplerConfig3(_PERIOD, 3.0)  # type: ignore[call-arg]


def test_all_fields() -> None:
    """Test all fields can be set by keyword."""
    timer_config = WallClockTimerConfig.from_interval(_PERIOD)
    config = ResamplerConfig3(
        resampling_period=_PERIOD,
        max_data_age_in_periods=3,
        closed=WindowSide.RIGHT,
        label=WindowSide.RIGHT,
        input_period_estimation_samples=2,
        warn_buffer_len=3,
        max_buffer_len=4,
        timer_config=timer_config,
        tick_delay=timedelta(seconds=1),
    )

    assert config.max_data_age_in_periods == 3
    assert config.closed == WindowSide.RIGHT
    assert config.label == WindowSide.RIGHT
    assert config.input_period_estimation_samples == 2
    assert config.warn_buffer_len == 3
    assert config.max_buffer_len == 4
    assert config.timer_config is timer_config
    assert config.tick_delay == timedelta(seconds=1)


@pytest.mark.parametrize(
    "period", [timedelta(0), timedelta(seconds=-1)], ids=["zero", "negative"]
)
def test_resampling_period_must_be_positive(period: timedelta) -> None:
    """Test a resampling period that is not positive is rejected."""
    with pytest.raises(ValueError, match=r"resampling_period .* must be positive"):
        ResamplerConfig3(period)


@pytest.mark.parametrize("max_age", [0.0, 0.999])
def test_max_data_age_must_be_at_least_one(max_age: float) -> None:
    """Test maximum data ages smaller than 1.0 are rejected."""
    with pytest.raises(ValueError, match=r"must be at least 1\.0"):
        ResamplerConfig3(_PERIOD, max_data_age_in_periods=max_age)


@pytest.mark.parametrize(
    "max_age", [math.nan, math.inf, 1e20, 10**400], ids=["nan", "inf", "big", "huge"]
)
def test_max_data_age_must_fit_in_a_timedelta(max_age: float) -> None:
    """Test maximum data ages that are not finite or too big are rejected."""
    with pytest.raises(ValueError, match=r"must be a finite number, small enough"):
        ResamplerConfig3(_PERIOD, max_data_age_in_periods=max_age)


def test_tiny_resampling_period_needs_a_timer_config() -> None:
    """Test periods too small for the default timer configuration are rejected."""
    period = timedelta(microseconds=1)
    with pytest.raises(ValueError, match=r"too small for the default timer"):
        ResamplerConfig3(period)

    config = ResamplerConfig3(period, timer_config=WallClockTimerConfig())
    assert config.resampling_period == period


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"warn_buffer_len": 0}, r"warn_buffer_len \(0\) should be at least 1"),
        (
            {"warn_buffer_len": 4, "max_buffer_len": 4},
            r"max_buffer_len \(4\) should be bigger than warn_buffer_len \(4\)",
        ),
        (
            {"input_period_estimation_samples": 0},
            r"input_period_estimation_samples \(0\) should be at least 1",
        ),
        (
            {"input_period_estimation_samples": DEFAULT_BUFFER_LEN_MAX + 1},
            r"input_period_estimation_samples .* is bigger than max_buffer_len",
        ),
        ({"tick_delay": timedelta(seconds=-1)}, r"should be non-negative"),
        ({"tick_delay": _PERIOD}, r"should be smaller than resampling_period"),
    ],
)
def test_invalid_values(kwargs: dict[str, object], message: str) -> None:
    """Test invalid values are rejected."""
    with pytest.raises(ValueError, match=message):
        ResamplerConfig3(_PERIOD, **kwargs)  # type: ignore[arg-type]


def test_input_period_estimation_samples_bigger_than_warn_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test an initial buffer length bigger than the warning length logs a warning."""
    with caplog.at_level(logging.WARNING):
        ResamplerConfig3(
            _PERIOD, input_period_estimation_samples=DEFAULT_BUFFER_LEN_WARN + 1
        )

    assert (
        "input_period_estimation_samples "
        f"({DEFAULT_BUFFER_LEN_WARN + 1}) is bigger than "
        f"warn_buffer_len ({DEFAULT_BUFFER_LEN_WARN})"
    ) in caplog.messages


def test_context_is_frozen() -> None:
    """Test the context can't be changed."""
    context = _context(ResamplerConfig3(_PERIOD))

    with pytest.raises(dataclasses.FrozenInstanceError):
        context.window_start = _START + _PERIOD  # type: ignore[misc]


def test_context_window_end() -> None:
    """Test the window end is one period after the window start."""
    context = _context(ResamplerConfig3(_PERIOD))

    assert context.window_end == _START + _PERIOD


@pytest.mark.parametrize(
    "offset, expected_start",
    [
        (0, _START),
        (-1, _START - _PERIOD),
        (-3, _START - 3 * _PERIOD),
        (1, _START + _PERIOD),
    ],
)
def test_context_window_bounds(offset: int, expected_start: datetime) -> None:
    """Test the bounds of windows relative to the current one."""
    context = _context(ResamplerConfig3(_PERIOD))

    assert context.window_bounds(offset) == (expected_start, expected_start + _PERIOD)


def test_context_window_bounds_default_is_current() -> None:
    """Test the default offset gives the current window."""
    context = _context(ResamplerConfig3(_PERIOD))

    assert context.window_bounds() == (context.window_start, context.window_end)


_ONE_US = timedelta(microseconds=1)


@pytest.mark.parametrize(
    "closed, timestamp, expected_offset",
    [
        # Closed on the left: [start, end)
        (WindowSide.LEFT, _START, 0),
        (WindowSide.LEFT, _START + _ONE_US, 0),
        (WindowSide.LEFT, _START + _PERIOD - _ONE_US, 0),
        (WindowSide.LEFT, _START + _PERIOD, 1),
        (WindowSide.LEFT, _START - _ONE_US, -1),
        (WindowSide.LEFT, _START - _PERIOD, -1),
        (WindowSide.LEFT, _START - _PERIOD - _ONE_US, -2),
        (WindowSide.LEFT, _START - 5 * _PERIOD / 2, -3),
        # Closed on the right: (start, end]
        (WindowSide.RIGHT, _START, -1),
        (WindowSide.RIGHT, _START + _ONE_US, 0),
        (WindowSide.RIGHT, _START + _PERIOD, 0),
        (WindowSide.RIGHT, _START + _PERIOD + _ONE_US, 1),
        (WindowSide.RIGHT, _START - _ONE_US, -1),
        (WindowSide.RIGHT, _START - _PERIOD, -2),
        (WindowSide.RIGHT, _START - _PERIOD + _ONE_US, -1),
        (WindowSide.RIGHT, _START - 5 * _PERIOD / 2, -3),
    ],
)
def test_context_window_offset(
    closed: WindowSide, timestamp: datetime, expected_offset: int
) -> None:
    """Test finding the window of a timestamp, including on boundaries."""
    context = _context(ResamplerConfig3(_PERIOD, closed=closed))

    offset = context.window_offset(timestamp)

    assert offset == expected_offset
    start, end = context.window_bounds(offset)
    match closed:
        case WindowSide.LEFT:
            assert start <= timestamp < end
        case WindowSide.RIGHT:
            assert start < timestamp <= end
