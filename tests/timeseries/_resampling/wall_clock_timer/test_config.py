# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the `WallClockTimerConfig` class."""

import math
import re
from datetime import datetime, timedelta, timezone

import pytest
from frequenz.core.datetime import UNIX_EPOCH

from frequenz.sdk.timeseries._resampling._wall_clock_timer import WallClockTimerConfig


def test_from_interval_defaults() -> None:
    """Test WallClockTimerConfig.from_interval() with only interval (all defaults)."""
    interval = timedelta(seconds=10)
    config = WallClockTimerConfig.from_interval(interval)
    assert config.align_to == UNIX_EPOCH
    assert config.async_drift_tolerance == pytest.approx(timedelta(seconds=1.0))
    assert config.wall_clock_drift_tolerance_factor == pytest.approx(0.1)
    assert config.wall_clock_jump_threshold == pytest.approx(timedelta(seconds=10.0))


def test_from_interval_all_args() -> None:
    """Test WallClockTimerConfig.from_interval() with all arguments provided."""
    interval = timedelta(seconds=5)
    align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
    async_factor = 0.2
    wall_factor = 0.3
    jump_factor = 0.4
    config = WallClockTimerConfig.from_interval(
        interval,
        align_to=align_to,
        async_drift_tolerance_factor=async_factor,
        wall_clock_drift_tolerance_factor=wall_factor,
        wall_clock_jump_threshold_factor=jump_factor,
    )
    assert config.align_to == align_to
    assert config.async_drift_tolerance == pytest.approx(timedelta(seconds=1.0))
    assert config.wall_clock_drift_tolerance_factor == pytest.approx(0.3)
    assert config.wall_clock_jump_threshold == pytest.approx(timedelta(seconds=2.0))


@pytest.mark.parametrize(
    "interval", [timedelta(seconds=0), timedelta(seconds=-1)], ids=str
)
def test_from_interval_invalid(interval: timedelta) -> None:
    """Test WallClockTimerConfig.from_interval() with invalid interval raises ValueError."""
    with pytest.raises(ValueError, match=r"^interval must be bigger than 0, not "):
        WallClockTimerConfig.from_interval(interval)


def test_trivial_defaults() -> None:
    """Test that WallClockTimerConfig can be constructed with all defaults."""
    config = WallClockTimerConfig()
    assert config.align_to == UNIX_EPOCH
    assert config.async_drift_tolerance is None
    assert config.wall_clock_drift_tolerance_factor is None
    assert config.wall_clock_jump_threshold is None


def test_all_valid_arguments() -> None:
    """Test that WallClockTimerConfig can be constructed with all valid arguments."""
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    async_drift_tolerance = timedelta(seconds=5)
    wall_clock_drift_tolerance_factor = 0.5
    wall_clock_jump_threshold = timedelta(seconds=10)
    config = WallClockTimerConfig(
        align_to=align_to,
        async_drift_tolerance=async_drift_tolerance,
        wall_clock_drift_tolerance_factor=wall_clock_drift_tolerance_factor,
        wall_clock_jump_threshold=wall_clock_jump_threshold,
    )
    assert config.align_to == align_to
    assert config.async_drift_tolerance == async_drift_tolerance
    assert config.wall_clock_drift_tolerance_factor == wall_clock_drift_tolerance_factor
    assert config.wall_clock_jump_threshold == wall_clock_jump_threshold


@pytest.mark.parametrize(
    "align_to",
    [None, datetime(2020, 1, 1, tzinfo=timezone.utc)],
    ids=str,
)
def test_valid_align_to(align_to: datetime | None) -> None:
    """Test that align_to is accepted and set for valid input."""
    config = WallClockTimerConfig(align_to=align_to)
    assert config.align_to == align_to


def test_align_to_timezone_unaware() -> None:
    """Test checks on the resampling buffer."""
    with pytest.raises(
        ValueError, match=r"^align_to (.*) should be a timezone aware datetime$"
    ):
        # Ignore the timezone-aware flake8 check because we want to validate it at runtime
        _ = WallClockTimerConfig(
            align_to=datetime(2020, 1, 1, tzinfo=None)  # noqa: DTZ001
        )


_VALID_NUMBERS = [
    0.1,
    1.0,
    None,
]
_VALID_TIMEDELTAS = [
    *(timedelta(seconds=v) for v in _VALID_NUMBERS if v is not None),
    None,
]

_INVALID_NUMBERS = [
    -0.0001,
    0.0,
    -1,
    0,
    float("inf"),
    float("-inf"),
    float("nan"),
]

_INVALID_TIMEDELTAS = [
    *(timedelta(seconds=v) for v in _INVALID_NUMBERS if math.isfinite(v)),
]


@pytest.mark.parametrize("value", _VALID_TIMEDELTAS, ids=str)
def test_valid_async_drift_tolerance(value: timedelta | None) -> None:
    """Test that async_drift_tolerance accepts valid values."""
    config = WallClockTimerConfig(async_drift_tolerance=value)
    assert config.async_drift_tolerance == value


@pytest.mark.parametrize("value", _INVALID_TIMEDELTAS, ids=str)
def test_invalid_async_drift_tolerance(value: timedelta | None) -> None:
    """Test that strictly positive fields reject invalid values (matrix)."""
    with pytest.raises(
        ValueError,
        match=rf"^async_drift_tolerance should be positive or None, not {re.escape(repr(value))}$",
    ):
        _ = WallClockTimerConfig(async_drift_tolerance=value)


@pytest.mark.parametrize("value", _VALID_TIMEDELTAS, ids=str)
def test_valid_wall_clock_jump_threshold(
    value: timedelta | None,
) -> None:
    """Test that wall_clock_jump_threshold accepts valid values."""
    config = WallClockTimerConfig(wall_clock_jump_threshold=value)
    assert config.wall_clock_jump_threshold == value


@pytest.mark.parametrize("value", _INVALID_TIMEDELTAS, ids=str)
def test_invalid_wall_clock_jump_threshold(
    value: timedelta | None,
) -> None:
    """Test that strictly positive fields reject invalid values (matrix)."""
    with pytest.raises(
        ValueError,
        match=r"^wall_clock_jump_threshold should be positive or None, not "
        + re.escape(repr(value))
        + r"$",
    ):
        _ = WallClockTimerConfig(wall_clock_jump_threshold=value)


@pytest.mark.parametrize("value", [0.1, 1.0, 1, None])
def test_valid_wall_clock_drift_tolerance_factor(
    value: float | None,
) -> None:
    """Test that strictly positive fields accept valid values."""
    config = WallClockTimerConfig(wall_clock_drift_tolerance_factor=value)
    assert config.wall_clock_drift_tolerance_factor == value


@pytest.mark.parametrize("value", _INVALID_NUMBERS, ids=str)
def test_invalid_wall_clock_drift_tolerance_factor(
    value: float | None,
) -> None:
    """Test that strictly positive fields reject invalid values (matrix)."""
    with pytest.raises(
        ValueError,
        match=r"^wall_clock_drift_tolerance_factor should be positive or None, not "
        + re.escape(repr(value))
        + r"$",
    ):
        _ = WallClockTimerConfig(wall_clock_drift_tolerance_factor=value)
