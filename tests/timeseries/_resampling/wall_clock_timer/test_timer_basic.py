# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Basic tests for `WallClockTimer`."""

import re
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from frequenz.sdk.timeseries._resampling._wall_clock_timer import (
    WallClockTimer,
    WallClockTimerConfig,
)

from .util import to_seconds, wall_now

pytestmark = pytest.mark.usefixtures("datetime_mock")


@pytest.mark.parametrize(
    "interval",
    [timedelta(seconds=0.0), timedelta(seconds=-0.01)],
    ids=["zero", "negative"],
)
def test_invalid_interval(interval: timedelta) -> None:
    """Test WallClockTimer with invalid intervals raises ValueError."""
    with pytest.raises(
        ValueError,
        match=rf"^interval must be positive, not {re.escape(str(interval))}$",
    ):
        _ = WallClockTimer(interval)


def test_custom_config() -> None:
    """Test WallClockTimer with a custom configuration."""
    interval = timedelta(seconds=5)
    config = MagicMock(name="config", spec=WallClockTimerConfig)

    timer = WallClockTimer(interval, config=config, auto_start=False)
    assert timer.interval == interval
    assert timer.config is config


def test_auto_start_default() -> None:
    """Test WallClockTimer uses auto_start=True by default."""
    interval = timedelta(seconds=1)
    timer = WallClockTimer(interval)
    assert timer.interval == interval
    assert timer.config == WallClockTimerConfig.from_interval(interval)
    assert timer.is_running
    assert timer.next_tick_time == wall_now() + interval


def test_auto_start_disabled() -> None:
    """Test WallClockTimer does not start when auto_start=False."""
    interval = timedelta(seconds=1)
    timer = WallClockTimer(interval, auto_start=False)
    assert timer.interval == interval
    assert timer.config == WallClockTimerConfig.from_interval(interval)
    assert not timer.is_running
    assert timer.next_tick_time is None


def test_reset() -> None:
    """Test WallClockTimer.reset() starts the timer and sets next_tick_time relative to now."""
    interval = timedelta(seconds=3)
    timer = WallClockTimer(interval, auto_start=False)
    timer.reset()
    assert timer.is_running
    assert timer.next_tick_time is not None
    assert to_seconds(timer.next_tick_time) == pytest.approx(
        to_seconds(wall_now() + interval)
    )


def test_close() -> None:
    """Test WallClockTimer.close() stops the timer and returns no next_tick_time."""
    interval = timedelta(seconds=2)
    timer = WallClockTimer(interval, auto_start=True)
    timer.close()
    assert not timer.is_running
    assert timer.next_tick_time is None


def test_str() -> None:
    """Test __str__ returns only the interval."""
    interval = timedelta(seconds=4)
    timer = WallClockTimer(interval, auto_start=False)
    assert str(timer) == f"WallClockTimer({interval})"


def test_repr() -> None:
    """Test __repr__ includes interval and running state."""
    interval = timedelta(seconds=4)
    timer = WallClockTimer(interval, auto_start=False)
    assert repr(timer) == f"WallClockTimer<interval={interval!r}, is_running=False>"
    timer.reset()
    assert (
        repr(timer) == f"WallClockTimer<interval={interval!r}, is_running=True, "
        f"next_tick_time={timer.next_tick_time!r}>"
    )
