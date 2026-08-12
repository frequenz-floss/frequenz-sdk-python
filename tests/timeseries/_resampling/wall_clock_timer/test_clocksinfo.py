# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the `ClocksInfo` class."""

import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from frequenz.sdk.timeseries._resampling._wall_clock_timer import ClocksInfo

from .util import approx_time

_DEFAULT_MONOTONIC_REQUESTED_SLEEP = timedelta(seconds=1.0)
_DEFAULT_MONOTONIC_TIME = 1234.5
_DEFAULT_WALL_CLOCK_TIME = datetime(2023, 1, 1, tzinfo=timezone.utc)
_DEFAULT_MONOTONIC_ELAPSED = timedelta(seconds=1.1)
_DEFAULT_WALL_CLOCK_ELAPSED = timedelta(seconds=1.2)


@pytest.mark.parametrize(
    "elapsed",
    [timedelta(seconds=0), timedelta(seconds=-1.0)],
    ids=["zero", "negative"],
)
def test_monotonic_requested_sleep_invalid(elapsed: timedelta) -> None:
    """Test monotonic_requested_sleep with invalid values."""
    with pytest.raises(
        ValueError,
        match=r"^monotonic_requested_sleep must be strictly positive, not "
        + re.escape(repr(elapsed))
        + r"$",
    ):
        _ = ClocksInfo(
            monotonic_requested_sleep=elapsed,
            monotonic_time=_DEFAULT_MONOTONIC_TIME,
            wall_clock_time=_DEFAULT_WALL_CLOCK_TIME,
            monotonic_elapsed=_DEFAULT_MONOTONIC_ELAPSED,
            wall_clock_elapsed=_DEFAULT_WALL_CLOCK_ELAPSED,
        )


@pytest.mark.parametrize(
    "time",
    [float("-inf"), float("nan"), float("inf")],
)
def test_monotonic_time_invalid(time: float) -> None:
    """Test monotonic_time with invalid values."""
    with pytest.raises(
        ValueError,
        match=rf"^monotonic_time must be a number, not {re.escape(repr(time))}$",
    ):
        _ = ClocksInfo(
            monotonic_requested_sleep=_DEFAULT_MONOTONIC_REQUESTED_SLEEP,
            monotonic_time=time,
            wall_clock_time=_DEFAULT_WALL_CLOCK_TIME,
            monotonic_elapsed=_DEFAULT_MONOTONIC_ELAPSED,
            wall_clock_elapsed=_DEFAULT_WALL_CLOCK_ELAPSED,
        )


@pytest.mark.parametrize(
    "elapsed",
    [timedelta(seconds=0), timedelta(seconds=-1.0)],
    ids=["zero", "negative"],
)
def test_monotonic_elapsed_invalid(elapsed: timedelta) -> None:
    """Test monotonic_elapsed with invalid values."""
    with pytest.raises(
        ValueError,
        match="^monotonic_elapsed must be strictly positive, not "
        rf"{re.escape(repr(elapsed))}$",
    ):
        _ = ClocksInfo(
            monotonic_requested_sleep=_DEFAULT_MONOTONIC_REQUESTED_SLEEP,
            monotonic_time=_DEFAULT_MONOTONIC_TIME,
            wall_clock_time=_DEFAULT_WALL_CLOCK_TIME,
            monotonic_elapsed=elapsed,
            wall_clock_elapsed=_DEFAULT_WALL_CLOCK_ELAPSED,
        )


@pytest.mark.parametrize("wall_clock_factor", [float("nan"), 2.3])
def test_clocks_info_construction(wall_clock_factor: float) -> None:
    """Test that ClocksInfo can be constructed and attributes are set correctly."""
    monotonic_requested_sleep = timedelta(seconds=1.0)
    monotonic_time = 1234.5
    wall_clock_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
    monotonic_elapsed = timedelta(seconds=1.1)
    wall_clock_elapsed = timedelta(seconds=1.2)

    info = ClocksInfo(
        monotonic_requested_sleep=monotonic_requested_sleep,
        monotonic_time=monotonic_time,
        wall_clock_time=wall_clock_time,
        monotonic_elapsed=monotonic_elapsed,
        wall_clock_elapsed=wall_clock_elapsed,
        wall_clock_factor=wall_clock_factor,
    )

    assert info.monotonic_requested_sleep == monotonic_requested_sleep
    assert info.monotonic_time == monotonic_time
    assert info.wall_clock_time == wall_clock_time
    assert info.monotonic_elapsed == monotonic_elapsed
    assert info.wall_clock_elapsed == wall_clock_elapsed

    # Check in particular that using nan explicitly is the same as using the default
    # We test how the default is calculated in another test
    if math.isnan(wall_clock_factor):
        assert info == ClocksInfo(
            monotonic_requested_sleep=monotonic_requested_sleep,
            monotonic_time=monotonic_time,
            wall_clock_time=wall_clock_time,
            monotonic_elapsed=monotonic_elapsed,
            wall_clock_elapsed=wall_clock_elapsed,
        )
    else:
        assert info.wall_clock_factor == wall_clock_factor


@pytest.mark.parametrize(
    "requested_sleep, monotonic_elapsed, expected_drift",
    [
        (timedelta(seconds=1.0), timedelta(seconds=1.1), timedelta(seconds=0.1)),
        (timedelta(seconds=1.0), timedelta(seconds=0.9), timedelta(seconds=-0.1)),
        (timedelta(seconds=1.0), timedelta(seconds=1.0), timedelta(seconds=0.0)),
    ],
    ids=["positive", "negative", "no_drift"],
)
def test_monotonic_drift(
    requested_sleep: timedelta,
    monotonic_elapsed: timedelta,
    expected_drift: timedelta,
) -> None:
    """Test the monotonic_drift property."""
    info = ClocksInfo(
        monotonic_requested_sleep=requested_sleep,
        monotonic_time=_DEFAULT_MONOTONIC_TIME,
        wall_clock_time=_DEFAULT_WALL_CLOCK_TIME,
        monotonic_elapsed=monotonic_elapsed,
        wall_clock_elapsed=_DEFAULT_WALL_CLOCK_ELAPSED,
    )
    assert info.monotonic_drift == approx_time(expected_drift)


@pytest.mark.parametrize(
    "wall_clock_elapsed, monotonic_elapsed, expected_jump",
    [
        (timedelta(seconds=1.0), timedelta(seconds=2.1), timedelta(seconds=-1.1)),
        (timedelta(seconds=1.0), timedelta(seconds=0.19), timedelta(seconds=0.81)),
        (timedelta(seconds=1.0), timedelta(seconds=1.0), timedelta(seconds=0.0)),
    ],
    ids=["positive", "negative", "no_jump"],
)
def test_wall_clock_jump(
    wall_clock_elapsed: timedelta,
    monotonic_elapsed: timedelta,
    expected_jump: timedelta,
) -> None:
    """Test the wall_clock_jump property."""
    info = ClocksInfo(
        monotonic_requested_sleep=_DEFAULT_MONOTONIC_REQUESTED_SLEEP,
        monotonic_time=_DEFAULT_MONOTONIC_TIME,
        wall_clock_time=_DEFAULT_WALL_CLOCK_TIME,
        monotonic_elapsed=monotonic_elapsed,
        wall_clock_elapsed=wall_clock_elapsed,
    )
    assert info.wall_clock_jump == approx_time(expected_jump)


@dataclass(kw_only=True, frozen=True)
class _TestCaseWallClockFactor:
    """Test case for wall clock factor calculation."""

    id: str
    monotonic_elapsed: timedelta
    wall_clock_elapsed: timedelta
    expected_factor: float


@pytest.mark.parametrize(
    "case",
    [
        _TestCaseWallClockFactor(
            id="wall_faster",
            monotonic_elapsed=timedelta(seconds=1.0),
            wall_clock_elapsed=timedelta(seconds=1.1),
            expected_factor=0.9090909090909091,
        ),
        _TestCaseWallClockFactor(
            id="wall_slower",
            monotonic_elapsed=timedelta(seconds=1.0),
            wall_clock_elapsed=timedelta(seconds=0.9),
            expected_factor=1.11111111111111,
        ),
        _TestCaseWallClockFactor(
            id="in_sync",
            monotonic_elapsed=timedelta(seconds=1.0),
            wall_clock_elapsed=timedelta(seconds=1.0),
            expected_factor=1.0,
        ),
        _TestCaseWallClockFactor(
            id="wall_twice_as_fast",
            monotonic_elapsed=timedelta(seconds=0.5),
            wall_clock_elapsed=timedelta(seconds=1.0),
            expected_factor=0.5,
        ),
    ],
    ids=lambda case: case.id,
)
def test_wall_clock_factor(case: _TestCaseWallClockFactor) -> None:
    """Test the calculate_wall_clock_factor method with valid inputs."""
    info = ClocksInfo(
        monotonic_requested_sleep=_DEFAULT_MONOTONIC_REQUESTED_SLEEP,
        monotonic_time=_DEFAULT_MONOTONIC_TIME,
        wall_clock_time=_DEFAULT_WALL_CLOCK_TIME,
        monotonic_elapsed=case.monotonic_elapsed,
        wall_clock_elapsed=case.wall_clock_elapsed,
    )
    assert info.wall_clock_factor == pytest.approx(case.expected_factor)
    assert info.wall_clock_to_monotonic(case.wall_clock_elapsed) == approx_time(
        case.monotonic_elapsed
    )


@pytest.mark.parametrize(
    "elapsed",
    [timedelta(seconds=0), timedelta(seconds=-1.0)],
    ids=["zero", "negative"],
)
def test_wall_clock_factor_invalid_wall_clock_elapsed(
    elapsed: timedelta, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a warning is logged when wall_clock_elapsed is zero."""
    expected_log = (
        "The monotonic clock advanced 0:00:01, but the wall clock "
        f"stayed still or jumped back (elapsed: {elapsed})!"
    )
    with caplog.at_level("WARNING"):
        info = ClocksInfo(
            monotonic_requested_sleep=_DEFAULT_MONOTONIC_REQUESTED_SLEEP,
            monotonic_time=_DEFAULT_MONOTONIC_TIME,
            wall_clock_time=_DEFAULT_WALL_CLOCK_TIME,
            monotonic_elapsed=timedelta(seconds=1.0),
            wall_clock_elapsed=elapsed,
        )
        assert info.wall_clock_to_monotonic(timedelta(seconds=1.0)) == timedelta(
            seconds=10.0
        )
        assert info.wall_clock_factor == 10.0

    assert expected_log in caplog.text
