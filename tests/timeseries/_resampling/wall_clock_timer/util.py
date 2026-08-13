# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Utility functions for testing the wall clock timer."""

import asyncio
import logging
import re
from collections.abc import Coroutine, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple, TypeVar, assert_never, overload
from unittest.mock import MagicMock

import pytest
from typing_extensions import override

from frequenz.sdk.timeseries import ClocksInfo, TickInfo

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")


@overload
def to_seconds(value: datetime | timedelta | float) -> float: ...


@overload
def to_seconds(value: None) -> None: ...


def to_seconds(value: datetime | timedelta | float | None) -> float | None:
    """Convert a datetime, timedelta, or float to seconds."""
    match value:
        case datetime():
            return value.timestamp()
        case timedelta():
            return value.total_seconds()
        case float() | int() | None:
            return value
        case unexpected:
            assert_never(unexpected)


def timestamp(ts: datetime | float, /) -> datetime:
    """Convert a timestamp in seconds since the epoch to a UTC datetime."""
    if isinstance(ts, datetime):
        return ts
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def delta(sec: float | timedelta, /) -> timedelta:
    """Create a timedelta from seconds."""
    if isinstance(sec, timedelta):
        return sec
    return timedelta(seconds=sec)


# This is needed to work with the datetime_mock fixture in conftest.py
def wall_now() -> datetime:
    """Get the current wall clock time from the mocked datetime in the target module."""
    # Disable isort formatting because it wants to put the ignore in the wrong line
    # We now also have to ignore the maximum line length (E501)
    # isort: off
    # pylint: disable-next=import-outside-toplevel
    from frequenz.sdk.timeseries._resampling._wall_clock_timer import (  # type: ignore[attr-defined] # noqa: E501
        datetime as mock_datetime,
    )

    # isort: on

    return mock_datetime.now(timezone.utc)


def mono_now() -> float:
    """Get the current monotonic time."""
    return asyncio.get_running_loop().time()


def approx_time(
    expected: datetime | timedelta,
    *,
    abs: timedelta = timedelta(milliseconds=1),  # pylint: disable=redefined-builtin
) -> Any:
    """Perform approximate comparisons for datetime or timedelta objects.

    This is only a thin wrapper around `pytest.approx()` to default the tolerance to
    1ms, as `pytest.approx()` requires an explicit tolerance for these types.

    Args:
        expected: The expected `datetime` or `timedelta` to compare against.
        abs: The absolute tolerance as a `timedelta`. Defaults to 1ms.

    Returns:
        An object comparing equal to any value within `abs` of `expected`.
    """
    return pytest.approx(expected, abs=abs)


# We need to rewrite most of the attributes in these classes to use approximate
# comparisons. This means if a new field is added, we need to update the
# approx_tick_info function below to handle the new fields. To catch this, we do
# a sanity check here, so if new fields are added we get an early warning instead of
# getting tests errors because of some rounding error in a newly added field.
assert set(ClocksInfo.__dataclass_fields__.keys()) == {
    "monotonic_requested_sleep",
    "monotonic_time",
    "wall_clock_time",
    "monotonic_elapsed",
    "wall_clock_elapsed",
    "wall_clock_factor",
}, "ClocksInfo fields were added or removed, please update the approx_tick_info function."
assert set(TickInfo.__dataclass_fields__.keys()) == {
    "expected_tick_time",
    "sleep_infos",
}, "TickInfo fields were added or removed, please update the approx_tick_info function."


def approx_tick_info(
    expected: TickInfo,
    *,
    abs: timedelta = timedelta(milliseconds=1),  # pylint: disable=redefined-builtin
) -> TickInfo:
    """Create a copy of a `TickInfo` object with approximate comparisons.

    Fields are replaced by approximate comparison objects (`approx_time` or
    `pytest.approx`).

    This version bypasses `__post_init__` to avoid validation `TypeError`s.

    Args:
        expected: The expected `TickInfo` object to compare against.
        abs: The absolute tolerance as a `timedelta` for all time-based
            comparisons. Defaults to 1ms.

    Returns:
        A new `TickInfo` instance ready for approximate comparison.
    """
    abs_s = abs.total_seconds()
    approx_sleeps = []
    for s_info in expected.sleep_infos:
        # HACK: Create a blank instance to bypass __init__ and __post_init__.
        # This prevents the TypeError from the validation logic.
        approx_s = object.__new__(ClocksInfo)

        # Use object.__setattr__ to assign fields to the frozen instance.
        object.__setattr__(
            approx_s,
            "monotonic_requested_sleep",
            approx_time(s_info.monotonic_requested_sleep, abs=abs),
        )
        # Use the standard pytest.approx for float values
        object.__setattr__(
            approx_s, "monotonic_time", pytest.approx(s_info.monotonic_time, abs=abs_s)
        )
        object.__setattr__(
            approx_s, "wall_clock_time", approx_time(s_info.wall_clock_time, abs=abs)
        )
        object.__setattr__(
            approx_s,
            "monotonic_elapsed",
            approx_time(s_info.monotonic_elapsed, abs=abs),
        )
        object.__setattr__(
            approx_s,
            "wall_clock_elapsed",
            approx_time(s_info.wall_clock_elapsed, abs=abs),
        )
        if s_info.wall_clock_factor is not None:
            object.__setattr__(
                approx_s,
                "wall_clock_factor",
                pytest.approx(s_info.wall_clock_factor, abs=abs_s),
            )
        approx_sleeps.append(approx_s)

    # Do the same for the top-level frozen TickInfo object
    approx_tick_info = object.__new__(TickInfo)
    object.__setattr__(
        approx_tick_info,
        "expected_tick_time",
        approx_time(expected.expected_tick_time, abs=abs),
    )
    object.__setattr__(approx_tick_info, "sleep_infos", approx_sleeps)

    return approx_tick_info


class matches_re:  # pylint: disable=invalid-name
    """Assert that a given string (or string representation) matches a regex pattern."""

    def __init__(self, pattern: str, flags: int = 0) -> None:
        """Initialize with a regex pattern and optional flags."""
        self._regex = re.compile(pattern, flags)

    @override
    def __eq__(self, other: object) -> bool:
        """Check if the string representation of `other` matches the regex pattern."""
        return bool(self._regex.match(str(other)))

    @override
    def __repr__(self) -> str:
        """Return a string representation of this instance."""
        return self._regex.pattern


class Adjustment(NamedTuple):
    """A time adjustment to be applied at a specific monotonic time."""

    mono_delta: timedelta | float
    """The monotonic time delta to sleep before adjusting the wall clock time, in seconds."""

    wall_time: datetime | float
    """The new wall clock time to set at that monotonic time, in seconds since the epoch in UTC."""


@dataclass(kw_only=True, frozen=True)
class TimeDriver:
    """A utility for driving the wall and monotonic clocks in tests.

    This class encapsulates the necessary mocks for `datetime.datetime` and
    provides methods to manipulate wall clock time during tests. This is particularly
    useful for testing components that rely on both wall clock time (which can be
    adjusted by the system) and monotonic time (which should always move forward).

    It is designed to be used as a pytest fixture, where `datetime_mock` is
    provided by another fixture.

    The main method to use in tests is `next_tick()`, which simulates time
    passing and wall clock adjustments while waiting for a timer tick.
    """

    datetime_mock: MagicMock
    """A mock for the `datetime` module."""

    loop: asyncio.AbstractEventLoop = field(default_factory=asyncio.get_event_loop)
    """The asyncio event loop."""

    mono_start: float = field(default_factory=mono_now)
    """The starting monotonic time of the test."""

    wall_start: datetime = field(default_factory=wall_now)
    """The starting wall clock time of the test."""

    def __post_init__(self) -> None:
        """Initialize the time driver by logging the start times."""
        _logger.debug(
            "Start: wall_now=%s, mono_now=%s", self.wall_start, self.mono_start
        )

    async def _shift_time(
        self,
        wall_delta: timedelta | float,
        *,
        mono_delta: timedelta | float | None = None,
    ) -> tuple[datetime, float]:
        """Advance the time by the given number of seconds.

        This advances both the wall clock and the time machine fake time.

        Args:
            wall_delta: The amount of time to advance the wall clock, in seconds or as a
                timedelta.
            mono_delta: The amount of time to advance the monotonic clock, in seconds or
                as a timedelta.  If None, it defaults to the same value as `wall_time`.

        Returns:
            A tuple containing the new wall clock time and the new monotonic time.
        """
        wall_delta = to_seconds(wall_delta)
        mono_delta = to_seconds(mono_delta)
        if mono_delta is None:
            mono_delta = wall_delta
        _logger.debug(
            "_shift_time(): wall_delta=%s, mono_delta=%s", wall_delta, mono_delta
        )

        wall_start = wall_now()
        mono_start = mono_now()

        _logger.debug(
            "_shift_time(): Before sleep: wall_now=%s, mono_now=%s",
            wall_start,
            mono_start,
        )
        await asyncio.sleep(mono_delta)
        self.datetime_mock.now.return_value = wall_now() + timedelta(seconds=wall_delta)
        _logger.debug(
            "_shift_time(): After shift: wall_now=%s, mono_now=%s",
            wall_now(),
            mono_now(),
        )

        new_wall_now = wall_now()
        new_mono_now = mono_now()
        _logger.debug(
            "NEW TIME: new_wall_now=%s, new_mono_now=%s", new_wall_now, new_mono_now
        )

        assert new_wall_now == approx_time(wall_start + delta(wall_delta))
        assert new_mono_now == pytest.approx(mono_start + mono_delta)

        return new_wall_now, new_mono_now

    def _update_wall_clock(self, new_time: datetime | float) -> None:
        """Update the wall clock time to the given datetime."""
        new_time = timestamp(new_time)
        _logger.debug(
            "_update_wall_clock(): at mono_now=%s %s -> %s (%s -> %s)",
            mono_now(),
            to_seconds(wall_now()),
            to_seconds(new_time),
            wall_now(),
            new_time,
        )
        self.datetime_mock.now.return_value = new_time
        _logger.debug(
            "_update_wall_clock(): wall clock updated to %s (%s)",
            to_seconds(wall_now()),
            wall_now(),
        )

    def _shift_wall_clock(self, shift_delta: timedelta | float) -> None:
        """Shift the wall clock by the given timedelta."""
        shift_delta = delta(shift_delta)
        new_wall_now = wall_now() + shift_delta
        self._update_wall_clock(new_wall_now)

    async def next_tick(
        self,
        next_tick_wall_times: Sequence[Adjustment],
        coro: Coroutine[None, None, _T],
    ) -> _T:
        """Wait for the next tick of the timer and return the result of the coroutine.

        This method simulates the passage of time and wall clock adjustments while a
        timer is waiting for its next tick. It runs a provided coroutine (like a
        timer's `receive()` or `ready()` method) and, while it's running, applies a
        series of time adjustments.

        This is useful for simulating wall clock jumps or drifts. The adjustments are
        applied after the timer has started waiting, ensuring the timer correctly
        observes the time change.

        Args:
            next_tick_wall_times: A sequence of `Adjustment` tuples. Each tuple
                specifies a monotonic time delta to wait before setting the wall
                clock to a new time. This simulates wall clock adjustments
                happening while the timer is sleeping.
            coro: The coroutine to run that will receive the next tick from the
                timer. Typically this will be the timer's `receive()` or `ready()`
                method.

        Returns:
            The result of the `coro` coroutine.
        """
        async with asyncio.TaskGroup() as tg:
            _logger.debug("_next_tick(): Creating timer task for receive()")
            timer_task: asyncio.Task[_T] = tg.create_task(coro)
            for adj in next_tick_wall_times:
                sleep_time = to_seconds(adj.mono_delta)
                wall_time = to_seconds(adj.wall_time)
                _logger.debug(
                    "_next_tick(): Waiting for %s seconds before setting wall clock to %s",
                    sleep_time,
                    wall_time,
                )
                await asyncio.sleep(sleep_time)
                assert not timer_task.done()
                self._update_wall_clock(adj.wall_time)
                _logger.debug(
                    "_next_tick(): After setting wall clock: now_wall=%s, now_mono=%s",
                    to_seconds(wall_now()),
                    mono_now(),
                )
            return await timer_task
