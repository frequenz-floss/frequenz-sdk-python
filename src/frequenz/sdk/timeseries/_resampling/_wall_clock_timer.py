# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries resampler."""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Self, assert_never

from frequenz.channels import Receiver, ReceiverStoppedError
from typing_extensions import override

from .._base_types import UNIX_EPOCH

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class WallClockTimerConfig:
    """Configuration for a wall clock timer."""

    align_to: datetime | None = UNIX_EPOCH
    """The time to align the timer to.

    The first timer tick will occur at the first multiple of the [`interval`][] after
    this value.

    It must be a timezone aware `datetime` or `None`. If `None`, the timer aligns to the
    time it was is started.
    """

    async_drift_tolerance: timedelta | None = None
    """The maximum allowed difference between the requested and the real sleep time.

    The timer will emit a warning if the difference is bigger than this value.

    It must be bigger than 0 or `None`. If `None`, no warnings will ever be emitted.
    """

    wall_clock_drift_tolerance_factor: float | None = None
    """The maximum allowed relative difference between the wall clock and monotonic time.

    The timer will emit a warning if the relative difference is bigger than this value.
    If the difference remains constant, the warning will be emitted only once, as the
    previous drift is taken into account. If there is information on the previous drift,
    the previous and current factor will be used to determine if a warning should be
    emitted.

    It must be bigger than 0 or `None`. If `None`, no warnings will be ever emitted.

    Info:
        The calculation is as follows:

        ```
        tolerance = wall_clock_drift_tolerance_factor
        factor = monotonic_elapsed / wall_clock_elapsed
        previous_factor = previous_monotonic_elapsed / previous_wall_clock_elapsed
        if abs(factor - previous_factor) > tolerance:
            emit warning
        ```

        If there is no previous information, a `previous_factor` of 1.0 will be used.
    """

    wall_clock_jump_threshold: timedelta | None = None
    """The amount of time that's considered a wall clock jump.

    When the drift between the wall clock and monotonic time is too big, it is
    considered a time jump and the timer will be resynced to the wall clock.

    This value determines how big the difference needs to be to be considered a
    jump.

    Smaller values are considered wall clock *expansions* or *compressions* and are
    always gradually adjusted, instead of triggering a resync.

    Must be bigger than 0 or `None`. If `None`, a resync will never be triggered due to
    time jumps.
    """

    def __post_init__(self) -> None:
        """Check that config values are valid.

        Raises:
            ValueError: If any value is out of range.
        """
        if self.align_to is not None and self.align_to.tzinfo is None:
            raise ValueError(
                f"align_to ({self.align_to}) should be a timezone aware datetime"
            )

        def _is_strictly_positive_or_none(value: float | timedelta | None) -> bool:
            match value:
                case None:
                    return True
                case timedelta() as delta:
                    return delta > timedelta(0)
                case float() as num:
                    return math.isfinite(num) and num > 0.0
                case int() as num:
                    return num > 0
                case _ as unknown:
                    assert_never(unknown)

        if not _is_strictly_positive_or_none(self.async_drift_tolerance):
            raise ValueError(
                "async_drift_tolerance should be positive or None, not "
                f"{self.async_drift_tolerance!r}"
            )
        if not _is_strictly_positive_or_none(self.wall_clock_drift_tolerance_factor):
            raise ValueError(
                "wall_clock_drift_tolerance_factor should be positive or None, not "
                f"{self.wall_clock_drift_tolerance_factor!r}"
            )
        if not _is_strictly_positive_or_none(self.wall_clock_jump_threshold):
            raise ValueError(
                "wall_clock_jump_threshold should be positive or None, not "
                f"{self.wall_clock_jump_threshold!r}"
            )

    @classmethod
    def from_interval(  # pylint: disable=too-many-arguments
        cls,
        interval: timedelta,
        *,
        align_to: datetime | None = UNIX_EPOCH,
        async_drift_tolerance_factor: float = 0.1,
        wall_clock_drift_tolerance_factor: float = 0.1,
        wall_clock_jump_threshold_factor: float = 1.0,
    ) -> Self:
        """Create a timer configuration based on an interval.

        This will set the tolerance and threshold values proportionally to the interval.

        Args:
            interval: The interval between timer ticks. Must be bigger than 0.
            align_to: The time to align the timer to. See the
                [class documentation][frequenz.sdk.timeseries.WallClockTimer]
                for details.
            async_drift_tolerance_factor: The maximum allowed difference between the
                requested and the real sleep time. See the
                [class documentation][frequenz.sdk.timeseries.WallClockTimer]
                for details.
            wall_clock_drift_tolerance_factor: The maximum allowed difference between
                the wall clock and monotonic time.. See the
                [class documentation][frequenz.sdk.timeseries.WallClockTimer]
                for details.
            wall_clock_jump_threshold_factor: The amount of time that's considered a
                wall clock jump.. See the
                [class documentation][frequenz.sdk.timeseries.WallClockTimer]
                for details.

        Returns:
            The created timer configuration.

        Raises:
            ValueError: If any value is out of range.
        """
        if interval <= timedelta(0):
            raise ValueError(f"interval must be bigger than 0, not {interval!r}")

        return cls(
            align_to=align_to,
            wall_clock_drift_tolerance_factor=wall_clock_drift_tolerance_factor,
            async_drift_tolerance=interval * async_drift_tolerance_factor,
            wall_clock_jump_threshold=interval * wall_clock_jump_threshold_factor,
        )


@dataclass(frozen=True, kw_only=True)
class ClocksInfo:
    """Information about the wall clock and monotonic clock and their drift."""

    monotonic_requested_sleep: timedelta
    """The requested monotonic sleep time used to gather information about the clocks."""

    monotonic_time: float
    """The current monotonic time when the drift was calculated."""

    wall_clock_time: datetime
    """The current wall clock time when the drift was calculated."""

    monotonic_elapsed: timedelta
    """The elapsed time in monotonic time."""

    wall_clock_elapsed: timedelta
    """The elapsed time in wall clock time."""

    @property
    def monotonic_drift(self) -> timedelta:
        """The difference between the monotonic elapsed and requested sleep time.

        This number should be always positive, as the monotonic time should never
        jump back in time.
        """
        return self.monotonic_elapsed - self.monotonic_requested_sleep

    def calculate_wall_clock_factor(self) -> float:
        """Calculate the factor to convert wall clock time to monotonic time.

        If the wall clock time expanded compared to the monotonic time (i.e. is more in
        the future), the returned value will be bigger than 1. If the wall clock time
        compressed compared to the monotonic time (i.e. is more in the past), the
        returned value will be smaller than 1.

        Returns:
            The factor to convert wall clock time to monotonic time.
        """
        wall_clock_elapsed = self.wall_clock_elapsed
        if not wall_clock_elapsed:
            _logger.warning(
                "The monotonic clock advanced %s, but the wall clock didn't move. "
                "Hopefully this was just a singular jump in time and not a "
                "permanent issue with the wall clock not moving at all. For purposes "
                "of calculating the wall clock factor, a fake elapsed time of one "
                "tenth of the elapsed monotonic time will be used.",
            )
            wall_clock_elapsed = self.monotonic_elapsed * 0.01
        return self.monotonic_elapsed / wall_clock_elapsed

    @property
    def wall_clock_jump(self) -> timedelta:
        """The amount of time the wall clock jumped compared to the monotonic time.

        If the wall clock time jumped forward compared to the monotonic time, the
        returned value will be positive. If the wall clock time jumped backwards
        compared to the monotonic time, the returned value will be negative.

        Note:
            Strictly speaking, both could be in sync and the result would be 0.0, but
            this is extremely unlikely due to floating point precision and the fact
            that both clocks are obtained as slightly different times.
        """
        return self.wall_clock_elapsed - self.monotonic_elapsed

    def wall_clock_to_monotonic(self, wall_clock_timedelta: timedelta, /) -> timedelta:
        """Convert a wall clock timetimedelta to a monotonic timedelta.

        Args:
            wall_clock_timedelta: The wall clock timedelta to convert.

        Returns:
            The monotonic time corresponding to `wall_clock_time`.
        """
        return wall_clock_timedelta * self.calculate_wall_clock_factor()


@dataclass(frozen=True, kw_only=True)
class TimerInfo:
    """Information about a `WallClockTimer` tick."""

    expected_tick_time: datetime
    """The expected time when the timer should have triggered."""

    clocks_info: ClocksInfo | None = None
    """The information about the clocks and their drift for this tick.

    If the timer didn't have do to a [`sleep()`][asyncio.sleep] to trigger the tick
    (i.e. the timer is catching up because there were big drifts in previous ticks),
    this will be `None`.
    """


class WallClockTimer(Receiver[TimerInfo]):
    """A timer attached to the wall clock.

    This timer uses the wall clock to trigger ticks and deals with differences between
    the wall clock and monotonic time, as time has to be done in monotonic time, which
    could sometimes drift from the wall clock.

    When the difference is small, we say that the wall clock is *compressed* when the
    wall clock time is in the past compared to the monotonic time (the wall clock time
    passes slower than the monotonic time), and *expanded* when the wall clock time is
    in the future compared to the monotonic time (the wall clock time passes faster
    than the monotonic time).

    If the compression or expansion is big, a warning will be emitted. The definition of
    *big* is controlled by the `wall_clock_drift_tolerance_factor` configuration.

    When the difference is **too big** it is considered a *time jump*, and the timer
    will be resynced to the wall clock time and a tick will be triggered immediately.
    Time jumps can happen when the wall clock is synced by NTP after a long time being
    offline.  The definition of *too big* is controlled by the
    `wall_clock_jump_threshold` configuration.

    The ticks are aligned to the `align_to` configuration, even in the event of time
    jumps, the alignment will be preserved.

    The timer will also emit warnings if the requested sleep time is different from the
    real sleep time. This can happen if the event loop is blocked for too long, or if
    the system is under heavy load. The definition of *too long* is controlled by the
    `async_drift_tolerance` configuration.

    Because of the complexities of dealing with time jumps, compression and expansion,
    the timer returns a `TimerInfo` on each tick, including information about the clocks
    and their drift.
    """

    def __init__(
        self,
        interval: timedelta,
        config: WallClockTimerConfig | None = None,
        *,
        auto_start: bool = True,
    ) -> None:
        """Initialize this timer.

        See the class documentation for details.

        Args:
            interval: The time between timer ticks. Must be at least 1 millisecond.
            config: The configuration for the timer. If `None`, a default configuration
                will be created using `from_interval()`.
            auto_start: Whether the timer should start automatically. If `False`,
                `reset()` must be called before the timer can be used.

        Raises:
            ValueError: If any value is out of range.
        """
        if interval <= timedelta(0):
            raise ValueError(f"interval must be positive, not {interval}")

        self._interval: timedelta = interval
        """The time to between timer ticks.

        The wall clock is used, so this will be added to the current time to calculate
        the next tick time.
        """

        self._config = config or WallClockTimerConfig.from_interval(interval)
        """The configuration for this timer."""

        self._stopped: bool = True
        """Whether the timer was requested to stop.

        If this is `False`, then the timer is running.

        If this is `True`, then it is stopped or there is a request to stop it
        or it was not started yet:

        * If `_next_tick_time` is `None`, it means it wasn't started yet (it was
          created with `auto_start=False`).  Any receiving method will start
          it by calling `reset()` in this case.

        * If `_next_tick_time` is not `None`, it means there was a request to
          stop it.  In this case receiving methods will raise
          a `ReceiverStoppedError`.
        """

        self._next_tick_time: datetime | None = None
        """The wall clock time when the next tick should happen.

        If this is `None`, it means the timer didn't start yet, but it should
        be started as soon as it is used.
        """

        self._current_info: TimerInfo | None = None
        """The current tick information.

        This is calculated by `ready()` but is returned by `consume()`. If
        `None` it means `ready()` wasn't called and `consume()` will assert.
        `consume()` will set it back to `None` to tell `ready()` that it needs
        to wait again.
        """

        self._clocks_info: ClocksInfo | None = None
        """The information about the clocks and their drift for the last tick."""

        if auto_start:
            self.reset()

    @property
    def interval(self) -> timedelta:
        """The interval between timer ticks.

        Since the wall clock is used, this will be added to the current time to
        calculate the next tick time.

        Danger:
            In real (monotonic) time, the actual time it passes between ticks could be
            smaller, bigger, or even **negative** if the wall clock jumped back in time!
        """
        return self._interval

    @property
    def config(self) -> WallClockTimerConfig:
        """The configuration for this timer."""
        return self._config

    @property
    def is_running(self) -> bool:
        """Whether the timer is running."""
        return not self._stopped

    @property
    def next_tick_time(self) -> datetime | None:
        """The wall clock time when the next tick should happen, or `None` if it is not running."""
        return None if self._stopped else self._next_tick_time

    def reset(self) -> None:
        """Reset the timer to start timing from now (plus an optional alignment).

        If the timer was stopped, or not started yet, it will be started.
        """
        self._stopped = False
        self._update_next_tick_time()
        self._current_info = None
        # We assume the clocks will behave similarly after the timer was reset, so we
        # purposefully don't reset the clocks info.

    def stop(self) -> None:
        """Stop the timer.

        Once `stop` has been called, all subsequent calls to `ready()` will immediately
        return False and calls to `consume()` / `receive()` or any use of the async
        iterator interface will raise
        a [`ReceiverStoppedError`][frequenz.channels.ReceiverStoppedError].

        You can restart the timer with `reset()`.
        """
        self._stopped = True
        # We need to make sure it's not None, otherwise `ready()` will start it
        self._next_tick_time = datetime.now(timezone.utc)

    @override
    async def ready(self) -> bool:
        """Wait until the timer `interval` passed.

        Once a call to `ready()` has finished, the resulting tick information
        must be read with a call to `consume()` (`receive()` or iterated over)
        to tell the timer it should wait for the next interval.

        The timer will remain ready (this method will return immediately)
        until it is consumed.

        Returns:
            Whether the timer was started and it is still running.
        """
        # If there are messages waiting to be consumed, return immediately.
        if self._current_info is not None:
            return True

        # If `_next_tick_time` is `None`, it means it was created with
        # `auto_start=False` and should be started.
        if self._next_tick_time is None:
            self.reset()
            assert (
                self._next_tick_time is not None
            ), "This should be assigned by reset()"

        # If a stop was explicitly requested, we bail out.
        if self._stopped:
            return False

        wall_clock_now = datetime.now(timezone.utc)
        wall_clock_time_to_next_tick = self._next_tick_time - wall_clock_now
        print(
            f"<<<<< TIMER (before adjusting with {self._clocks_info=}):\n"
            f"    next_tick_time={self._next_tick_time}\n"
            f"    now={wall_clock_now}\n"
            f"    wall_clock_time_to_next_tick={wall_clock_time_to_next_tick}\n"
            ">>>>>"
        )
        # If we have information about the clocks from the previous tick, we need to
        # adjust the time to sleep based on that
        if self._clocks_info is not None:
            wall_clock_time_to_next_tick = self._clocks_info.wall_clock_to_monotonic(
                wall_clock_time_to_next_tick
            )
            print(
                f"<<<<< TIMER adjusted to: "
                f"time_to_next_tick={wall_clock_time_to_next_tick} >>>>>"
            )

        # If we didn't reach the tick yet, sleep until we do.
        # We need to do this in a loop to react to resets, time jumps and wall clock
        # time compression, in which cases we need to recalculate the time to the next
        # tick and try again.
        needs_resync: bool = False
        while wall_clock_time_to_next_tick > timedelta(0):
            clocks_info = await self._sleep(wall_clock_time_to_next_tick)

            # We need to adjust the time to sleep based on how much the wall clock time
            # compressed, otherwise we will sleep too little and only approximate to the
            # next tick asymptotically.
            wall_clock_time_to_next_tick = clocks_info.wall_clock_to_monotonic(
                self._next_tick_time - wall_clock_now
            )

            # Technically the monotonic drift should always be positive, but we handle
            # negative values just in case, we've seen a lot of weird things happen.
            monotonic_drift = abs(clocks_info.monotonic_drift)
            drift_tolerance = self._config.async_drift_tolerance
            if drift_tolerance is not None and monotonic_drift > drift_tolerance:
                _logger.warning(
                    "The timer was supposed to sleep for %s, but it slept for %s "
                    "instead [difference=%s, tolerance=%s]. This is likely due to a "
                    "task taking too much time to complete and blocking the event "
                    "loop for too long. You probablyu should profile your code to "
                    "find out what's taking too long.",
                    clocks_info.monotonic_requested_sleep,
                    clocks_info.monotonic_elapsed,
                    monotonic_drift,
                    drift_tolerance,
                )

            # If there was a time jump, we need to resync the timer to the wall clock,
            # otherwise we can be sleeping for a long time until the timer catches up,
            # which is not suitable for many use cases.
            #
            # Resyncing the timer ensures that we keep ticking more or less at `interval`
            # even in the event of time jumps, with the downside that the timer will
            # trigger more than once for the same timestamp if it jumps back in time,
            # and will skip ticks if it jumps forward in time.
            #
            # When there is no threshold, so there is no resync, the ticks will be
            # contigous in time from the wall clock perspective, waiting until we reach
            # the expected next tick time when jumping back in time, and bursting all
            # missed ticks when jumping forward in time.
            wall_clock_jump = clocks_info.wall_clock_jump
            threshold = self._config.wall_clock_jump_threshold
            if threshold is not None and abs(wall_clock_jump) > threshold:
                needs_resync = True
                _logger.warning(
                    "The wall clock jumped %s in time (threshold=%s). A tick will "
                    "be triggered immediately with the timestamp as it was before the "
                    "time jump and the timer will be resynced to the wall clock.",
                    wall_clock_jump,
                    threshold,
                )
                print("<<<<< TIMER: RESYNCING TO THE WALL CLOCK >>>>>")
                break

            print(
                f"<<<<< TIMER: IN WHILE: next_tick_time={self._next_tick_time} "
                f"now={wall_clock_now} new_now={wall_clock_now} >>>>>"
            )

        # TODO: if we are falling too far in the future, we should also resync and emit a
        # warning, as we are not able to keep up with the wall clock time.

        # If a stop was explicitly requested during the sleep, we bail out.
        if self._stopped:
            return False

        self._current_info = TimerInfo(
            expected_tick_time=self._next_tick_time, clocks_info=self._clocks_info
        )

        if needs_resync:
            self._update_next_tick_time(now=wall_clock_now)
            print(
                f"<<<<< TIMER: next_tick_time={self._next_tick_time} now={wall_clock_now} "
                f"time_to_next_tick={wall_clock_time_to_next_tick} >>>>>"
            )
            print("<<<<< TIMER: WALL CLOCK JUMPED BACK IN TIME >>>>>")
        else:
            self._next_tick_time += self._interval

        return True

    @override
    def consume(self) -> TimerInfo:
        """Return the latest tick information once `ready()` is complete.

        Once the timer has triggered ([`ready()`][] is done), this method returns the
        information about the tick that just happened.

        Returns:
            The information about the tick that just happened.

        Raises:
            ReceiverStoppedError: If the timer was stopped via `stop()`.
        """
        # If it was stopped and there it no pending result, we raise
        # (if there is a pending result, then we still want to return it first)
        if self._stopped and self._current_info is None:
            raise ReceiverStoppedError(self)

        assert (
            self._current_info is not None
        ), "calls to `consume()` must be follow a call to `ready()`"
        info = self._current_info
        self._current_info = None
        return info

    def _update_next_tick_time(self, *, now: datetime | None = None) -> None:
        """Update the next tick time, aligning it to `self._align_to` or now."""
        if now is None:
            now = datetime.now(timezone.utc)

        elapsed = timedelta(0)

        if self._config.align_to is not None:
            elapsed = (now - self._config.align_to) % self._interval

        self._next_tick_time = now + self._interval - elapsed

    async def _sleep(self, delay: timedelta, /) -> ClocksInfo:
        """Sleep for a given time and return information about the clocks and their drift.

        The time to sleep is adjusted based on the previously observed drift between the
        wall clock and monotonic time, if any.

        Also saves the information about the clocks and their drift for the next sleep.

        Args:
            delay: The time to sleep.

        Returns:
            The information about the clocks and their drift for this sleep.
        """
        previous_info = self._clocks_info

        start_monotonic_time = asyncio.get_running_loop().time()
        start_wall_clock_time = datetime.now(timezone.utc)

        await asyncio.sleep(delay.total_seconds())

        end_monotonic_time = asyncio.get_running_loop().time()
        end_wall_clock_time = datetime.now(timezone.utc)

        elapsed_monotonic = timedelta(seconds=end_monotonic_time - start_monotonic_time)
        elapsed_wall_clock = end_wall_clock_time - start_wall_clock_time

        self._clocks_info = ClocksInfo(
            monotonic_requested_sleep=delay,
            monotonic_time=end_monotonic_time,
            wall_clock_time=end_wall_clock_time,
            monotonic_elapsed=elapsed_monotonic,
            wall_clock_elapsed=elapsed_wall_clock,
        )

        tolerance = self._config.wall_clock_drift_tolerance_factor
        if tolerance is None:
            return self._clocks_info

        previous_factor = (
            previous_info.calculate_wall_clock_factor() if previous_info else 1.0
        )
        current_factor = self._clocks_info.calculate_wall_clock_factor()
        if abs(current_factor - previous_factor) > tolerance:
            _logger.warning(
                "The wall clock time drifted too much from the monotonic time. The "
                "monotonic time will be adjusted to compensate for this difference. "
                "We expected the wall clock time to have advanced (%s), but the "
                "monotonic time advanced (%s) [relative difference: previous=%s "
                "current=%s, tolerance=%s].",
                elapsed_wall_clock,
                elapsed_monotonic,
                previous_factor,
                current_factor,
                tolerance,
            )

        return self._clocks_info

    def __str__(self) -> str:
        """Return a string representation of this timer."""
        return f"{type(self).__name__}({self.interval})"

    def __repr__(self) -> str:
        """Return a string representation of this timer."""
        return f"{type(self).__name__}<{self.interval=}, {self.is_running=}>"
