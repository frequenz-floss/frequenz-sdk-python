# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""A timer attached to the wall clock for the resampler."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal, Self, assert_never

from frequenz.channels import Receiver, ReceiverStoppedError
from frequenz.core.datetime import UNIX_EPOCH
from typing_extensions import override

_logger = logging.getLogger(__name__)

_TD_ZERO = timedelta()


@dataclass(frozen=True, kw_only=True)
class WallClockTimerConfig:
    """Configuration for a [wall clock timer][frequenz.sdk.timeseries.WallClockTimer]."""

    align_to: datetime | None = UNIX_EPOCH
    """The time to align the timer to.

    The first timer tick will occur at the first multiple of the timer's interval after
    this value.

    It must be a timezone aware `datetime` or `None`. If `None`, the timer aligns to the
    time it is started.
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

        ```py
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
                    return delta > _TD_ZERO
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
                [`WallClockTimer`][frequenz.sdk.timeseries.WallClockTimer] documentation
                for details.
            async_drift_tolerance_factor: The maximum allowed difference between the
                requested and the real sleep time, relative to the interval.
                `async_drift_tolerance` will be set to `interval * this_factor`.  See
                the [`WallClockTimer`][frequenz.sdk.timeseries.WallClockTimer]
                documentation for details.
            wall_clock_drift_tolerance_factor: The maximum allowed relative difference
                between the wall clock and monotonic time. See the
                [`WallClockTimer`][frequenz.sdk.timeseries.WallClockTimer] documentation
                for details.
            wall_clock_jump_threshold_factor: The amount of time that's considered a
                wall clock jump, relative to the interval. This will be set to
                `interval * this_factor`. See the
                [`WallClockTimer`][frequenz.sdk.timeseries.WallClockTimer] documentation
                for details.

        Returns:
            The created timer configuration.

        Raises:
            ValueError: If any value is out of range.
        """
        if interval <= _TD_ZERO:
            raise ValueError(f"interval must be bigger than 0, not {interval!r}")

        return cls(
            align_to=align_to,
            wall_clock_drift_tolerance_factor=wall_clock_drift_tolerance_factor,
            async_drift_tolerance=interval * async_drift_tolerance_factor,
            wall_clock_jump_threshold=interval * wall_clock_jump_threshold_factor,
        )


@dataclass(frozen=True, kw_only=True)
class ClocksInfo:
    """Information about the wall clock and monotonic clock and their drift.

    The `monotonic_requested_sleep` and `monotonic_elapsed` values must be strictly
    positive, while the `wall_clock_elapsed` can be negative if the wall clock jumped
    back in time.
    """

    monotonic_requested_sleep: timedelta
    """The requested monotonic sleep time used to gather the information (must be positive)."""

    monotonic_time: float
    """The monotonic time right after the sleep was done."""

    wall_clock_time: datetime
    """The wall clock time right after the sleep was done."""

    monotonic_elapsed: timedelta
    """The elapsed time in monotonic time (must be non-negative)."""

    wall_clock_elapsed: timedelta
    """The elapsed time in wall clock time."""

    wall_clock_factor: float = float("nan")
    """The factor to convert wall clock time to monotonic time.

    Typically, if the wall clock time expanded compared to the monotonic time (i.e.
    is more in the future), the returned value will be smaller than 1. If the wall
    clock time compressed compared to the monotonic time (i.e. is more in the past),
    the returned value will be bigger than 1.

    In cases where there are big time jumps this might be overridden by the previous
    wall clock factor to avoid adjusting by excessive amounts, when the time will
    resync anyway to catch up.
    """

    def __post_init__(self) -> None:
        """Check that the values are valid.

        Raises:
            ValueError: If any value is out of range.
        """
        if self.monotonic_requested_sleep <= _TD_ZERO:
            raise ValueError(
                f"monotonic_requested_sleep must be strictly positive, not "
                f"{self.monotonic_requested_sleep!r}"
            )
        if not math.isfinite(self.monotonic_time):
            raise ValueError(
                f"monotonic_time must be a number, not {self.monotonic_time!r}"
            )
        if self.monotonic_elapsed <= _TD_ZERO:
            raise ValueError(
                f"monotonic_elapsed must be strictly positive, not {self.monotonic_elapsed!r}"
            )

        # This is a hack to cache the calculated value, once set it will be "immutable"
        # too, so it shouldn't change the logical "frozenness" of the class.
        if math.isnan(self.wall_clock_factor):
            wall_clock_elapsed = self.wall_clock_elapsed
            if wall_clock_elapsed <= _TD_ZERO:
                _logger.warning(
                    "The monotonic clock advanced %s, but the wall clock stayed still or "
                    "jumped back (elapsed: %s)! Hopefully this was just a singular jump in "
                    "time and not a permanent issue with the wall clock not moving at all. "
                    "For purposes of calculating the wall clock factor, a fake elapsed time "
                    "of one tenth of the elapsed monotonic time will be used.",
                    self.monotonic_elapsed,
                    wall_clock_elapsed,
                )
                wall_clock_elapsed = self.monotonic_elapsed * 0.1
            # We need to use __setattr__ here to bypass the frozen nature of the
            # dataclass. Since we are constructing the class, this is fine and the only
            # way to set calculated defaults in frozen dataclasses at the moment.
            super().__setattr__(
                "wall_clock_factor", self.monotonic_elapsed / wall_clock_elapsed
            )

    @property
    def monotonic_drift(self) -> timedelta:
        """The difference between the monotonic elapsed and requested sleep time.

        This number should be always positive, as the monotonic time should never
        jump back in time.
        """
        return self.monotonic_elapsed - self.monotonic_requested_sleep

    @property
    def wall_clock_jump(self) -> timedelta:
        """The amount of time the wall clock jumped compared to the monotonic time.

        If the wall clock is faster then the monotonic time (or jumped forward in time),
        the returned value will be positive. If the wall clock is slower than the
        monotonic time (or jumped backwards in time), the returned value will be
        negative.

        Note:
            Strictly speaking, both could be in sync and the result would be 0.0, but
            this is extremely unlikely due to floating point precision and the fact
            that both clocks are obtained as slightly different times.
        """
        return self.wall_clock_elapsed - self.monotonic_elapsed

    def wall_clock_to_monotonic(self, wall_clock_timedelta: timedelta, /) -> timedelta:
        """Convert a wall clock timedelta to a monotonic timedelta.

        This is useful to calculate how much one should sleep on the monotonic clock
        to reach a particular wall clock time, adjusting to the difference in speed
        or jumps between both.

        Args:
            wall_clock_timedelta: The wall clock timedelta to convert.

        Returns:
            The monotonic timedelta corresponding to `wall_clock_time` using the
                `wall_clock_factor`.
        """
        return wall_clock_timedelta * self.wall_clock_factor


@dataclass(frozen=True, kw_only=True)
class TickInfo:
    """Information about a `WallClockTimer` tick."""

    expected_tick_time: datetime
    """The expected time when the timer should have triggered."""

    sleep_infos: Sequence[ClocksInfo]
    """The information about every sleep performed to trigger this tick.

    If the timer didn't have do to a [`sleep()`][asyncio.sleep] to trigger the tick
    (i.e. the timer is catching up because there were big drifts in previous ticks),
    this will be empty.
    """

    @property
    def latest_sleep_info(self) -> ClocksInfo | None:
        """The clocks information from the last sleep done to trigger this tick.

        If no sleeps were done, this will be `None`.
        """
        return self.sleep_infos[-1] if self.sleep_infos else None


class WallClockTimer(Receiver[TickInfo]):
    """A timer synchronized with the wall clock.

    This timer uses the wall clock to trigger ticks and handles discrepancies between
    the wall clock and monotonic time. Since sleeping is performed using monotonic time,
    differences between the two clocks can occur.

    When the wall clock progresses slower than monotonic time, it is referred to as
    *compression* (wall clock time appears in the past relative to monotonic time).
    Conversely, when the wall clock progresses faster, it is called *expansion*
    (wall clock time appears in the future relative to monotonic time). If these
    differences exceed a configured threshold, a warning is emitted. The threshold
    is defined by the
    [`wall_clock_drift_tolerance_factor`][frequenz.sdk.timeseries.WallClockTimerConfig.wall_clock_drift_tolerance_factor].

    If the difference becomes excessively large, it is treated as a *time jump*.
    Time jumps can occur, for example, when the wall clock is adjusted by NTP after
    being out of sync for an extended period. In such cases, the timer resynchronizes
    with the wall clock and triggers an immediate tick. The threshold for detecting
    time jumps is controlled by the
    [`wall_clock_jump_threshold`][frequenz.sdk.timeseries.WallClockTimerConfig.wall_clock_jump_threshold].

    The timer ensures ticks are aligned to the
    [`align_to`][frequenz.sdk.timeseries.WallClockTimerConfig.align_to] configuration,
    even after time jumps.

    Additionally, the timer emits warnings if the actual sleep duration deviates
    significantly from the requested duration. This can happen due to event loop
    blocking or system overload. The tolerance for such deviations is defined by the
    [`async_drift_tolerance`][frequenz.sdk.timeseries.WallClockTimerConfig.async_drift_tolerance].

    To account for these complexities, each tick provides a
    [`TickInfo`][frequenz.sdk.timeseries.TickInfo] object, which includes detailed
    information about the clocks and their drift.
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
            interval: The time between timer ticks. Must be positive.
            config: The configuration for the timer. If `None`, a default configuration
                will be created using `from_interval()`.
            auto_start: Whether the timer should start automatically. If `False`,
                `reset()` must be called before the timer can be used.

        Raises:
            ValueError: If any value is out of range.
        """
        if interval <= _TD_ZERO:
            raise ValueError(f"interval must be positive, not {interval}")

        self._interval: timedelta = interval
        """The time to between timer ticks.

        The wall clock is used, so this will be added to the current time to calculate
        the next tick time.
        """

        self._config = config or WallClockTimerConfig.from_interval(interval)
        """The configuration for this timer."""

        self._closed: bool = True
        """Whether the timer was requested to close.

        If this is `False`, then the timer is running.

        If this is `True`, then it is closed or there is a request to close it
        or it was not started yet:

        * If `_next_tick_time` is `None`, it means it wasn't started yet (it was
          created with `auto_start=False`).  Any receiving method will start
          it by calling `reset()` in this case.

        * If `_next_tick_time` is not `None`, it means there was a request to
          close it.  In this case receiving methods will raise
          a `ReceiverStoppedError`.
        """

        self._next_tick_time: datetime | None = None
        """The wall clock time when the next tick should happen.

        If this is `None`, it means the timer didn't start yet, but it should
        be started as soon as it is used.
        """

        self._current_tick_info: TickInfo | None = None
        """The current tick information.

        This is calculated by `ready()` but is returned by `consume()`. If
        `None` it means `ready()` wasn't called and `consume()` will assert.
        `consume()` will set it back to `None` to tell `ready()` that it needs
        to wait again.
        """

        self._clocks_info: ClocksInfo | None = None
        """The latest information about the clocks and their drift.

        Or `None` if no sleeps were done yet.
        """

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
        return not self._closed

    @property
    def next_tick_time(self) -> datetime | None:
        """The wall clock time when the next tick should happen, or `None` if it is not running."""
        return None if self._closed else self._next_tick_time

    def reset(self) -> None:
        """Reset the timer to start timing from now (plus an optional alignment).

        If the timer was closed, or not started yet, it will be started.
        """
        self._closed = False
        self._update_next_tick_time()
        self._current_tick_info = None
        # We assume the clocks will behave similarly after the timer was reset, so we
        # purposefully don't reset the clocks info.
        _logger.debug("reset(): _next_tick_time=%s", self._next_tick_time)

    @override
    def close(self) -> None:
        """Close and stop the timer.

        Once `close` has been called, all subsequent calls to `ready()` will immediately
        return False and calls to `consume()` / `receive()` or any use of the async
        iterator interface will raise a
        [`ReceiverStoppedError`][frequenz.channels.ReceiverStoppedError].

        You can restart the timer with `reset()`.
        """
        self._closed = True
        # We need to make sure it's not None, otherwise `ready()` will start it
        self._next_tick_time = datetime.now(timezone.utc)

    def _should_resync(self, info: ClocksInfo | timedelta | None) -> bool:
        """Check if the timer needs to resynchronize with the wall clock.

        This checks if the wall clock jumped beyond the configured threshold, which
        is defined in the timer configuration.

        Args:
            info: The information about the clocks and their drift. If `None`, it will
                not check for a resync, and will return `False`. If it is a
                `ClocksInfo`, it will check the `wall_clock_jump` property. If it is a
                `timedelta`, it will check if the absolute value is greater than the
                configured threshold.

        Returns:
            Whether the timer should resync to the wall clock.
        """
        threshold = self._config.wall_clock_jump_threshold
        if threshold is None or info is None:
            return False
        if isinstance(info, ClocksInfo):
            info = info.wall_clock_jump
        return abs(info) > threshold

    # We need to disable too many branches here, because the method is too complex but
    # it is not trivial to split into smaller parts.
    @override
    async def ready(self) -> bool:  # pylint: disable=too-many-branches
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
        if self._current_tick_info is not None:
            return True

        # If `_next_tick_time` is `None`, it means it was created with
        # `auto_start=True` and should be started.
        if self._next_tick_time is None:
            self.reset()
            assert (
                self._next_tick_time is not None
            ), "This should be assigned by reset()"

        # If a close was explicitly requested, we bail out.
        if self._closed:
            return False

        wall_clock_now = datetime.now(timezone.utc)
        wall_clock_time_to_next_tick = self._next_tick_time - wall_clock_now

        # If we didn't reach the tick yet, sleep until we do.
        # We need to do this in a loop to react to resets, time jumps and wall clock
        # time compression, in which cases we need to recalculate the time to the next
        # tick and try again.
        sleeps: list[ClocksInfo] = []
        should_resync: bool = self._should_resync(self._clocks_info)
        while wall_clock_time_to_next_tick > _TD_ZERO:
            prev_clocks_info = self._clocks_info
            # We don't assign directly to self._clocks_info because its type is
            # ClocksInfo | None, and sleep() returns ClocksInfo, so we can avoid some
            # None checks further in the code with `clocks_info` (and we make the code
            # more succinct).
            clocks_info = await self._sleep(
                wall_clock_time_to_next_tick, prev_clocks_info=prev_clocks_info
            )
            should_resync = self._should_resync(clocks_info)
            wall_clock_now = datetime.now(timezone.utc)
            self._clocks_info = clocks_info

            sleeps.append(clocks_info)

            if previous_factor := self._has_drifted_beyond_tolerance(
                new_clocks_info=clocks_info, prev_clocks_info=prev_clocks_info
            ):
                # If we are resyncing we have a different issue, and we are not going to
                # use the factor to adjust the clock, but will just resync
                if not should_resync:
                    _logger.warning(
                        "The wall clock time drifted too much from the monotonic time. The "
                        "monotonic time will be adjusted to compensate for this difference. "
                        "We expected the wall clock time to have advanced (%s), but the "
                        "monotonic time advanced (%s) [previous_factor=%s "
                        "current_factor=%s, factor_change_absolute_tolerance=%s].",
                        clocks_info.wall_clock_elapsed,
                        clocks_info.monotonic_elapsed,
                        previous_factor,
                        clocks_info.wall_clock_factor,
                        self._config.wall_clock_drift_tolerance_factor,
                    )

            wall_clock_time_to_next_tick = self._next_tick_time - wall_clock_now

            # Technically the monotonic drift should always be positive, but we handle
            # negative values just in case, we've seen a lot of weird things happen.
            monotonic_drift = abs(clocks_info.monotonic_drift)
            drift_tolerance = self._config.async_drift_tolerance
            if drift_tolerance is not None and monotonic_drift > drift_tolerance:
                _logger.warning(
                    "The timer was supposed to sleep for %s, but it slept for %s "
                    "instead [difference=%s, tolerance=%s]. This is likely due to a "
                    "task taking too much time to complete and blocking the event "
                    "loop for too long. You probably should profile your code to "
                    "find out what's taking too long.",
                    clocks_info.monotonic_requested_sleep,
                    clocks_info.monotonic_elapsed,
                    monotonic_drift,
                    drift_tolerance,
                )

            # If we detect a time jump, we exit the loop and handle it outside of it, to
            # also account for time jumps in the past that could happen without even
            # having entered into the sleep loop.
            if should_resync:
                _logger.debug(
                    "ready(): Exiting the wait loop because we detected a time jump "
                    "and need to re-sync."
                )
                break

            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "ready(): In sleep loop:\n"
                    "    next_tick_time=%s (%s)\n"
                    "    now=%s (%s)\n"
                    "    mono_now=%s\n"
                    "    wall_clock_time_to_next_tick=%s (%s)",
                    self._next_tick_time,
                    self._next_tick_time.timestamp(),
                    wall_clock_now,
                    wall_clock_now.timestamp(),
                    asyncio.get_running_loop().time(),
                    wall_clock_time_to_next_tick,
                    wall_clock_time_to_next_tick.total_seconds(),
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
        if should_resync:
            assert self._clocks_info is not None
            _logger.warning(
                "The wall clock jumped %s (%s seconds) in time (threshold=%s). "
                "A tick will be triggered immediately with the `expected_tick_time` "
                "as it was before the time jump and the timer will be resynced to "
                "the wall clock.",
                self._clocks_info.wall_clock_jump,
                self._clocks_info.wall_clock_jump.total_seconds(),
                self._config.wall_clock_jump_threshold,
            )

        # If a close was explicitly requested during the sleep, we bail out.
        if self._closed:
            return False

        self._current_tick_info = TickInfo(
            expected_tick_time=self._next_tick_time, sleep_infos=sleeps
        )

        if should_resync:
            _logger.debug(
                "ready(): Before resync:\n"
                "    next_tick_time=%s\n"
                "    now=%s\n"
                "    wall_clock_time_to_next_tick=%s",
                self._next_tick_time,
                wall_clock_now,
                wall_clock_time_to_next_tick,
            )
            self._update_next_tick_time(now=wall_clock_now)
            _logger.debug(
                "ready(): After resync: next_tick_time=%s", self._next_tick_time
            )
        else:
            self._next_tick_time += self._interval
            _logger.debug(
                "ready(): No resync needed: next_tick_time=%s",
                self._next_tick_time,
            )

        return True

    @override
    def consume(self) -> TickInfo:
        """Return the latest tick information once `ready()` is complete.

        Once the timer has triggered
        ([`ready()`][frequenz.sdk.timeseries.WallClockTimer.ready] is done), this method
        returns the information about the tick that just happened.

        Returns:
            The information about the tick that just happened.

        Raises:
            ReceiverStoppedError: If the timer was closed via `close()`.
        """
        # If it was closed and there it no pending result, we raise
        # (if there is a pending result, then we still want to return it first)
        if self._closed and self._current_tick_info is None:
            raise ReceiverStoppedError(self)

        assert (
            self._current_tick_info is not None
        ), "calls to `consume()` must be follow a call to `ready()`"
        info = self._current_tick_info
        self._current_tick_info = None
        return info

    def _update_next_tick_time(self, *, now: datetime | None = None) -> None:
        """Update the next tick time, aligning it to `self._align_to` or now."""
        if now is None:
            now = datetime.now(timezone.utc)

        elapsed = _TD_ZERO

        if self._config.align_to is not None:
            elapsed = (now - self._config.align_to) % self._interval

        self._next_tick_time = now + self._interval - elapsed

    def _has_drifted_beyond_tolerance(
        self, *, new_clocks_info: ClocksInfo, prev_clocks_info: ClocksInfo | None
    ) -> float | Literal[False]:
        """Check if the wall clock drifted beyond the configured tolerance.

        This checks the relative difference between the wall clock and monotonic time
        based on the `wall_clock_drift_tolerance_factor` configuration.

        Args:
            new_clocks_info: The information about the clocks and their drift from the
                current sleep.
            prev_clocks_info: The information about the clocks and their drift from the
                previous sleep. If `None`, the previous factor will be considered 1.0.

        Returns:
            The previous wall clock factor if the drift is beyond the tolerance, or
                `False` if it is not.
        """
        tolerance = self._config.wall_clock_drift_tolerance_factor
        if tolerance is None:
            return False

        previous_factor = (
            prev_clocks_info.wall_clock_factor if prev_clocks_info else 1.0
        )
        current_factor = new_clocks_info.wall_clock_factor
        if abs(current_factor - previous_factor) > tolerance:
            return previous_factor
        return False

    async def _sleep(
        self, delay: timedelta, /, *, prev_clocks_info: ClocksInfo | None
    ) -> ClocksInfo:
        """Sleep for a given time and return information about the clocks and their drift.

        The time to sleep is adjusted based on the previously observed drift between the
        wall clock and monotonic time, if any.

        Also saves the information about the clocks and their drift for the next sleep.

        Args:
            delay: The time to sleep. Will be adjusted based on `prev_clocks_info` if
                available.
            prev_clocks_info: The information about the clocks and their drift from the
                previous sleep. If `None`, the sleep will be done as requested, without
                adjusting the time to sleep.

        Returns:
            The information about the clocks and their drift for this sleep.
        """
        if prev_clocks_info is not None:
            _logger.debug(
                "_sleep(): Adjusted original requested delay (%s) with factor %s",
                delay.total_seconds(),
                prev_clocks_info.wall_clock_factor,
            )
            delay = prev_clocks_info.wall_clock_to_monotonic(delay)

        delay_s = delay.total_seconds()

        _logger.debug("_sleep(): Will sleep for %s seconds", delay_s)
        start_monotonic_time = asyncio.get_running_loop().time()
        start_wall_clock_time = datetime.now(timezone.utc)
        await asyncio.sleep(delay_s)

        end_monotonic_time = asyncio.get_running_loop().time()
        end_wall_clock_time = datetime.now(timezone.utc)

        elapsed_monotonic = timedelta(seconds=end_monotonic_time - start_monotonic_time)
        elapsed_wall_clock = end_wall_clock_time - start_wall_clock_time

        wall_clock_jump = elapsed_wall_clock - elapsed_monotonic
        should_resync = self._should_resync(wall_clock_jump)
        _logger.debug("_sleep(): SHOULD RESYNC? %s", should_resync)
        clocks_info = ClocksInfo(
            monotonic_requested_sleep=delay,
            monotonic_time=end_monotonic_time,
            wall_clock_time=end_wall_clock_time,
            monotonic_elapsed=elapsed_monotonic,
            wall_clock_elapsed=elapsed_wall_clock,
            # If we should resync it means there was a big time jump, which should be
            # exceptional (NTP adjusting the clock or something like that), in this case
            # we want to use the previous factor as the current one will be way off.
            wall_clock_factor=(
                prev_clocks_info.wall_clock_factor
                if prev_clocks_info and should_resync
                else float("nan")  # nan means let ClocksInfo calculate it
            ),
        )
        _logger.debug(
            "_sleep(): After sleeping:\n"
            "    monotonic_requested_sleep=%s\n"
            "    monotonic_time=%s\n"
            "    wall_clock_time=%s\n"
            "    monotonic_elapsed=%s\n"
            "    wall_clock_elapsed=%s\n"
            "    factor=%s\n",
            clocks_info.monotonic_requested_sleep,
            clocks_info.monotonic_time,
            clocks_info.wall_clock_time,
            clocks_info.monotonic_elapsed,
            clocks_info.wall_clock_elapsed,
            clocks_info.wall_clock_factor,
        )

        return clocks_info

    def __str__(self) -> str:
        """Return a string representation of this timer."""
        return f"{type(self).__name__}({self.interval})"

    def __repr__(self) -> str:
        """Return a string representation of this timer."""
        next_tick = (
            ""
            if self._next_tick_time is None
            else f", next_tick_time={self._next_tick_time!r}"
        )
        return (
            f"{type(self).__name__}<interval={self.interval!r}, "
            f"is_running={self.is_running!r}{next_tick}>"
        )
