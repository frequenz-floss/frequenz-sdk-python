# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the ticking behavior of the WallClockTimer.

This module contains tests to verify that the `WallClockTimer` behaves correctly
under various clock conditions, such as clock drift and time jumps.

The core of the testing is done in the `test_ticking` function, which is a
parameterized test that runs multiple scenarios defined as `_TickTestCase`
instances. Each test case simulates a sequence of timer ticks, specifying how the
wall clock should be manipulated during each tick's sleep phase.

Key aspects of the testing approach:

- **Time Mocking**: We mock both the wall clock (`datetime.now`) and monotonic
  clock (`asyncio.sleep`) to have full control over time. The `TimeDriver`
  utility class orchestrates the time advancements and adjustments.

- **Simplified Time Representation**: All time values in the test cases (timestamps,
  deltas) are expressed in plain seconds (as floats). This is possible because
  the mocked wall clock starts at the Unix epoch (time 0). This convention
  greatly simplifies writing and debugging tests, as it's easier to reason
  about small numbers rather than complex `datetime` and `timedelta` objects,
  which can be especially cryptic when they are large or negative.

- **Scenario-based Testing**: Each `_TickTestCase` defines a complete scenario,
  like "constant forward drift" or "backward jump". For each tick within the
  scenario, a `_TickSpec` defines the expected `TickInfo` and any wall clock
  adjustments to be made. This allows for precise testing of the timer's logic
  for drift compensation and resynchronization.
"""

import logging
import re
from collections.abc import Sequence
from unittest.mock import AsyncMock, call

import async_solipsism
import pytest
from attr import dataclass

from frequenz.sdk.timeseries._resampling._wall_clock_timer import (
    ClocksInfo,
    TickInfo,
    WallClockTimer,
    WallClockTimerConfig,
)

from .util import (
    Adjustment,
    TimeDriver,
    approx_tick_info,
    approx_time,
    delta,
    matches_re,
    mono_now,
    timestamp,
    to_seconds,
    wall_now,
)

_logger = logging.getLogger(__name__)


@pytest.fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


async def test_ready_called_twice_returns_immediately(time_driver: TimeDriver) -> None:
    """Test that calling ready() twice returns immediately the second time."""
    interval = delta(1.0)
    timer = WallClockTimer(
        interval, config=WallClockTimerConfig.from_interval(interval)
    )

    # The first call to ready() will start the timer and wait for the first tick.
    mono_start = mono_now()
    await time_driver.next_tick([Adjustment(0.9, 1.0)], timer.ready())
    mono_end = mono_now()
    assert mono_end == pytest.approx(mono_start + 1.0)

    # The second call should return immediately.
    mono_start = mono_now()
    assert await timer.ready()
    mono_end = mono_now()

    # Time should not have advanced.
    assert mono_end == pytest.approx(mono_start)


@dataclass(kw_only=True, frozen=True)
class _TickSpec:
    """Specification for one tick in a TickTestCase."""

    wall_clock_adjustments: Sequence[Adjustment]
    """The wall clock time adjustments to do while waiting for the next tick."""

    expected_tick_info: TickInfo
    """Expected TickInfo returned by the timer after the tick, if any."""

    expected_warnings: Sequence[str] = ()
    """The expected warning messages during the tick.

    Each string will be used as a regex pattern that should match the logged warning
    messages.
    """


@dataclass(kw_only=True, frozen=True)
class _TickTestCase:
    """A test case for the WallClockTimer ticking behavior."""

    id: str
    """The identifier for the test case, used in parameterized tests."""

    ticks: Sequence[_TickSpec]
    """The specifications for the ticks in this test case."""


# IMPORTANT: All tests are written for a timer with a 1 second interval and default
# config. If the default changes, the tests will need to be updated accordingly.
@pytest.mark.parametrize(
    "case",
    [
        # Both clocks are perfecly in sync. The timer should sleep for the full
        # interval and wake up at the expected wall clock time.
        _TickTestCase(
            id="in_sync",
            ticks=[
                # We adjust the wall clock time slightly before the timer wakes up
                # to the interval (1.0 seconds) to match the elapsed time in the
                # monotonic clock, to ensure both clocks are in sync.
                _TickSpec(  # Tick 1, 2, 3, 4, 5
                    wall_clock_adjustments=[Adjustment(0.9, 1.0 * (i + 1))],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(1.0 * (i + 1)),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(1.0 * (i + 1)),
                                monotonic_time=1.0 * (i + 1),
                                wall_clock_elapsed=delta(1.0),
                                monotonic_elapsed=delta(1.0),
                                monotonic_requested_sleep=delta(1.0),
                            )
                        ],
                    ),
                )
                for i in range(5)
            ],
        ),
        # The wall clock is slightly ahead of the monotonic clock, but with a
        # constant drift.  The first tick will be slightly off because of the drift,
        # the next tick the timer will adjust using a factor due to the drift, but
        # also account for the drift in the first tick, ending up with a
        # particularly short sleep time. Afterwards the timer should apply a factor
        # and always sleep slightly less than the interval to account for the drift.
        _TickTestCase(
            id="constant_forward_drift_within_tolerance",
            ticks=[
                # We adjust the wall clock time slightly before the timer wakes up
                # to a bit more than the interval (1.01 seconds) to simulate a wall
                # clock forward drift.
                _TickSpec(  # Tick 1
                    wall_clock_adjustments=[Adjustment(0.9, 1.01)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(1.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(1.01),
                                monotonic_time=1.0,
                                wall_clock_elapsed=delta(1.01),
                                monotonic_elapsed=delta(1.0),
                                monotonic_requested_sleep=delta(1.0),
                            )
                        ],
                    ),
                ),
                # In the second tick, we expect the timer to detect that drift and
                # adjust the sleep time accordingly, so it will sleep slightly less
                # than the interval to account for the drift. It calculates an
                # adjustment factor of 0.99 seconds but for this tick sleeps even
                # less, because it also woke up late, so it effectively sleeps
                # 0.980198 seconds.
                # We adjust the wall clock time slightly before the timer wakes up
                # to a perfect multiple of the interval (2.0 seconds) to simulate
                # that the wall clock drift stays constant (so after the adjustments
                # the timer could wake up at the desired wall clock time of 2
                # intervals).
                _TickSpec(  # Tick 2
                    wall_clock_adjustments=[Adjustment(0.9, 2.0)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(2.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(2.0),
                                monotonic_time=1.980198,
                                wall_clock_elapsed=delta(0.99),
                                monotonic_elapsed=delta(0.980198),
                                monotonic_requested_sleep=delta(0.980198),
                            )
                        ],
                    ),
                ),
                # For the rest of the ticks, we just expect the timer to always
                # sleep for the interval * the factor (0.99099 seconds) to account
                # for the drift, so we adjust the wall clock time to match the
                # expected wall clock time for each tick at a multiple of the
                # interval (3.0 seconds, 4.0 seconds, etc.) and verify the timer
                # reports a constant factor (sleeps always for 0.990099 seconds).
                *[
                    _TickSpec(  # Tick 3, 4, ..., 10
                        wall_clock_adjustments=[Adjustment(0.9, 3.0 + i)],
                        expected_tick_info=TickInfo(
                            expected_tick_time=timestamp(3.0 + i),
                            sleep_infos=[
                                ClocksInfo(
                                    wall_clock_time=timestamp(3.0 + i),
                                    monotonic_time=2.970297 + i * 0.990099,
                                    wall_clock_elapsed=delta(1.0),
                                    monotonic_elapsed=delta(0.990099),
                                    monotonic_requested_sleep=delta(0.990099),
                                )
                            ],
                        ),
                    )
                    for i in range(8)
                ],
            ],
        ),
        # The wall clock is slightly behind the monotonic clock, but with a
        # constant drift. The first tick will be slightly off because of the lag,
        # the next tick the timer will adjust using a factor due to the lag, but
        # also account for the lag in the first tick, ending up with a
        # particularly long sleep time. Afterwards the timer should apply a factor
        # and always sleep slightly more than the interval to account for the lag.
        _TickTestCase(
            id="constant_backward_drift_within_tolerance",
            ticks=[
                # We adjust the wall clock time slightly before the timer wakes up
                # to a bit less than the interval (0.99 seconds) to simulate a wall
                # clock backward drift.
                _TickSpec(  # Tick 1
                    wall_clock_adjustments=[
                        Adjustment(0.9, 0.99),
                        Adjustment(0.105, 1.0),
                    ],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(1.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(0.99),
                                monotonic_time=1.0,
                                wall_clock_elapsed=delta(0.99),
                                monotonic_elapsed=delta(1.0),
                                monotonic_requested_sleep=delta(1.0),
                            ),
                            ClocksInfo(
                                wall_clock_time=timestamp(1.0),
                                monotonic_time=1.010101,
                                wall_clock_elapsed=delta(0.01),
                                monotonic_elapsed=delta(0.010101),
                                monotonic_requested_sleep=delta(0.010101),
                            ),
                        ],
                    ),
                ),
                _TickSpec(  # Tick 2
                    wall_clock_adjustments=[Adjustment(0.9, 2.0)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(2.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(2.0),
                                monotonic_time=2.020202,
                                wall_clock_elapsed=delta(1.0),
                                monotonic_elapsed=delta(1.010101),
                                monotonic_requested_sleep=delta(1.010101),
                            )
                        ],
                    ),
                ),
                *[
                    _TickSpec(  # Tick 3, 4, ..., 10
                        wall_clock_adjustments=[Adjustment(0.9, 3.0 + i)],
                        expected_tick_info=TickInfo(
                            expected_tick_time=timestamp(3.0 + i),
                            sleep_infos=[
                                ClocksInfo(
                                    wall_clock_time=timestamp(3.0 + i),
                                    monotonic_time=3.030303 + i * 1.010101,
                                    wall_clock_elapsed=delta(1.0),
                                    monotonic_elapsed=delta(1.010101),
                                    monotonic_requested_sleep=delta(1.010101),
                                )
                            ],
                        ),
                    )
                    for i in range(8)
                ],
            ],
        ),
        # The wall clock is erratic and drifts forward and backwards but always within
        # the tolerance drift tolerance.
        _TickTestCase(
            id="erratic_drift_without_jumps",
            ticks=[
                # We adjust the wall clock time slightly before the timer wakes up
                # to a bit more than the interval (1.01 seconds) to simulate a wall
                # clock forward drift.
                _TickSpec(  # Tick 1
                    wall_clock_adjustments=[Adjustment(0.9, 1.01)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(1.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(1.01),
                                monotonic_time=1.0,
                                wall_clock_elapsed=delta(1.01),
                                monotonic_elapsed=delta(1.0),
                                monotonic_requested_sleep=delta(1.0),
                            )
                        ],
                    ),
                ),
                # The timer should have adjusted for the forward drift. Now we
                # introduce a backward drift.
                _TickSpec(  # Tick 2
                    wall_clock_adjustments=[
                        Adjustment(0.9, 1.99),
                        # So the timer will need to sleep again to compensate
                        Adjustment(0.09, 2.07),
                    ],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(2.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(1.99),
                                monotonic_time=1.980198,
                                wall_clock_elapsed=delta(0.98),
                                monotonic_elapsed=delta(0.980198),
                                monotonic_requested_sleep=delta(0.980198),
                            ),
                            ClocksInfo(
                                wall_clock_time=timestamp(2.07),
                                monotonic_time=1.9902,
                                wall_clock_elapsed=delta(0.08),
                                monotonic_elapsed=delta(0.010002),
                                monotonic_requested_sleep=delta(0.010002),
                            ),
                        ],
                    ),
                    # We get a warning this time because the last time was WAY off,
                    # it wanted to sleep for 0.01002 seconds but the wall clock
                    # advanced 0.080000 seconds, so the factor changed too much,
                    # which should trigger a warning.
                    # The total difference between the clocks is still way under the
                    # jump tolerance, so we don't get a resync.
                    expected_warnings=[
                        re.escape(
                            "The wall clock time drifted too much from the monotonic time. The "
                            "monotonic time will be adjusted to compensate for this "
                            "difference. We expected the wall clock time to have advanced "
                            "(0:00:00.080000), but the monotonic time advanced "
                            "(0:00:00.010002) [previous_factor=1.00020204"
                        )
                        + r"\d*"
                        + re.escape(
                            " current_factor=0.125025, factor_change_absolute_tolerance=0.1]."
                        ),
                    ],
                ),
                # The timer should have adjusted for the backward drift. Now we
                # introduce a cumulative backward drift.
                # The clock adjustment factor is now way off because of the last
                # small sleep that took too long, so the timer will compensate for
                # it, and sleep 0.116273 seconds when trying to sleep for 0.93
                # seconds (3.0 - 2.07).
                _TickSpec(  # Tick 3
                    wall_clock_adjustments=[
                        # Start monotonic_time=1.9902
                        Adjustment(0.11, 2.998),
                        # monotonic_time=2.1002 (next sleep end at 2.106473)
                        Adjustment(0.0064, 2.999),
                        # monotonic_time=2.1066 (next sleep end at 2.106724)
                        Adjustment(0.0002, 3.0),
                        # monotonic_time=2.1068 (next sleep end at 2.106975)
                    ],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(3.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(2.998),
                                monotonic_time=2.106473,
                                wall_clock_elapsed=delta(0.928),
                                monotonic_elapsed=delta(0.116273),
                                monotonic_requested_sleep=delta(0.116273),
                            ),
                            ClocksInfo(
                                wall_clock_time=timestamp(2.999),
                                monotonic_time=2.106724,
                                wall_clock_elapsed=delta(0.001),
                                monotonic_elapsed=delta(0.000251),
                                monotonic_requested_sleep=delta(0.000251),
                            ),
                            ClocksInfo(
                                wall_clock_time=timestamp(3.0),
                                monotonic_time=2.106975,
                                wall_clock_elapsed=delta(0.001000),
                                monotonic_elapsed=delta(0.000251),
                                monotonic_requested_sleep=delta(0.000251),
                            ),
                        ],
                    ),
                    # Again there is a big difference in the drifts of different sleeps
                    expected_warnings=[
                        re.escape(
                            "The wall clock time drifted too much from the monotonic time. The "
                            "monotonic time will be adjusted to compensate for this "
                            "difference. We expected the wall clock time to have advanced "
                            "(0:00:00.001000), but the monotonic time advanced "
                            "(0:00:00.000251) [previous_factor=0.125294"
                        )
                        + r"\d*"
                        + re.escape(
                            " current_factor=0.251, factor_change_absolute_tolerance=0.1]."
                        ),
                    ],
                ),
                # Now it goes back to a forward drift
                _TickSpec(  # Tick 4
                    wall_clock_adjustments=[Adjustment(0.2, 4.01)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(4.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(4.01),
                                monotonic_time=2.3579749,
                                wall_clock_elapsed=delta(1.01),
                                monotonic_elapsed=delta(0.251),
                                monotonic_requested_sleep=delta(0.251),
                            )
                        ],
                    ),
                ),
                # And finally it stabilizes to a monotonic clock that is much
                # faster than the wall clock, so the sleeps are constant but much smaller
                # than the interval to make up for the drift.
                _TickSpec(  # Tick 5
                    wall_clock_adjustments=[Adjustment(0.2, 5.0)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(5.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(5.0),
                                monotonic_time=2.604005,
                                wall_clock_elapsed=delta(0.99),
                                monotonic_elapsed=delta(0.246030),
                                monotonic_requested_sleep=delta(0.246030),
                            )
                        ],
                    ),
                ),
                *[
                    _TickSpec(  # Tick 6, 7, ..., 10
                        wall_clock_adjustments=[Adjustment(0.2, 6.0 + i)],
                        expected_tick_info=TickInfo(
                            expected_tick_time=timestamp(6.0 + i),
                            sleep_infos=[
                                ClocksInfo(
                                    wall_clock_time=timestamp(6.0 + i),
                                    monotonic_time=2.852519 + i * 0.248515,
                                    wall_clock_elapsed=delta(1.0),
                                    monotonic_elapsed=delta(0.248515),
                                    monotonic_requested_sleep=delta(0.248515),
                                )
                            ],
                        ),
                    )
                    for i in range(5)
                ],
            ],
        ),
        _TickTestCase(
            id="forward_jump",
            ticks=[
                # First tick is in sync
                _TickSpec(  # Tick 1
                    wall_clock_adjustments=[Adjustment(0.9, 1.0)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(1.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(1.0),
                                monotonic_time=1.0,
                                wall_clock_elapsed=delta(1.0),
                                monotonic_elapsed=delta(1.0),
                                monotonic_requested_sleep=delta(1.0),
                            )
                        ],
                    ),
                ),
                # The second tick has a forward jump of 1.01 seconds, which is more
                # than the jump tolerance of 1 second, so we expect a warning and a
                # resync of the timer to the wall clock.  The clocks info will also
                # have an override for the wall clock factor to use the previous
                # one, because the factor will be either extremely low otherwise due
                # to the jump.
                _TickSpec(  # Tick 2
                    wall_clock_adjustments=[Adjustment(0.9, 3.01)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(2.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(3.01),
                                monotonic_time=2.0,
                                wall_clock_elapsed=delta(2.01),
                                monotonic_elapsed=delta(1.0),
                                monotonic_requested_sleep=delta(1.0),
                                wall_clock_factor=1.0,
                            )
                        ],
                    ),
                    expected_warnings=[
                        re.escape(
                            "The wall clock jumped 0:00:01.010000 (1.01 seconds) in time "
                            "(threshold=0:00:01). A tick will be triggered immediately with "
                            "the `expected_tick_time` as it was before the time jump and the "
                            "timer will be resynced to the wall clock."
                        ),
                    ],
                ),
                _TickSpec(  # Tick 3
                    wall_clock_adjustments=[Adjustment(0.9, 4.0)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(4.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(4.0),
                                monotonic_time=2.99,
                                wall_clock_elapsed=delta(0.99),
                                monotonic_elapsed=delta(0.99),
                                monotonic_requested_sleep=delta(0.99),
                            )
                        ],
                    ),
                ),
                # For the rest of the ticks, we just expect the clocks to keep being
                # in sync, so the timer will sleep for the full interval
                # (1.0 seconds) and wake up at the expected wall clock time.
                *[
                    _TickSpec(  # Tick 4, 5, ..., 10
                        wall_clock_adjustments=[Adjustment(0.9, 5.0 + i)],
                        expected_tick_info=TickInfo(
                            expected_tick_time=timestamp(5.0 + i),
                            sleep_infos=[
                                ClocksInfo(
                                    wall_clock_time=timestamp(5.0 + i),
                                    monotonic_time=3.99 + i,
                                    wall_clock_elapsed=delta(1.0),
                                    monotonic_elapsed=delta(1.0),
                                    monotonic_requested_sleep=delta(1.0),
                                )
                            ],
                        ),
                    )
                    for i in range(7)
                ],
            ],
        ),
        _TickTestCase(
            id="backward_jump",
            ticks=[
                # First tick is in sync
                _TickSpec(  # Tick 1
                    wall_clock_adjustments=[Adjustment(0.9, 1.0)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(1.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(1.0),
                                monotonic_time=1.0,
                                wall_clock_elapsed=delta(1.0),
                                monotonic_elapsed=delta(1.0),
                                monotonic_requested_sleep=delta(1.0),
                            )
                        ],
                    ),
                ),
                # The second tick has a backward jump of 1.1 seconds, which is more
                # than the jump tolerance of 1 second, so we expect a warning and a
                # resync of the timer to the wall clock.
                _TickSpec(  # Tick 2
                    wall_clock_adjustments=[Adjustment(0.9, 0.9)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(2.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(0.9),
                                monotonic_time=2.0,
                                wall_clock_elapsed=delta(-0.1),
                                monotonic_elapsed=delta(1.0),
                                monotonic_requested_sleep=delta(1.0),
                                wall_clock_factor=1.0,
                            )
                        ],
                    ),
                    expected_warnings=[
                        re.escape(
                            "The wall clock jumped -1 day, 23:59:58.900000 (-1.1 seconds) in "
                            "time (threshold=0:00:01). A tick will be triggered immediately "
                            "with the `expected_tick_time` as it was before the time jump "
                            "and the timer will be resynced to the wall clock.",
                        ),
                    ],
                ),
                # After the jump, the timer is resynced. The next tick is set to be
                # at 1.0, which is the next interval aligned to the epoch after 0.9
                # seconds. We make both clocks advance in sync for this tick, so
                # after this all should get back to normal.
                _TickSpec(  # Tick 3
                    wall_clock_adjustments=[Adjustment(0.09, 1.0)],
                    expected_tick_info=TickInfo(
                        expected_tick_time=timestamp(1.0),
                        sleep_infos=[
                            ClocksInfo(
                                wall_clock_time=timestamp(1.0),
                                monotonic_time=2.1,
                                wall_clock_elapsed=delta(0.1),
                                monotonic_elapsed=delta(0.1),
                                monotonic_requested_sleep=delta(0.1),
                            )
                        ],
                    ),
                ),
                # For the rest of the ticks, we just expect the clocks to keep being
                # in sync, so the timer will sleep for the full interval
                # (1.0 seconds) and wake up at the expected wall clock time.
                *[
                    _TickSpec(  # Tick 4, 5, ..., 10
                        wall_clock_adjustments=[Adjustment(0.9, 2.0 + i)],
                        expected_tick_info=TickInfo(
                            expected_tick_time=timestamp(2.0 + i),
                            sleep_infos=[
                                ClocksInfo(
                                    wall_clock_time=timestamp(2.0 + i),
                                    monotonic_time=3.1 + i,
                                    wall_clock_elapsed=delta(1.0),
                                    monotonic_elapsed=delta(1.0),
                                    monotonic_requested_sleep=delta(1.0),
                                )
                            ],
                        ),
                    )
                    for i in range(7)
                ],
            ],
        ),
    ],
    ids=lambda case: case.id,
)
async def test_ticking(
    case: _TickTestCase,
    time_driver: TimeDriver,
    caplog: pytest.LogCaptureFixture,
    asyncio_sleep_mock: AsyncMock,
) -> None:
    """Test ticking behavior of the wall clock timer."""
    # You might need to comment this out if you want to enable debug logging
    caplog.set_level(
        logging.WARNING,
        logger="frequenz.sdk.timeseries._resampling._wall_clock_timer",
    )

    # IMPORTANT: All test cases are relying on a timer with a 1 second interval and
    # default config. If the default changes, the tests will need to be updated
    # accordingly.
    interval = delta(1.0)
    timer = WallClockTimer(
        interval, config=WallClockTimerConfig.from_interval(interval)
    )

    for i, tick in enumerate(case.ticks):
        _logger.debug(
            "================== %s: tick %s/%s =============================",
            case.id,
            i + 1,
            len(case.ticks),
        )
        tick = case.ticks[i]
        expected_tick_info = tick.expected_tick_info

        mono_start = mono_now()
        actual_tick_info = await time_driver.next_tick(
            tick.wall_clock_adjustments, timer.receive()
        )
        mono_end = mono_now()
        _logger.debug(
            "After tick %s: now_wall=%s, now_mono=%s, factor=%s",
            i,
            wall_now(),
            mono_end,
            (
                actual_tick_info.latest_sleep_info.wall_clock_factor
                if actual_tick_info.latest_sleep_info
                else None
            ),
        )

        assert actual_tick_info == approx_tick_info(expected_tick_info)

        if actual_tick_info.latest_sleep_info:
            assert actual_tick_info.latest_sleep_info.wall_clock_time == approx_time(
                wall_now()
            )
        assert actual_tick_info.sleep_infos[-1].monotonic_time == pytest.approx(
            mono_end
        )

        if tick.wall_clock_adjustments:
            assert timestamp(tick.wall_clock_adjustments[-1].wall_time) == approx_time(
                wall_now()
            )
        assert sum(
            to_seconds(i.monotonic_elapsed) for i in actual_tick_info.sleep_infos
        ) == pytest.approx(mono_end - mono_start)
        assert asyncio_sleep_mock.mock_calls == [
            call(pytest.approx(to_seconds(t.monotonic_elapsed)))
            for t in expected_tick_info.sleep_infos
        ]

        filtered_logs = [
            r.message
            for r in caplog.records
            if r.levelno == logging.WARNING
            and r.name == "frequenz.sdk.timeseries._resampling._wall_clock_timer"
        ]
        assert filtered_logs == [matches_re(w) for w in tick.expected_warnings]

        asyncio_sleep_mock.reset_mock()
        caplog.clear()
