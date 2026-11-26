# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the microgrid resampling functions."""

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

import pytest
from frequenz.core.typing import FloatInt
from frequenz.quantities import Quantity

from frequenz.sdk.microgrid import latest_window_mean
from frequenz.sdk.timeseries import (
    ResamplerConfig3,
    ResamplingContext,
    SourceProperties,
    WindowSide,
)
from frequenz.sdk.timeseries._resampling._resampler import _ResamplingHelper

_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)
_PERIOD = timedelta(seconds=10)


def _t(seconds: FloatInt) -> datetime:
    """Return the timestamp of a number of seconds after the test epoch."""
    return _EPOCH + timedelta(seconds=seconds)


def _mean(
    samples: Sequence[tuple[FloatInt, float]],
    *,
    window_start: float = 20.0,
    closed: WindowSide = WindowSide.LEFT,
) -> float:
    """Resample samples given in seconds after the epoch with `latest_window_mean`."""
    context = ResamplingContext(
        window_start=_t(window_start),
        config=ResamplerConfig3(
            _PERIOD, max_data_age_in_periods=5.0, closed=closed, label=closed
        ),
        source_properties=SourceProperties(),
    )
    return latest_window_mean([(_t(t), v) for t, v in samples], context)


def test_current_window_only() -> None:
    """Test only the current window is used when it has samples."""
    assert _mean([(5, 100.0), (15, 100.0), (21, 1.0), (25, 2.0)]) == 1.5


@pytest.mark.parametrize("gap", [1, 2], ids=["one window", "two windows"])
def test_empty_windows_use_newest_previous_window(gap: int) -> None:
    """Test the newest previous window with samples is used if newer ones are empty."""
    start = 20 - 10 * gap
    samples = [(start - 5, 100.0), (start + 1, 1.0), (start + 9, 3.0)]

    assert _mean(samples) == 2.0


@pytest.mark.parametrize(
    "closed, samples, expected",
    [
        # [10, 20) and [20, 30): the sample at 20 is in the current window
        (WindowSide.LEFT, [(15, 100.0), (20, 1.0), (25, 3.0)], 2.0),
        # [10, 20): the sample at 10 is in it
        (WindowSide.LEFT, [(5, 100.0), (10, 1.0), (15, 3.0)], 2.0),
        # (10, 20] and (20, 30]: the sample at 20 is in the previous window
        (WindowSide.RIGHT, [(15, 1.0), (20, 3.0)], 2.0),
        # (10, 20]: the sample at 10 is not in it
        (WindowSide.RIGHT, [(10, 100.0), (15, 1.0), (20, 3.0)], 2.0),
        # (20, 30]: the sample at 30 is in the current window
        (WindowSide.RIGHT, [(20, 100.0), (25, 1.0), (30, 3.0)], 2.0),
    ],
)
def test_samples_on_boundaries(
    closed: WindowSide, samples: list[tuple[FloatInt, float]], expected: float
) -> None:
    """Test samples on window boundaries are in the window on their closed side."""
    assert _mean(samples, closed=closed) == expected


def test_fractional_max_data_age_partial_window() -> None:
    """Test a fractional maximum data age can leave only part of the window."""
    helper = _ResamplingHelper(
        "test",
        ResamplerConfig3(
            _PERIOD,
            max_data_age_in_periods=1.5,
            resampling_function=latest_window_mean,
        ),
    )
    for t, v in [(2, 100.0), (6, 1.0), (8, 3.0)]:
        helper.add_sample((_t(t), v))

    # The current window [10, 20) is empty, and samples older than 5 are dropped
    assert helper.resample(_t(20)).value == Quantity(2.0)


def test_upsampling_repeats_last_value() -> None:
    """Test the last value of a slow source is repeated until a new one arrives."""
    helper = _ResamplingHelper(
        "test",
        ResamplerConfig3(
            timedelta(seconds=1),
            input_period_estimation_samples=2,
            resampling_function=latest_window_mean,
        ),
    )

    helper.add_sample((_t(0), 1.0))
    helper.add_sample((_t(3), 4.0))
    assert helper.resample(_t(4)).value == Quantity(4.0)
    # The input period is estimated from 2 samples in 4 seconds
    assert helper.source_properties.sampling_period == timedelta(seconds=2)

    assert helper.resample(_t(5)).value == Quantity(4.0)

    helper.add_sample((_t(5), 7.0))
    assert helper.resample(_t(6)).value == Quantity(7.0)
