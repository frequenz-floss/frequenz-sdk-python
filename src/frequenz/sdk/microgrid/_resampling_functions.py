# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Resampling functions for microgrid data."""

import statistics
from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from datetime import datetime
from typing import assert_never

from ..timeseries import ResamplingContext, WindowSide


def latest_window_mean(
    input_samples: Sequence[tuple[datetime, float]],
    context: ResamplingContext,
    /,
) -> float:
    """Return the mean of the samples in the newest window that has samples.

    Samples from older windows are only used when all newer windows are empty. This
    way resampled values don't average several periods, but short gaps in a
    timeseries are covered with the newest values available. How old these values
    can be depends on
    [`max_data_age_in_periods`][frequenz.sdk.timeseries.ResamplerConfig3.max_data_age_in_periods].
    See [`initialize()`][frequenz.sdk.microgrid.initialize] for how to use it to
    control a microgrid.

    Windows are the resampling windows of the resampler, and
    [`closed`][frequenz.sdk.timeseries.ResamplerConfig3.closed] decides to which
    window a sample on a boundary belongs. The newest window with samples can have
    only some of its samples, for example when a fractional
    `max_data_age_in_periods` leaves out the oldest part of it.

    Warning: Limitation
        The resampler currently expects the samples of each timeseries to arrive
        in timestamp order. Samples that arrive out of order can make this
        function use the wrong window. See
        [frequenz-sdk-python#47](https://github.com/frequenz-floss/frequenz-sdk-python/issues/47).

    Example:
        ```python
        from datetime import timedelta

        from frequenz.sdk.microgrid import latest_window_mean
        from frequenz.sdk.timeseries import ResamplerConfig3

        config = ResamplerConfig3(
            timedelta(seconds=1),
            max_data_age_in_periods=3.0,
            resampling_function=latest_window_mean,
        )
        ```

    Args:
        input_samples: The samples to resample, sorted by timestamp. It must not
            be empty.
        context: The resampling window being calculated, the configuration of the
            resampler and the properties of the timeseries being resampled.

    Returns:
        The mean of the samples in the newest window that has samples.
    """
    # The newest sample is in the newest window with samples, and every sample
    # after the first one in that window is in it too, as samples are sorted and
    # never newer than the end of the current window.
    start, _ = context.window_bounds(context.window_offset(input_samples[-1][0]))
    match context.config.closed:
        case WindowSide.LEFT:
            first = bisect_left(input_samples, start, key=lambda s: s[0])
        case WindowSide.RIGHT:
            first = bisect_right(input_samples, start, key=lambda s: s[0])
        case unexpected:
            assert_never(unexpected)
    return statistics.fmean(s[1] for s in input_samples[first:])
