# Frequenz Python SDK Release Notes

## Summary

This release provides an experimental, opt-in, time-jumps resilient resampler, that can be enabled by using the new `ResamplerConfig2` class.

## Upgrading

* The resampling function now takes plain `float`s as values instead of `Quantity` objects.
* `frequenz.sdk.timeseries.UNIX_EPOCH` was removed, use [`frequenz.core.datetime.UNIX_EPOCH`](https://frequenz-floss.github.io/frequenz-core-python/latest/reference/frequenz/core/datetime/#frequenz.core.datetime.UNIX_EPOCH) instead.

## New Features

- A new configuration mode was added to the resampler (and thus the resampling actor and microgrid high-level interface). When passing a new `ResamplerConfig2` instance to the resampler, it will use a wall clock timer instead of a monotonic clock timer. This timer adjustes sleeps to account for drifts in the monotonic clock, and thus allows for more accurate resampling in cases where the monotonic clock drifts away from the wall clock. The monotonic clock timer option will be deprecated in the future, as it is not really suitable for resampling. The new `ResamplerConfig2` class accepts a `WallClockTimerConfig` to fine-tune the wall clock timer behavior, if necessary.

   Example usage:

   ```python
   from frequenz.sdk import microgrid
   from frequenz.sdk.timeseries import ResamplerConfig2

    await microgrid.initialize(
        MICROGRID_API_URL,
        # Just replace the old `ResamplerConfig` with the new `ResamplerConfig2`
        resampler_config=ResamplerConfig2(resampling_period=timedelta(seconds=1.0)),
    )
    ```

## Bug Fixes

- When using the new wall clock timer in the resampmler, it will now resync to the system time if it drifts away for more than a resample period, and do dynamic adjustments to the timer if the monotonic clock has a small drift compared to the wall clock.

- A power distributor logging issue is fixed, that was causing the power for multiple batteries connected to the same inverter to be reported incorrectly.
