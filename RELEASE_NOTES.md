# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

This release adds `ResamplerConfig3`, a resampler configuration with defaults that don't surprise data analysis: when downsampling, each resampled value is the mean of its own window, labelled with the start of the window. It deprecates `ResamplerConfig` and `ResamplerConfig2`. For controlling a microgrid, the new `microgrid.latest_window_mean()` resampling function covers short gaps in the data without averaging several periods.

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

- `ResamplerConfig` and `ResamplingFunction` are deprecated. Use `ResamplerConfig3` and `ResamplingFunction3` instead. This is not a search and replace:

    - The resampler uses a wall clock timer with `ResamplerConfig3`, so `align_to` moves to `timer_config=WallClockTimerConfig.from_interval(resampling_period, align_to=...)`. The wall clock timer also resamples the incomplete window after a start ([#1472](https://github.com/frequenz-floss/frequenz-sdk-python/issues/1472)), and can skip or repeat timestamps when the wall clock jumps ([#1478](https://github.com/frequenz-floss/frequenz-sdk-python/issues/1478)).
    - `ResamplerConfig3` has different defaults, see below.
    - Only `resampling_period` can be passed by position.
    - `initial_buffer_len` is called `input_period_estimation_samples`.
    - Resampling functions receive a `ResamplingContext` instead of the configuration and the source properties, which are now `context.config` and `context.source_properties`.

- `ResamplerConfig2` and `ResamplingFunction2` are deprecated. Use `ResamplerConfig3` and `ResamplingFunction3` instead. `ResamplerConfig3` uses the same wall clock timer, but it has different defaults (see below), only `resampling_period` can be passed by position, `initial_buffer_len` is called `input_period_estimation_samples`, and resampling functions receive a `ResamplingContext` instead of the configuration and the source properties. Keep any `timer_config` and `tick_delay` you pass. Subclassing `ResamplerConfig2` also shows the deprecation warning of `ResamplerConfig`, whose advice about the timer doesn't apply to it.

- `ResamplerConfig3` has new defaults: `max_data_age_in_periods=1.0`, `closed=WindowSide.LEFT` and `label=WindowSide.LEFT`. The old classes use `3.0`, `RIGHT` and `RIGHT`. When downsampling, each value is now the mean of the samples in its own window only, instead of the last three periods, and it is labelled with the start of its window instead of its end, so timestamps are one period earlier. To keep the aggregation and the window conventions of the old classes, pass the old defaults explicitly:

    ```python
    from datetime import timedelta

    from frequenz.sdk.timeseries import ResamplerConfig3, WindowSide

    config = ResamplerConfig3(
        timedelta(seconds=1),
        max_data_age_in_periods=3.0,
        closed=WindowSide.RIGHT,
        label=WindowSide.RIGHT,
    )
    ```

- Changing `closed` and `label` can break code in subtle ways, without any error:

    - Timeseries resampled with different `label` values are off by one period, and with different `closed` values, samples exactly on a window boundary go to different windows. When they are combined, for example in a formula or when the output of a resampler is resampled again (like a `MovingWindow` with its own resampler fed by microgrid data), values from adjacent windows are silently paired. Use the same values for all resamplers whose output is combined, also while migrating only part of an application.
    - Code that compares the timestamps of resampled values with the current time sees values one period older, as values are still sent at the end of their window, or later with `tick_delay`.
    - Stored or exported values are labelled one period earlier after changing `label`, and the timestamp of the last value before the change is repeated by the first one after it.

- Applications that control a microgrid and relied on the 3-period mean of the old defaults to tolerate gaps in the data should use the new `microgrid.latest_window_mean()` with a `max_data_age_in_periods` bigger than 1.0. Its values are noisier than the old 3-period mean, as they are not averaged over several periods anymore, so controllers that relied on that smoothing will see more noise. `microgrid.EXAMPLE_RESAMPLER_CONFIG` shows a configuration like this.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

- `ResamplerConfig3` is a new resampler configuration. It always uses the wall clock timer, and has new defaults, see the upgrading notes.

- Resampling functions for `ResamplerConfig3` (`ResamplingFunction3`) receive a `ResamplingContext`, with the window being resampled, the configuration and the source properties. Its `window_end` property, and `window_bounds()` and `window_offset()` methods, give the boundaries of the current and previous windows and the window a sample belongs to.

- `microgrid.latest_window_mean()` is a resampling function that averages the samples in the newest window that has any. Older windows are only used when newer ones are empty, so short gaps in the data and devices that send data less often than the resampling period don't produce `None` values, as long as some samples are not older than `max_data_age_in_periods`.

- `microgrid.EXAMPLE_RESAMPLER_CONFIG` is a resampler configuration for examples and quick experiments. Applications that control a microgrid can use it as a baseline for their own configuration: it uses `microgrid.latest_window_mean()` with a `max_data_age_in_periods` of 3, and its documentation explains why.

- `SourceProperties` has a new `dropped_samples` field, with the number of samples the resampler dropped because its buffer was full.

- `SourceProperties` has new `total_windows_without_samples` and `current_windows_without_samples` fields, with the number of resampling windows without relevant samples in total and in the current gap in the data. They tell how healthy a timeseries is, and whether it is in a gap right now.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->

- The resampler no longer drops samples a window needs before its buffer holds `max_buffer_len` samples. This happened to the first windows after a start, which were resampled using only the last `initial_buffer_len` samples ([#1471](https://github.com/frequenz-floss/frequenz-sdk-python/issues/1471)), and to any window when a sample of the next window arrived before the window was resampled, for example because of `tick_delay`. Old samples are now removed when resampling, so the buffer only drops samples when it is full. Until the input sampling period is estimated, old samples are not removed, so the buffer can grow up to `max_buffer_len` samples. Because of this, these settings mean something slightly different now:

    - `initial_buffer_len` is the number of samples to receive before estimating the input sampling period.
    - `warn_buffer_len` logs a warning, once per timeseries, when a buffer holds more samples than this after removing old ones.
    - `max_buffer_len` logs an error, once per timeseries, when samples needed to resample may have been dropped because the buffer was full.

- The resampler no longer logs a warning at every resampling while a timeseries has no relevant samples. It logs it once when the samples stop, and logs when they are back, with the number of windows without them.

- The resampler no longer fails when `max_data_age_in_periods` is so big that the oldest relevant timestamp can't be represented. All samples are relevant then.
