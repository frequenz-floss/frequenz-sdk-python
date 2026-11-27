# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

- `SourceProperties` has a new `dropped_samples` field, with the number of samples the resampler dropped because its buffer was full.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->

- The resampler no longer drops samples a window needs before its buffer holds `max_buffer_len` samples. This happened to the first windows after a start, which were resampled using only the last `initial_buffer_len` samples ([#1471](https://github.com/frequenz-floss/frequenz-sdk-python/issues/1471)), and to any window when a sample of the next window arrived before the window was resampled, for example because of `tick_delay`. Old samples are now removed when resampling, so the buffer only drops samples when it is full. Until the input sampling period is estimated, old samples are not removed, so the buffer can grow up to `max_buffer_len` samples. Because of this, these settings mean something slightly different now:

    - `initial_buffer_len` is the number of samples to receive before estimating the input sampling period.
    - `warn_buffer_len` logs a warning, once per timeseries, when a buffer holds more samples than this after removing old ones.
    - `max_buffer_len` logs an error, once per timeseries, when samples needed to resample may have been dropped because the buffer was full.

- The resampler no longer logs a warning at every resampling while a timeseries has no relevant samples. It logs it once when the samples stop, and logs when they are back, with the number of windows without them.
