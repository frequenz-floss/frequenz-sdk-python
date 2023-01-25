# Release Notes

## Summary

## Upgrading

* The resampler now takes a `name` argument for `add_timeseries()`. This is only used for logging purposes.

* The resampler and resampling actor now takes a `ResamplerConfig` object in the constructor instead of the individual values.

* The resampler and resampling actor can emit errors or warnings if the buffer needed to resample is too big. If it is bigger than `ResamplingConfig.max_buffer_len`, the buffer will be truncated to that length, so the resampling can lose accuracy.

* The `ResamplingFunction` now takes different arguments:

  * `resampling_period_s` was removed.
  * `resampler_config` is the configuration of the resampler calling the resampling function.
  * `source_properties` is the properties of the source being resampled.

* Update frequenz-channel-python dependency to the latest release v0.12.0

* The MetricFetcher now propagates `NaN` to handle None values when None are not treated from the stream as 0s. Then any FormulaStep can compute the results without checking for None on each value involved. However the final result is written as None rather than NaN/INF in the FormulaEngine.

## New Features

* The resampler and resampling actor can now take a few new options via the new `ResamplerConfig` object:

  * `warn_buffer_len`: The minimum length of the resampling buffer that will emit a warning.
  * `max_buffer_len`: The maximum length of the resampling buffer.

* The resampler now infers the input sampling rate of sources and use a buffer size according to it.

  This information can be consulted via `resampler.get_source_properties(source)`. The buffer size is now calculated so it can store all the needed samples requested via the combination of `resampling_period_s`, `max_data_age_in_periods` and the calculated `input_sampling_period_s`.

  If we are upsampling, one sample could be enough for back-filling, but we store `max_data_age_in_periods` using `input_sampling_period_s` as period, so the resampling functions can do more complex inter/extrapolation if they need to.

  If we are downsampling, we want a buffer that can hold `max_data_age_in_periods * resampling_period_s` seconds of data, and we have one sample every `input_sampling_period_s`, so we use a buffer length of: `max_data_age_in_periods * resampling_period_s / input_sampling_period_s`

## Bug Fixes

* Fixed logger creationg for some modules.

  Some modules didn't create the logger properly so there was no way to configure them using the standard logger configuration system. Because of this, it might have happened that some log messages were never showed, or some message that the user didn't want to get were emitted anyway.

* When automatically generating formulas for calculating grid power, include measurements from EV chargers if any are directly connected to the grid.
