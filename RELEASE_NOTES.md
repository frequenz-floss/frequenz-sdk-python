# Release Notes

## Summary

This release improves the performance of the `ComponentMetricsResamplingActor`, and fixes a bug in the `LogicalMeter` that was causing it to crash.

## Upgrading

- `frequenz.sdk.timseries`:
  - The resample classes (`GroupResampler`, `Resampler`, `ResamplingFunction`) where removed and the replacement classes are hidden for now, as they are going through a rewrite.

## New Features

- New in `frequenz.sdk.timeseries`:

  - `Source` and `Sink` types to work generically with streaming timeseries.

- New `frequenz.sdk.timeseries.resampling` module with:
  - `Resampler` class that works on timseries `Source` and `Sink`.
  - `ResamplingFunction` (moved from `frequenz.sdk.timeseries`).
  - `ResamplingError` and `SourceStoppedError` exceptions.
  - `average` function (the default resampling function).

## Bug Fixes

- The Resampling actor sends None values out when there is no data from a component. The logical meter used to raise an exception if it saw a `None` value from any of its streams.  The logical meter is now able to handle `None` values by propagating the `None` values,  or treating them as `0`s.
