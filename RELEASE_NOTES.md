# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `frequenz.sdk.timseries`:
  - The resample classes in the `frequenz.sdk.timseries` were removed. Use the new `frequenz.sdk.timseries.resampling.Resampler` class instead.
  - The `ResamplingFunction` was moved to the new module `frequenz.sdk.timseries.resampling`.

- `frequenz.sdk.actor.ComponentMetricsResamplingActor`:
  - The constructor now requires to pass all arguments as keywords.
  - The following constructor arguments were renamed to make them more clear:
    - `subscription_sender` -> `data_sourcing_request_sender`
    - `subscription_receiver` -> `resampling_request_receiver`


## New Features

- New in `frequenz.sdk.timeseries`:

  - `Source` and `Sink` types to work generically with streaming timeseries.

- New `frequenz.sdk.timeseries.resampling` module with:
  - `Resampler` class that works on timseries `Source` and `Sink`.
  - `ResamplingFunction` (moved from `frequenz.sdk.timeseries`).
  - `ResamplingError` and `SourceStoppedError` exceptions.
  - `average` function (the default resampling function).

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
