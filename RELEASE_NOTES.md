# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- `Resampler`: The resampler can now be configured to have the resampling window closed to the right (default) or left, and to also set the resampler timestamp to the right (default) or left end of the window being resampled. You can configure setting the new options `closed` and `label` in the `ResamplerConfig`.

## Bug Fixes

- Improved formula validation: Consistent error messages for invalid formulas and conventional span semantics.
