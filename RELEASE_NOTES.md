# Release Notes

## Summary

## Upgrading

## New Features

- Upgrade PowerDistributingActor to track if batteries are working
  https://github.com/frequenz-floss/frequenz-sdk-python/pull/117

- Simplify `Resampler` by moving configuration to its own class `ResamplerConfig`
  https://github.com/frequenz-floss/frequenz-sdk-python/pull/131

- Add `initial_buffer_len` to the ResamplerConfig. This means that `Resampler` will store at least this number of arguments. Other `Resampler` behaviour won't change
  https://github.com/frequenz-floss/frequenz-sdk-python/pull/131

- Ability to compose formula outputs into higher-order formulas:
  https://github.com/frequenz-floss/frequenz-sdk-python/pull/133

- Add a formula generator for SoC in the LogicalMeter
  https://github.com/frequenz-floss/frequenz-sdk-python/pull/137


## Bug Fixes

- Formulas with repeated operators like `#1 - #2 - #3` were getting
  calculated incorrectly as `#1 - (#2 - #3)`.  This has been fixed in
  https://github.com/frequenz-floss/frequenz-sdk-python/pull/141
