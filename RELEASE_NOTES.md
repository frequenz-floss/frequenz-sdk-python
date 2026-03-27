# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- `Resampler`: The resampler can now be configured to have the resampling window closed to the right (default) or left, and to also set the resampler timestamp to the right (default) or left end of the window being resampled. You can configure setting the new options `closed` and `label` in the `ResamplerConfig`.
- `EventResampler`: A new event-driven resampler for cascaded resampling stages. Unlike the timer-based `Resampler`, `EventResampler` emits windows when sample timestamps exceed window boundaries, eliminating data loss at window boundaries in cascaded scenarios. See the class documentation for usage guidelines.
- `StreamingHelper`: Added callback mechanism via `register_sample_callback()` to notify external consumers when samples arrive, enabling event-driven resampling without polling internal buffers.
- `Resampler._emit_window()`: Extracted window emission logic into a dedicated method for code sharing between timer-based and event-driven resampler implementations.


## Bug Fixes

- Improved formula validation: Consistent error messages for invalid formulas and conventional span semantics.

- This fixes a rare power distributor bug where some battery inverters becoming unreachable because of network outages would lead to excess power values getting set.  This is fixed by measuring the power of the unreachable inverters through their fallback meters and excluding that power from what is distributed to the other inverters.

- Fixed stopping formulas: It will now also stop the evaluator and sub-formulas correctly.
