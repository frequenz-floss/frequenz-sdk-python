# Frequenz Python SDK Release Notes

## New Features

- `Resampler`: The resampler can now be configured to have the resampling window closed to the right (default) or left, and to also set the resampler timestamp to the right (default) or left end of the window being resampled. You can configure setting the new options `closed` and `label` in the `ResamplerConfig`.

## Bug Fixes

- Improved formula validation: Consistent error messages for invalid formulas and conventional span semantics.

- This fixes a rare power distributor bug where some battery inverters becoming unreachable because of network outages would lead to excess power values getting set.  This is fixed by measuring the power of the unreachable inverters through their fallback meters and excluding that power from what is distributed to the other inverters.

- Fixed stopping formulas: It will now also stop the evaluator and sub-formulas correctly.
