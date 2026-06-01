# Frequenz Python SDK Release Notes

## Summary

Along with the new `tick_delay` resampler config option, this release also includes some performance improvements in the data pipeline.

## New Features

* A new `tick_delay` option was added to `ResamplerConfig` and `ResamplerConfig2` to delay resampling execution after each timer tick. The delay was designed to postpone processing while keeping window boundaries aligned to the original tick times, which can be used for cascaded resampling pipelines. This option is experimental and may be changed or deprecated in a future release.

## Bug Fixes

* Accept `ComponentDataSamples` that only carry state changes (the state changes were dropped before).
* Update microgrid client to v0.18.3+, which fixes a problem with missing steam boilers on formula generation.
