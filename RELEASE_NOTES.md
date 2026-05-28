# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* A new `tick_delay` option was added to `ResamplerConfig` and `ResamplerConfig2` to delay resampling execution after each timer tick. The delay was designed to postpone processing while keeping window boundaries aligned to the original tick times, which can be used for cascaded resampling pipelines. This option is experimental and may be changed or deprecated in a future release.

## Bug Fixes

* Accept `ComponentDataSamples` that only carry state changes (the state changes were dropped before).
