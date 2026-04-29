# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Transient component data outages (e.g. Modbus timeouts) no longer collapse
  the reported system power bounds to `[0, 0]`.  Previously the
  `PowerManagingActor` propagated `inclusion_bounds=None` through to the
  Matryoshka allocation algorithm which clamped it to zero, causing all
  downstream actors to drop their setpoints.  The last known good bounds are
  now retained when the bounds calculator emits `None` due to stale data.
  (Fixes [#1396](https://github.com/frequenz-floss/frequenz-sdk-python/pull/1396))
