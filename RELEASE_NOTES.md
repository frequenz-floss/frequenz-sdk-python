# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- Add stop method to the FormulaEngine. Now it is possible to stop custom formulas.

- Stop fallback formulas when primary formula starts working again.

## Bug Fixes

- Fixed bug with formulas raising exception when stopped.

- Fix a bug that raised `CancelledError` when actor was started with `frequenz.sdk.actor.run` and stopped.

- Stop catching `BaseException` in `frequenz.sdk.actor.run`. Only `CancelledError` and `Exception` are caught now.