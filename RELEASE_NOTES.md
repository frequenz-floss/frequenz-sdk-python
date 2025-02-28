# Frequenz Python SDK Release Notes

## Summary

## New Features

- Add a `stop()` method to the `FormulaEngine`. Now it is possible to stop custom formulas.

- Stop fallback formulas when primary formula starts working again.

## Bug Fixes

- Fixed a bug with formulas raising exception when stopped.

- Fixed a bug that raised `CancelledError` when actor was started with `frequenz.sdk.actor.run` and stopped.

- Stop catching `BaseException` in `frequenz.sdk.actor.run`. Only `CancelledError` and `Exception` are caught now.
