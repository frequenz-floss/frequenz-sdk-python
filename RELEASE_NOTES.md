# Frequenz Python SDK Release Notes

## New Features

- The `MovingWindow` now has an async `wait_for_samples` method that waits for a given number of samples to become available in the moving window and then returns.

- Add stop method to the FormulaEngine. Now it is possible to stop custom formulas.

- Stop fallback formulas when primary formula starts working again.

## Bug Fixes

- Fixed a bug that was preventing power proposals to go through if there once existed some proposals with overlapping component IDs, even if the old proposals have expired.

- Fixed a bug that was causing formulas to fallback to CHPs, when the CHP meters didn't have data.  CHPs are not supported in the data sourcing actor and in the client, so we can't fallback to CHPs.

- Fixed bug with formulas raising exception when stopped.