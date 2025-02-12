# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- The `MovingWindow` now has an async `wait_for_samples` method that waits for a given number of samples to become available in the moving window and then returns.

## Bug Fixes

- Fixed a bug that was preventing power proposals to go through if there once existed some proposals with overlapping component IDs, even if the old proposals have expired.
