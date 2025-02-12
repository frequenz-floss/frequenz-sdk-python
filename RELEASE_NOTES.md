# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Fixed a bug that was preventing power proposals to go through if there once existed some proposals with overlapping component IDs, even if the old proposals have expired.

- Fixed a bug that was causing formulas to fallback to CHPs, when the CHP meters didn't have data.  CHPs are not supported in the data sourcing actor and in the client, so we can't fallback to CHPs.
