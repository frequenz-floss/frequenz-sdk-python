# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Fixed an off-by-one calculation in `OrderedRingBuffer.count_covered` by switching to integer timedelta division, ensuring accurate sample counting for all window sizes and sampling periods.
