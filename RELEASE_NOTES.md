# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with --> 

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Formulas with repeated operators like `#1 - #2 - #3` were getting
  calculated incorrectly as `#1 - (#2 - #3)`.  This has been fixed in
  https://github.com/frequenz-floss/frequenz-sdk-python/pull/141
