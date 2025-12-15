# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

- The `FormulaEngine` is now replaced by a newly implemented `Formula` type.  This doesn't affect the high level interfaces.

- The `ComponentGraph` has been replaced by the `frequenz-microgrid-component-graph` package, which provides python bindings for the rust implementation.

## New Features

- The power manager algorithm for batteries can now be changed from the default ShiftingMatryoshka, by passing it as an argument to `microgrid.initialize()`

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
