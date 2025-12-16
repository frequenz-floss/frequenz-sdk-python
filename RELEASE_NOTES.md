# Frequenz Python SDK Release Notes

## Upgrading

- The `FormulaEngine` is now replaced by a newly implemented `Formula` type.  This doesn't affect the high level interfaces.  `FormulaEngine` is now a deprecated wrapper to `Formula`.

- The `ComponentGraph` has been replaced by the `frequenz-microgrid-component-graph` package, which provides python bindings for the rust implementation.

## New Features

- The power manager algorithm for batteries can now be changed from the default ShiftingMatryoshka, by passing it as an argument to `microgrid.initialize()`
