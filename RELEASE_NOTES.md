# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Update the microgrid component graph library to v0.5.0.  The per-category formulas (battery, PV, CHP, EV charger, ...) now read the component first and use the meter as the fallback.  This is the opposite of the old order, so generated formula strings and power values can change.  To keep the old order, pass `ComponentGraphConfig(prefer_meters_in_component_formulas=True)` to `microgrid.initialize()`.

* Graph validation errors now use a new message format.

## New Features

* `microgrid.initialize()` has a new keyword argument `component_graph_config`.  It takes a `ComponentGraphConfig` and gives full control over how the component graph is built.  For example, pass `ComponentGraphConfig(prefer_meters_in_component_formulas=True)` to make the per-category formulas read from the meter first, as in previous releases.  `ComponentGraphConfig` and `FormulaOverrides` are re-exported from `frequenz.sdk.microgrid`.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
