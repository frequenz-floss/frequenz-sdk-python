# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Update the microgrid component graph library to v0.5.0.  The per-category formulas (battery, PV, CHP, EV charger, ...) now read the component first and use the meter as the fallback.  This is the opposite of the old order, so generated formula strings and power values can change.  To keep the old order, pass `ComponentGraphConfig(prefer_meters_in_component_formulas=True)` to `microgrid.initialize()`.

* Graph validation errors now use a new message format.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
