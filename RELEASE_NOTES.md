# Frequenz Python SDK Release Notes

## Summary

This release updates the microgrid component graph library to v0.5.0.  Formulas now fall back to more sources: a component that shares a meter with other components can be measured as that meter minus its siblings, so a formula can still return a value where it used to return nothing.

## Upgrading

* Update the microgrid component graph library to v0.5.0.  The per-category formulas (battery, PV, CHP, EV charger, ...) now read the component first and use the meter as the fallback.  This is the opposite of the old order, so generated formula strings and power values can change.  To keep the old order, pass `ComponentGraphConfig(prefer_meters_in_component_formulas=True)` to `microgrid.initialize()`.

* Graph validation errors now use a new message format.

## New Features

* Formulas now fall back to more sources.  A component that shares a meter with other components can be measured as that meter minus its siblings, which also covers diamond topologies, and a meter can fall back to the components under it.  So a formula can still return a value when some readings are missing.

* `microgrid.initialize()` has a new keyword argument `component_graph_config`.  It takes a `ComponentGraphConfig` and gives full control over how the component graph is built.  `ComponentGraphConfig` and `FormulaOverrides` are re-exported from `frequenz.sdk.microgrid`.
