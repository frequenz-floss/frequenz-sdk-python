# Frequenz Python SDK Release Notes

## Summary

This version ships an experimental version of the **Power Manager**, adds preliminary support for n:m relations between inverters and batteries and includes user documentation.

## Upgrading

- `microgrid.battery_pool()` method now accepts a priority value.

- `BatteryPool`'s control methods

  * Original methods `{set_power/charge/discharge}` are now replaced by `propose_{power/charge/discharge}`
  * The `propose_*` methods send power proposals to the `PowerManagingActor`, where it can be overridden by proposals from other actors.
  * They no longer have the `adjust_power` flag, because the `PowerManagingActor` will always adjust power to fit within the available bounds.

- `BatteryPool`'s reporting methods

  * `power_bounds` is replaced by `power_status`
  * The `power_status` method streams objects containing:
    + bounds adjusted to the actor's priorities
    + the latest target power for the set of batteries
    + the results from the power distributor for the last request

## New Features

- New and improved documentation.

  * A new *User Guide* section was added, with:

    + A glossary.
    + An introduction to actors.

  * A new *Tutorials* section was added, with:

    + A getting started tutorial.

- In `OrderedRingBuffer`:
  - Rename `datetime_to_index` to `to_internal_index` to avoid confusion between the internal index and the external index.
  - Add `index_to_datetime` method to convert external index to corresponding datetime.
  - Remove `__setitem__` method to enforce usage of dedicated `update` method only.
- In `OrderedRingBuffer` and `MovingWindow`:
  - Support for integer indices is added.
  - Add `count_covered` method to count the number of elements covered by the used time range.
  - Add `fill_value` option to window method to impute missing values. By default missing values are imputed with `NaN`.
- Add `at` method to `MovingWindow` to access a single element and use it in `__getitem__` magic to fully support single element access.

- The PowerDistributingActor now supports n:m relations between inverters and batteries.

  This means that one or more inverters can be connected to one or more batteries.

- A `PowerManagingActor` implementation

## Bug Fixes

- Fix rendering of diagrams in the documentation.
- The `__getitem__` magic of the `MovingWindow` is fixed to support the same functionality that the `window` method provides.
- Fixes incorrect implementation of single element access in `__getitem__` magic of `MovingWindow`.
