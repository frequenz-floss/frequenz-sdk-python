# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `frequenz.sdk.microgrid.*_pool` methods has been renamed to `new_*_pool`, to make it explicit that they create new instances of the pool classes.
  + `battery_pool` -> `new_battery_pool`
  + `ev_charger_pool` -> `new_ev_charger_pool`
  + `pv_pool` -> `new_pv_pool`

- The following component metric streams have been renamed to clarify that they stream per-phase values:
  + `frequenz.sdk.microgrid.`
    * `voltage` -> `voltage_per_phase`
    * `grid.current` -> `grid.current_per_phase`
    * `ev_charger_pool.current` -> `ev_charger_pool.current_per_phase`

## New Features

- Classes `Bounds` and `SystemBounds` now implement the `__contains__` method, allowing the use of the `in` operator to check whether a value falls within the bounds or not.

## Bug Fixes

- Fixed a typing issue that occurs in some cases when composing formulas with constants.
- Fixed a bug where sending tasks in the data sourcing actor might have not been properly awaited.
- Updated the logical meter documentation to reflect the latest changes.
