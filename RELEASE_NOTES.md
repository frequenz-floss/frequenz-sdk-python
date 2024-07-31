# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `frequenz.sdk.microgrid.*_pool` methods has been renamed to `new_*_pool`, to make it explicit that they create new instances of the pool classes.
  - `battery_pool` -> `new_battery_pool`
  - `ev_charger_pool` -> `new_ev_charger_pool`
  - `pv_pool` -> `new_pv_pool`

- The following component metric streams have been renamed to clarify that they stream per-phase values:
  - `frequenz.sdk.microgrid.`
    - `voltage` -> `voltage_per_phase`
    - `grid.current` -> `grid.current_per_phase`
    - `ev_charger_pool.current` -> `ev_charger_pool.current_per_phase`

- Passing a `request_timeout` in calls to `*_pool.propose_power` is no longer supported.  It may be specified at application startup, through the new optional `api_power_request_timeout` parameter in the `microgrid.initialize()` method.

- Power distribution results are no longer available through the `power_status` streams in the `*Pool`s.    They can now be accessed as a stream from a separate property `power_distribution_results`, which is available from all the `*Pool`s.

- The `ConfigManagingActor` now uses `collections.abc.Mapping` as the output sender type. This change indicates that the broadcasted configuration is intended to be read-only.

## New Features

- Classes `Bounds` and `SystemBounds` now implement the `__contains__` method, allowing the use of the `in` operator to check whether a value falls within the bounds or not.

## Enhancements

- The resampler now shows an error message where it is easier to identify the component and metric when it can't find relevant data for the current resampling window.

## Bug Fixes

- Fixed a typing issue that occurs in some cases when composing formulas with constants.
- Fixed a bug where sending tasks in the data sourcing actor might have not been properly awaited.
- Updated the logical meter documentation to reflect the latest changes.
- Fixed a bug in the code examples in the getting-started tutorial.
- Fixed a bug in `ConfigManagingActor` that was not properly comparing the event path to the config file path when the config file is a relative path.
- Re-expose `ComponentMetricId` to the docs.
- Fixed typing ambiguities when building composite formulas on streaming data.
- Fixed a bug that was causing the `PowerDistributor` to exit if power requests to PV inverters or EV chargers timeout.
- Fix handling of cancelled tasks in the data sourcing and resampling actor.
- Fix PV power distribution excluding inverters that haven't sent any data since startup.
