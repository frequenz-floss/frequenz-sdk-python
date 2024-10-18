# Frequenz Python SDK Release Notes

## Summary

The SDK starts using the [`frequenz-quantities`](https://pypi.org/project/frequenz-quantities/) package with this release.

A new method for streaming reactive power at the grid connection point has been added, and the `ConfigManagingActor` has been improved.

## Upgrading

- Replace `Quantity` and its sub-classes (`Power`, `Current`, etc.) in the `frequenz.sdk.timeseries` module with the external [`frequenz-quantities`](https://pypi.org/project/frequenz-quantities/) package. Please add the new library as a dependency and adapt your imports if you are using these types.
- The `QuantityT` has been moved to the `frequenz.sdk.timeseries._base_types` module.
- The `QuantityT` doesn't include itself (`QuantityT`) anymore.

## New Features

- `ConfigManagingActor`: The file polling mechanism is now forced by default. Two new parameters have been added:
  - `force_polling`: Whether to force file polling to check for changes. Default is `True`.
  - `polling_interval`: The interval to check for changes. Only relevant if polling is enabled. Default is 1 second.

- Add a new method `microgrid.grid().reactive_power` to stream reactive power at the grid connection point.

## Bug Fixes

- Many long running async tasks including metric streamers in the BatteryPool now have automatic recovery in case of exceptions.
