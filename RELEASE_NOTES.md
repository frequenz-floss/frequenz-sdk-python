# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- Replace `Quantity` and its sub-classes (`Power`, `Current`, etc.) in the `frequenz.sdk.timeseries` module with the external
[`frequenz-quantities`](https://pypi.org/project/frequenz-quantities/) package. Please add the new library as a dependency
and adapt your imports if you are using these types.
- The `QuantityT` has been moved to the `frequenz.sdk.timeseries._base_types` module.
- The `QuantityT` doesn't include itself (`QuantityT`) anymore.

## New Features

- `ConfigManagingActor`: The file polling mechanism is now forced by default. Two new parameters have been added:
  - `force_polling`: Whether to force file polling to check for changes. Default is `True`.
  - `polling_interval`: The interval to check for changes. Only relevant if polling is enabled. Default is 1 second.

## Bug Fixes

- Many long running async tasks including metric streamers in the BatteryPool now have automatic recovery in case of exceptions.
