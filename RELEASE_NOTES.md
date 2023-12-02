# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `BatteryPool.power_status` method now streams objects of type `BatteryPoolReport`, replacing the previous `Report` objects.

- `Channels` has been upgraded to version 1.0.0b2, for information on how to upgrade please read [Channels release notes](https://github.com/frequenz-floss/frequenz-channels-python/releases/tag/v1.0.0-beta.2).

- In `BatteryPoolReport.distribution_result`,
  * the following fields have been renamed:
    + `Result.succeeded_batteries` → `Result.succeeded_components`
    + `Result.failed_batteries` → `Result.failed_components`
    + `Request.batteries` → `Request.component_ids`
  * and the following fields are now type-hinted as `collections.abc.Set`, to clearly indicate that they are read-only:
    + `Result.succeeded_components`
    + `Result.failed_components`


- The `Fuse` class has been moved to the `frequenz.sdk.timeseries` module.

- `microgrid.grid()`
  - A `Grid` object is always instantiated now, even if the microgrid is not connected to the grid (islanded microgrids).
  - The rated current of the grid fuse is set to `Current.zero()` in case of islanded microgrids.
  - The grid fuse is set to `None` when the grid connection component metadata lacks information about the fuse.
  - Grid power and current metrics were moved from `microgrid.logical_meter()` to `microgrid.grid()`.

    Previously,

    ```python
    logical_meter = microgrid.logical_meter()
    grid_power_recv = logical_meter.grid_power.new_receiver()
    grid_current_recv = logical_meter.grid_current.new_receiver()
    ```

    Now,

    ```python
    grid = microgrid.grid()
    grid_power_recv = grid.power.new_receiver()
    grid_current_recv = grid.current.new_receiver()
    ```

- The `ComponentGraph.components()` parameters `component_id` and `component_category` were renamed to `component_ids` and `component_categories`, respectively.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- 0W power requests are now not adjusted to exclusion bounds by the `PowerManager` and `PowerDistributor`, and are sent over to the microgrid API directly.
- `timeseries.resampling` will now resync to the system time if it drifts away for more then a resample period.
