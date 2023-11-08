# Frequenz Python SDK Release Notes

## Summary

The `microgrid` package now exposes grid connections uniformly and introduces formula operators for `consumption` and `production`, replacing the `logical_meter.*_{production,consumption}()` formulas. The `actor` package restarts crashed actors with a delay, and the `ConnectionManager` exposes the `microgrid_id` and `location` details.

There are also a few bug fixes, documentation improvements and other minor breaking changes.

## Upgrading

- `actor` package

  * Actors are now restarted after a small delay when they crash to avoid a busy loop and spamming the logs if the actor keeps failing to start.

  * The `include_broken_batteries` argument was removed from the `PowerDistributingActor`'s `Request`. This option is no longer supported.

- `microgrid` package

  * `grid`: The grid connection is now exposed as `microgrid.grid()`. This is more consistent with other objects exposed in the `microgrid` module, such as `microgrid.battery_pool()` and `microgrid.logical_meter()`.

  * `battery_pool()`: The `include_broken_batteries` argument was removed from the `propose_*()` methods (it was also removed from the underlying type, `timeseries.BatteryPool`). This option is no longer supported.

  * `ComponentGraph`: The component graph is now exposed as `microgrid.component_graph.ComponentGraph`.

  * `logical_meter()`: The `*_consumption()` and `*_production()` methods were removed. You should use the new `consumption` and `production` formula operators instead.

    For example:

    ```python
    # Old:
    pv_consumption = logical_meter.pv_consumption_power()
    production = (logical_meter.pv_production_power() + logical_meter.chp_production_power()).build()
    # New:
    pv_consumption = logical_meter.pv_power().consumption().build()
    production = (logical_meter.pv_power().production() + logical_meter.chp_power().production()).build()
    ```

## New Features

- The configuration flag `resend_latest` can now be changed for channels owned by the `ChannelRegistry`.

- New formula operators for calculating `consumption()` and `production()` were added.

- The `ConnectionManager` now fetches microgrid metadata when connecting to the microgrid and exposes `microgrid_id` and `location` properties of the connected microgrid.

  Users can access this information using `microgrid.connection_manager.get().microgrid_id` and `microgrid.connection_manager.get().location`.

- The documentation has been improved to:

  * Display signatures with types.
  * Show inherited members.
  * Publish documentation for pre-releases.
  * Present the full tag name as the documentation version.
  * Ensure all development branches have their documentation published (the `next` version has been removed).
  * Fix the order of the documentation versions.

## Bug Fixes

- Fixed incorrect grid current calculations in locations where the calculations depended on current measurements from an inverter.

- Corrected the power failure report to exclude any failed power calculations from the successful ones.
