# Frequenz Python SDK Release Notes

## Summary

The most notable features for this release is the addition of the `PVPool` (exposed via `microgrid.pv_pool()`), which can be used to manage PV arrays as a single entity and the `EVChargerPool` (`microgrid.ev_charger_pool()`) learning to manage power for the whole pool (before it could only be used to control chargers individually).

Another notable change is the microgrid API client being moved to its own [repository](https://github.com/frequenz-floss/frequenz-client-microgrid-python/).

## Upgrading

- The SDK is now using the microgrid API client from [`frequenz-client-microgrid`](https://github.com/frequenz-floss/frequenz-client-microgrid-python/). You should update your code if you are using the microgrid API client directly.

- The minimum required `frequenz-channels` version is now [`v1.0.0-rc1`](https://github.com/frequenz-floss/frequenz-channels-python/releases/tag/v1.0.0-rc.1).

- The set of battery IDs managed by a battery pool are now available through `BatteryPool.component_ids`, and no longer through `BatteryPool.battery_ids`.  This is done to have a consistent interface with other `*Pool`s.

- The `maxsize` parameter in calls to `BatteryPool.{soc/capacity/temperature}.new_receiver()` methods have now been renamed to `limit`, to be consistent with the channels repository.

- Support for per-component interaction in `EVChargerPool` has been removed. Please use the new `propose_power()` method to manage power for the whole pool. If you still need to manage power of chargers individually, you can create one pool per charger.

- PV power is now available from `microgrid.pv_pool().power`, and no longer from `microgrid.logical_meter().pv_power`.

## New Features

- `EVChargerPool`/`microgrid.ev_charger_pool()`: New `propose_power` and `power_status` methods have been added, similar to the `BatteryPool`.  These method interface with the `PowerManager` and `PowerDistributor`, which currently uses a first-come-first-serve algorithm to distribute power to EVs.

- A PV pool (`PVPool`/`microgrid.pv_pool()`) was added, with `propose_power`, `power_status` and `power` methods similar to Battery and EV pools.

- The microgrid API client now exposes the reactive power for inverters, meters and EV chargers.

## Enhancements

- Warning messages are logged when multiple instances of `*Pool`s are created for the same set of batteries, with the same priority values.

- A warning message will now be logged if no relevant samples are found in a component for resampling.

## Bug Fixes

- A bug was fixed where the grid fuse was not created properly and would end up with a `max_current` with type `float` instead of `Current`.

- `BatteryPool.propose_discharge` now converts power values to the passive-sign convention.  Earlier it was not doing this and that was causing it to charge instead of discharge.

- Fix a bug that was causing the power managing actor to crash and restart when cleaning up old proposals.
