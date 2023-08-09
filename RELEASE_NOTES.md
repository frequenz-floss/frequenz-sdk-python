# Frequenz Python SDK Release Notes

## Summary

Now the microgrid API v0.15.x is being used. The SDK can only connect to microgrids using this version of the API. Inclusion and exclusion bounds in the new API are now handled by the power distributor and battery pool.

## Upgrading

- Upgrade to microgrid API v0.15.1.  If you're using any of the lower level microgrid interfaces, you will need to upgrade your code.
- The argument `conf_file` of the `ConfigManagingActor` constructor was renamed to `config_path`.

- The `BatteryPool.power_bounds` method now streams inclusion/exclusion bounds.  The bounds are now represented by `Power` objects and not `float`s.

## New Features

- The `ConfigManagingActor` constructor now can accept a `pathlib.Path` as `config_path` too (before it accepted only a `str`).

- The `PowerDistributingActor` now considers exclusion bounds, when finding an optimal distribution for power between batteries.
