# Release Notes

## Summary

The project has a new home!

https://frequenz-floss.github.io/frequenz-sdk-python/

For now the documentation is pretty scarce but we will be improving it with
time.

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

* `EVChargerData`'s `active_power_consumption` has been renamed to `active_power`

* `PowerDistributor` - type of `Request`'s `request_timeout_sec` has been changed from `int` to `float`

* `ComponentCategory.LOAD` has been added

* The
  [`frequenz-channels`](https://github.com/frequenz-floss/frequenz-channels-python/)
  was upgraded to
  [v0.11.0](https://github.com/frequenz-floss/frequenz-channels-python/releases/tag/v0.11.0)
  which includes a bunch of [breaking
  changes](https://github.com/frequenz-floss/frequenz-channels-python/blob/v0.11.0/RELEASE_NOTES.md#upgrading-breaking-changes)
  you should be aware of.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

* `MeterData` objects now expose the AC `frequency` measured by the meter.
* `BatteryData` objects now expose the temperature of the hottest block in the
  battery as `temperature_max`

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
