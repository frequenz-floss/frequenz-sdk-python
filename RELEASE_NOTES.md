# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `BatteryPool.power_status` method now streams objects of type `BatteryPoolReport`, replacing the previous `Report` objects.

- In `BatteryPoolReport.distribution_result`,
  * the following fields have been renamed:
    + `Result.succeeded_batteries` → `Result.succeeded_components`
    + `Result.failed_batteries` → `Result.failed_components`
    + `Request.batteries` → `Request.component_ids`
  * and the following fields are now type-hinted as `collections.abc.Set`, to clearly indicate that they are read-only:
    + `Result.succeeded_components`
    + `Result.failed_components`


## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
