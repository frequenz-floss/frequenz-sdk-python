# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->

* Make the PowerManager fall back to zero instead of changing the requested power direction when a target is snapped outside asymmetric exclusion bounds.
* The battery pool no longer drops the cached data of batteries that stop working. A battery that started working again was left out of the pool's metrics until its next data sample arrived. When every working battery had just started working again, `system_power_bounds` briefly reported no bounds, and if an actor had requested power, the power manager set the target power to zero.
