# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

* A new class `OrderedRingBuffer` is now available, providing a sorted ring buffer of datetime-value pairs with tracking of any values that have not yet been written.
* Add logical meter formula for EV power.
* A `MovingWindow` class has been added that consumes a data stream from a logical meter and updates an `OrderedRingBuffer`.

## Bug Fixes

* Add COMPONENT_STATE_DISCHARGING as valid state for the inverter. DISCHARGING state was missing by mistake and this caused the power distributor to error out if the inverter is already discharging.
<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
