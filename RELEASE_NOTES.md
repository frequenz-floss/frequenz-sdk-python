# Release Notes

## Summary

## Upgrading

## New Features

* A new class `OrderedRingBuffer` is now available, providing a sorted ring buffer of datetime-value pairs with tracking of any values that have not yet been written.
* Add logical meter formula for EV power.
* A `MovingWindow` class has been added that consumes a data stream from a logical meter and updates an `OrderedRingBuffer`.
* Add EVChargerPool implementation. It has only streaming state changes for ev chargers, now.
* Add 3-phase current formulas: `3-phase grid_current` and `3-phase ev_charger_current` to the LogicalMeter.
* A new class `SerializableRingbuffer` is now available, extending the `OrderedRingBuffer` class with the ability to load & dump the data to disk.
* The datasourcing actor now automatically closes all sending channels when the input channel closes.
* The datasourcing actor no longer creates an extra task for every single sample and sender

## Bug Fixes

* Add COMPONENT_STATE_DISCHARGING as valid state for the inverter. DISCHARGING state was missing by mistake and this caused the power distributor to error out if the inverter is already discharging.
