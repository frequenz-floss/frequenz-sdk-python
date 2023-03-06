# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Remove `_soc` formula from the LogicalMeter. This feature has been moved to the BatteryPool.

## New Features

* A new class `SerializableRingbuffer` is now available, extending the `OrderedRingBuffer` class with the ability to load & dump the data to disk.
* Add the `run(*actors)` function for running and synchronizing the execution of actors. This new function simplifies the way actors are managed on the client side, allowing for a cleaner and more streamlined approach. Users/apps can now run actors simply by calling run(actor1, actor2, actor3...) without the need to manually call join() and deal with linting errors.
* The datasourcing actor now automatically closes all sending channels when the input channel closes.

## Bug Fixes

* The resampler now correctly produces resampling windows of exact *resampling period* size, which only include samples emitted during the resampling window (see #170)
