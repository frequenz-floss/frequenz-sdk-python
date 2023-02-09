# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* A new class `SerializableRingbuffer` is now available, extending the `OrderedRingBuffer` class with the ability to load & dump the data to disk.
* Add the `run(*actors)` function for running and synchronizing the execution of actors. This new function simplifies the way actors are managed on the client side, allowing for a cleaner and more streamlined approach. Users/apps can now run actors simply by calling run(actor1, actor2, actor3...) without the need to manually call join() and deal with linting errors.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
