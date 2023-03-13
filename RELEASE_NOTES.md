# Release Notes

## Summary

## Upgrading

* Remove `_soc` formula from the LogicalMeter. This feature has been moved to the BatteryPool.
* Upgrade PowerDistributingActor to handle components with the NaN metrics (#247):
    * if power bounds are NaN, then it tries to replace them with corresponding power bounds from adjacent component. If these components are also NaN, then it ignores battery.
    * if other metrics metrics are NaN then it ignores battery.
* BatteryStatus to track that a component stopped sending messages. If the battery or its adjacent inverter stopped sending messages, then the battery would be considered as not working. (#207)
* PowerDistributing to send battery status to subscribed users (#205)
* Rename microgrid.Microgrid to microgrid.ConnectionManager (#208)
* Change few resampler logs from info to debug, because they were polluting startup logs (#238)

## New Features

* A new class `SerializableRingbuffer` is now available, extending the `OrderedRingBuffer` class with the ability to load & dump the data to disk.
* Add the `run(*actors)` function for running and synchronizing the execution of actors. This new function simplifies the way actors are managed on the client side, allowing for a cleaner and more streamlined approach. Users/apps can now run actors simply by calling run(actor1, actor2, actor3...) without the need to manually call join() and deal with linting errors.
* The datasourcing actor now automatically closes all sending channels when the input channel closes.
* BatteryPool implementation for aggregating battery-inverter metrics into higher level metrics. (#205)
* Add EV power and current streams to `EVChargerPool` (#201)


## Bug Fixes

* The resampler now correctly produces resampling windows of exact *resampling period* size, which only include samples emitted during the resampling window (see #170)

## Removing
* Deprecated code (#232):
    * frequenz.sdk._data_ingestion
    * frequenz.sdk._data_handling
