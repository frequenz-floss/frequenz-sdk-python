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
* After a power request to a battery's inverter fails or times out, the next request that includes that inverter is now always sent, even if it is 0 W. Before, a 0 W request was skipped when the last successful request to that inverter was also 0 W. The failed request may have been applied anyway, so the battery could keep running at that request's power. As before, a battery whose request failed is left out of new requests for a while: 1 second at first, and up to 30 seconds if its requests keep failing. This only happens while another battery in the new request is working. If none is, the failed battery is used again right away.
