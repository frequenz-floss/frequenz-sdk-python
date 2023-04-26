# Release Notes

## Summary

This release drops support for Python versions older than 3.11.

## Upgrading

* Now Python 3.11 is the minimum supported version.  All users must upgrade to Python 3.11 (including virtual environments used for development).

* Now `float` is used everywhere for representing power (before power metrics were `float` but setting power was done using `int`).
  * `frequenz.sdk.actor.power_distributing`: the `power` attribute of the `Request` class has been updated from `int` to a `float`.
  * `frequenz.sdk.microgrid`: the `set_power()` method of both the `MicrogridApiClient` and `MicrogridGrpcClient` classes now expect a `float` value for the `power_w` parameter instead of `int`.

* The `LogicalMeter` no longer takes a `component_graph` parameter.

* Now `frequenz.sdk.timeseries.Sample` uses a more sensible comparison.  Before this release `Sample`s were compared only based on the `timestamp`.  This was due to a limitation in Python versions earlier than 3.10.  Now that the minimum supported version is 3.11 this hack is not needed anymore and `Sample`s are compared using both `timestamp` and `value` as most people probably expects.

* `BatteryPool` metric streaming interfaces have changed for `soc`, `capacity` and `power_bounds`:

  ```python
  soc_rx = battery_pool.soc()    # old

  soc_rx = battery_pool.soc.new_receiver()    # new
  ```

* A power request can now be forced by setting the `include_broken` attribute. This is especially helpful as a safety measure when components appear to be failing, such as when battery metrics are unavailable. Note that applications previously relying on automatic fallback to all batteries when none of them was working will now require the `include_broken` attribute to be explicitly set in the request.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
