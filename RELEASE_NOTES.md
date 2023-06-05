# Release Notes

## Summary

This release main introduces the new `PeriodicFeatureExtractor`, the control interface to the `BatteryPool`, and a new naming scheme for retrieving `LogicalMeter` and `BatteryPool` metrics. It also drops support for Python versions older than 3.11.

## Upgrading

* Now Python 3.11 is the minimum supported version.  All users must upgrade to Python 3.11 (including virtual environments used for development).

* `BatteryPool` metric streaming interfaces have changed for `soc`, `capacity` and `power_bounds`:

  ```python
  soc_rx = battery_pool.soc()    # old

  soc_rx = battery_pool.soc.new_receiver()    # new
  ```

* Formulas now follow the new naming scheme.

  - `BatteryPool.{power, production_power, consumption_power}`
  - `EVChargerPool.{power, production_power, consumption_power}`
  - `LogicalMeter`:
    - `consumer_power`
    - `grid_power`
    - `grid_production_power`
    - `grid_consumption_power`
    - `chp_power`
    - `chp_production_power`
    - `chp_consumption_power`

* A power request can now be forced by setting the `include_broken` attribute. This is especially helpful as a safety measure when components appear to be failing, such as when battery metrics are unavailable. Note that applications previously relying on automatic fallback to all batteries when none of them was working will now require the `include_broken` attribute to be explicitly set in the request.

* Now `float` is used everywhere for representing power (before power metrics were `float` but setting power was done using `int`).
  * `frequenz.sdk.actor.power_distributing`: the `power` attribute of the `Request` class has been updated from `int` to a `float`.
  * `frequenz.sdk.microgrid`: the `set_power()` method of both the `MicrogridApiClient` and `MicrogridGrpcClient` classes now expect a `float` value for the `power_w` parameter instead of `int`.

* The `LogicalMeter` no longer takes a `component_graph` parameter.

* Now `frequenz.sdk.timeseries.Sample` uses a more sensible comparison.  Before this release `Sample`s were compared only based on the `timestamp`.  This was due to a limitation in Python versions earlier than 3.10.  Now that the minimum supported version is 3.11 this hack is not needed anymore and `Sample`s are compared using both `timestamp` and `value` as most people probably expects.

* The dependency to `sympy` was unused and thus removed from the SDK.  If you used it indirectly without declaring the dependency in your project you should do it now.

## New Features

* The `MovingWindow` has new public methods that return the oldest and newest timestamp of all stored samples.

* The `PeriodicFeatureExtractor` has been added.

  This is a tool to create certain profiles out of periodic reoccurring windows inside a `MovingWindow`.

  As an example one can create a daily profile of specific weekdays which will be returned as numpy arrays.

* The `BatteryPool` can now be used to control the batteries in it via the new methods `charge()`, `discharge()`, and `set_power()`.

## Bug Fixes

* Fixed many examples in the documentation.
