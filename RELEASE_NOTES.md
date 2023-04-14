# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Update BatteryStatus to mark battery with unknown capacity as not working (#263)
* The channels dependency was updated to v0.14.0 (#292)
* Some properties for `PowerDistributingActor` results were renamed to be more consistent between `Success` and `PartialFailure`:
  * The `Success.used_batteries` property was renamed to `succeeded_batteries`.
  * The `PartialFailure.success_batteries` property was renamed to `succeeded_batteries`.
  * The `succeed_power` property was renamed to `succeeded_power` for both `Success` and `PartialFailure`.
* Update MovingWindow to accept size parameter as timedelta instead of int (#269).
  This change allows users to define the time span of the moving window more intuitively, representing the duration over which samples will be stored.
* Add a resampler in the MovingWindow to control the granularity of the samples to be stored in the underlying buffer (#269).
  Notice that the parameter `sampling_period` has been renamed to `input_sampling_period`
  to better distinguish it from the sampling period parameter in the resampler.
* The serialization feature for the ringbuffer was made more flexible. The `dump` and `load` methods can now work directly with a ringbuffer instance.
* The `ResamplerConfig` now takes the resampling period as a `timedelta`. The configuration was renamed from `resampling_period_s` to `resampling_period` accordingly.

## New Features

* Automatic creation of core data-pipeline actors, to eliminate a lot
  of boiler plate code.  This makes it much simpler to deploy apps
  (#270).  For example:

  ``` python
  async def run():
      await microgrid.initialize(
          host=HOST, port=PORT, resampler_config=ResamplerConfig(resampling_period_s=1.0)
      )
      grid_power = microgrid.logical_meter().grid_power()
  ```

* The `Result` class (and subclasses) for the `PowerDistributingActor` are now dataclasses, so logging them will produce a more detailed output.

## Bug Fixes

* Change PowerDistributor to use all batteries if none is working (#258)
* Update the ordered ring buffer to fix the len() function so that it returns a value equal to or greater than zero, as expected (#274)
