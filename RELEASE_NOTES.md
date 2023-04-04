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

## Bug Fixes

* Change PowerDistributor to use all batteries if none is working (#258)
* Update the ordered ring buffer to fix the len() function so that it returns a value equal to or greater than zero, as expected (#274)
