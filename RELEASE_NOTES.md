# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Battery power is no longer available through the `LogicalMeter`, but through the `BatteryPool` (#338)

  ``` python
  battery_power_receiver = microgrid.battery_pool().power.new_receiver()
  ```

* Formulas composition has changed (#327)
  * Receivers from formulas are no longer composable.
  * Formula composition is now done by composing FormulaEngine instances.
  * Automatic formulas from the logical meter and \*pools, are now properties, and return `FormulaEngine` instances, which can be composed further, or can provide a receiver to fetch values.

  ``` python
  grid_power_receiver = microgrid.logical_meter().grid_power.new_receiver()

  self._inverter_power = (
      microgrid.logical_meter().pv_power
      + microgrid.battery_pool().power
  ).build("inverter_power")

  inverter_power_receiver = self._inverter_power.new_receiver()
  ```

* Update `BatteryStatus` to mark battery with unknown capacity as not working (#263)

* The channels dependency was updated to v0.14.0 (#292)

* Some properties for `PowerDistributingActor` results were renamed to be more consistent between `Success` and `PartialFailure`:

  * The `Success.used_batteries` property was renamed to `succeeded_batteries`.
  * The `PartialFailure.success_batteries` property was renamed to `succeeded_batteries`.
  * The `succeed_power` property was renamed to `succeeded_power` for both `Success` and `PartialFailure`.

* `MovingWindow`

  * The class is now publicly available in the `frequenz.sdk.timeseries` package.

  * Accept the `size` parameter as `timedelta` instead of `int` (#269).

    This change allows users to define the time span of the moving window more intuitively, representing the duration over which samples will be stored.

  * The input data will be resampled if a `resampler_config` is passed (#269).

    This allows controlling the granularity of the samples to be stored in the underlying buffer.

    Note that the parameter `sampling_period` has been renamed to `input_sampling_period` to better distinguish it from the sampling period parameter in the `resampler_config`.

  * Rename the constructor argument `window_alignment` to `align_to` and change the default to `UNIX_EPOCH`. This is to make it more consistent with the `ResamplerConfig`.

* `Resampler`

  * The `ResamplerConfig` class is now publicly available in the `frequenz.sdk.timeseries` package.

  * The `ResamplerConfig` now takes the resampling period as a `timedelta`. The configuration was renamed from `resampling_period_s` to `resampling_period` accordingly.

  * The `SourceProperties` of the resampler now uses a `timedelta` for the input sampling period. The attribute was renamed from `sampling_period_s` to `sampling_period` accordingly.

  * The periods are now aligned to the `UNIX_EPOCH` by default.

    To use the old behaviour (aligning to the time the resampler was created), pass `align_to=None` to the `ResamplerConfig`.

## New Features

* The core data-pipeline actors are now created automatically (#270).

  This eliminates a lot of boiler plate code and makes it much simpler to deploy apps.

  For example:

  ``` python
  async def run():
      await microgrid.initialize(
          host=HOST, port=PORT, resampler_config=ResamplerConfig(resampling_period_s=1.0)
      )
      grid_power = microgrid.logical_meter().grid_power()
  ```

* The `Result` class (and subclasses) for the `PowerDistributingActor` are now `dataclass`es, so logging them will produce a more detailed output.

* The `Resampler` can now can align the resampling period to an arbitrary `datetime`.

  This can be configured via the new `align_to` option in the `ResamplerConfig`. By default the resampling period is aligned to the `UNIX_EPOCH`.

## Bug Fixes

* Change `PowerDistributor` to use all batteries when none are working (#258)

* Update the ordered ring buffer used by the `MovingWindow` to fix the `len()` function so that it returns a value equal to or greater than zero, as expected (#274)
