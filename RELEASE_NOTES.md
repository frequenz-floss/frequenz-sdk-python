# Release Notes

## Summary

The project has a new home!

https://frequenz-floss.github.io/frequenz-sdk-python/

For now the documentation is pretty scarce but we will be improving it with
time.

The most prominent changes in this release is the cleanup of the public API,
which is much more concise and clear now and the addition of classes
implementing the new data flow design, in particular the `DataSourcingActor`
and the `ComponentMetricsResamplingActor`.

## Upgrading

* `EVChargerData`'s `active_power_consumption` has been renamed to `active_power`

* `PowerDistributor` - type of `Request`'s `request_timeout_sec` has been changed from `int` to `float`

* `ComponentCategory.LOAD` has been added

* The
  [`frequenz-channels`](https://github.com/frequenz-floss/frequenz-channels-python/)
  was upgraded to
  [v0.11.0](https://github.com/frequenz-floss/frequenz-channels-python/releases/tag/v0.11.0)
  which includes a bunch of [breaking
  changes](https://github.com/frequenz-floss/frequenz-channels-python/blob/v0.11.0/RELEASE_NOTES.md#upgrading-breaking-changes)
  you should be aware of.

* The public API has been cleaned up, several symbols were moved or exposed in
  a single module, and some symbols were hidden because they are either
  schedule for deprecation or not yet stabilized.

  * `frequenz.sdk.actor`: The `decorator` sub-module was hidden and now the
    `@actor` decorator is exposed directly only in the main module.

  * `frequenz.sdk.configs`: was renamed to `frequenz.sdk.config`, `Config` is
    only exposed in the main module and the `ConfigManager` was moved to
    `frequenz.sdk.actor.ConfigManagingActor`.

  * The modules `frequenz.sdk.data_handling` and `frequenz.sdk.data_ingestion`
    were hidden from the public interface and will probably be removed in the
    future. They are still available as `frequenz.sdk._data_handling` and
    `frequenz.sdk._data_ingestion` for users still needing them.

  * The module `frequenz.sdk.power_distribution` was renamed to
    `frequenz.sdk.power` and the `PowerDistributor` was moved to
    `frequenz.sdk.actor.power_distributing.PowerDistributingActor` (with the
    utility classes `Request` and `Result`).

  * The module `frequenz.sdk.microgrid` was simplified.

    * All component-related symbols (`.component`, `.component_data`,
      `.component_states`) were moved to the sub-module
      `frequenz.sdk.microgrid.component`.

    * All API client-related symbols (`.client`, `.connection`, `.retry`) were
      moved to the sub-module `frequenz.sdk.microgrid.client`.

    * The `ComponentGraph` is exposed directly in the main module (and only
      there).

    * The `microgrid_api` module is now exposed via the main module directly
      (and thus indirectly renamed to `microgrid`, so instead of using `from
      frequenz.sdk.microgrid import microgrid_api; microgrid_api.initialize()`
      (for example) you should use `from frequenz.sdk.microgrid import
      microgridi; microgrid.initialize()`.

    * The `MicrogridApi` class was renamed to `Microgrid` to make it clear it
      is not exclusively about the API.

    * The `Microgrid.microgrid_api_client` attribute was renamed to
      `Microgrid.api_client` to avoid the redundancy.

## New Features

* `MeterData` objects now expose the AC `frequency` measured by the meter.

* `BatteryData` objects now expose the temperature of the hottest block in the
  battery as `temperature_max`

* A new `frequenz.sdk.actor.DataSourcingActor` was added.

* A new `frequenz.sdk.actor.ComponentMetricsResamplingActor` was added.

* A new `frequenz.sdk.actor.ChannelRegistry` was added.

* A new module `frequenz.sdk.timeseries` was added.
