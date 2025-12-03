# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The SDK now depends on the `frequenz-client-microgrid` v0.18.x series.

    * Check the release notes for the client [v0.18].
    * There were a lot of changes, so it might be also worth having a quick look at the microgrid API [v0.17][api-v0.17] and [v0.18][api-v0.17] releases.
    * Checking out the API common releases [v0.6][common-v0.6], [v0.7][common-v0.7], [v0.8][common-v0.8] might also be worthwhile, at least if you find any errors about renamed or missing types.
    * Although many of the changes in lower layer are hidden by the SDK, there are some changes that can't be hidden away. For example the `Metric` enum had some renames, and `Component.component_id` was renamed to `Component.id`. Also, there is a new component class hierarchy.

- `ComponentGraph` methods arguments were renamed to better reflect what they expect.

    * The `components()` method now uses `matching_ids` and `matching_types` instead of `component_ids` and `component_categories` respectively. `matching_types` takes types inheriting from `Component` instead of categories, for example `Battery` or `BatteryInverter`.
    * The `connections()` methods now take `matching_sources` and `matching_destinations` instead of `start` and `end` respectively. This is to match the new names in `ComponentConnection`.
    * All arguments for both methods can now receiver arbitrary iterables instead of `set`s, and can also accept a single value.

[v0.18]: https://github.com/frequenz-floss/frequenz-client-microgrid-python/releases/tag/v0.18.0
[api-v0.17]: https://github.com/frequenz-floss/frequenz-api-microgrid/releases/tag/v0.17.0
[api-v0.18]: https://github.com/frequenz-floss/frequenz-api-microgrid/releases/tag/v0.18.0
[common-v0.6]: https://github.com/frequenz-floss/frequenz-api-common/releases/tag/v0.6.0
[common-v0.7]: https://github.com/frequenz-floss/frequenz-api-common/releases/tag/v0.7.0
[common-v0.8]: https://github.com/frequenz-floss/frequenz-api-common/releases/tag/v0.8.0

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
