# Frequenz Python SDK Release Notes

## Upgrading

- The `microgrid.new_*_pool` methods no longer accept a `set_operating_point` parameter.
- The power manager now uses a new algorithm described [here](https://frequenz-floss.github.io/frequenz-sdk-python/v1.0-dev/user-guide/microgrid-concepts/#frequenz.sdk.microgrid--setting-power).

## Bug Fixes

- Fix `MetricFetcher` leaks when a requested metric fetcher already existed.
