# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `microgrid.new_*_pool` methods no longer accept a `set_operating_point` parameter.
- The power manager now uses a new algorithm described [here](https://frequenz-floss.github.io/frequenz-sdk-python/v1.0-dev/user-guide/microgrid-concepts/#frequenz.sdk.microgrid--setting-power).

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Fix `MetricFetcher` leaks when a requested metric fetcher already existed.
