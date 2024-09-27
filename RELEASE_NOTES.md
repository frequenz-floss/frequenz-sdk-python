# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- `ConfigManagingActor`: The file polling mechanism is now forced by default. Two new parameters have been added:
  - `force_polling`: Whether to force file polling to check for changes. Default is `True`.
  - `polling_interval`: The interval to check for changes. Only relevant if polling is enabled. Default is 1 second.

## Bug Fixes

- Many long running async tasks including metric streamers in the BatteryPool now have automatic recovery in case of exceptions.
