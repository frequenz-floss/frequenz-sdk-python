# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- The SDK now officially support Python 3.13.

## Bug Fixes

- Fixed issue where actors would restart instead of stopping when exceptions occurred during cancellation. Actors now properly stop and surface the unhandled exception.