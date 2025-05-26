# Frequenz Python SDK Release Notes

## New Features

- The SDK now officially support Python 3.13.

## Bug Fixes

- Fixed issue where actors would restart instead of stopping when exceptions occurred during cancellation. Actors now properly stop and surface the unhandled exception.
