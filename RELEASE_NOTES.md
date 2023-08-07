# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- Upgrade to microgrid API v0.15.1.  If you're using any of the lower level microgrid interfaces, you will need to upgrade your code.
- The argument `conf_file` of the `ConfigManagingActor` constructor was renamed to `config_path`.

## New Features

- The `ConfigManagingActor` constructor now can accept a `pathlib.Path` as `config_path` too (before it accepted only a `str`).

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
