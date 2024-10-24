# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `ConfigManagingActor` now takes multiple configuration files as input, and the argument was renamed from `config_file` to `config_files`. If you are using this actor, please update your code. For example:

   ```python
   # Old
   actor = ConfigManagingActor(config_file="config.yaml")
   # New
   actor = ConfigManagingActor(config_files=["config.yaml"])
   ```

## New Features

- The `ConfigManagingActor` can now take multiple configuration files as input, allowing to override default configurations with custom configurations.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
