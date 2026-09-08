# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- Custom validators passed through a configuration dataclass field's `metadata` (as used by `load_config()` and `ConfigManager.new_receiver()`) must now raise a `marshmallow.ValidationError` to reject a value. `marshmallow` 4 removed support for validators that signal failure by returning `False`, so such validators are now ignored and the invalid value is accepted silently. This affects you as soon as `marshmallow` 4 is installed, which `frequenz-quantities` 1.0.2 and later require.

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
