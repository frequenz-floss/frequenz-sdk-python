# Frequenz Python SDK Release Notes

## Summary

This is a minor non-breaking release that adds new features and fixes a few bug.

## New Features

- Allow multiplying and dividing any `Quantity` by a `float`. This just scales the `Quantity` value.
- Allow dividing any `Quantity` by another quaintity of the same type. This just returns a ration between both quantities.
- The battery pool `power` method now supports scenarios where one or more inverters can have multiple batteries connected to it and one or more batteries can have multiple inverters connected to it.

## Bug Fixes

- Fix grid current formula generator to add the operator `+` to the engine only when the component category is handled.
- Fix bug where sometimes the `base_value` of a `Quantity` could be of a different type than `float`.
