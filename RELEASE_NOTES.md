# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- Allow multiplying any `Quantity` by a `float` too. This just scales the `Quantity` value.

## Bug Fixes

- Fix grid current formula generator to add the operator `+` to the engine only when the component category is handled.
- Fix bug where sometimes the `base_value` of a `Quantity` could be of a different type than `float`.
