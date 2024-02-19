# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- Allow multiplying and dividing any `Quantity` by a `float`. This just scales the `Quantity` value.
- Allow dividing any `Quantity` by another quaintity of the same type. This just returns a ration between both quantities.

## Bug Fixes

- Fix grid current formula generator to add the operator `+` to the engine only when the component category is handled.
- Fix bug where sometimes the `base_value` of a `Quantity` could be of a different type than `float`.
