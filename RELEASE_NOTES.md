# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

- `Channels` has been upgraded to version 0.16.0, for information on how to upgrade visit https://github.com/frequenz-floss/frequenz-channels-python/releases/tag/v0.16.0
- `Quantity` objects are no longer hashable.  This is because of the pitfalls of hashing `float` values.

## New Features

- Add `abs()` support for quantities.
- Add quantity class `Frequency` for frequency values.
- Quantities can now be multiplied with `Percentage` types.
- `FormulaEngine` arithmetics now supports scalar multiplication with floats and addition with Quantities.
- Add a `isclose()` method on quantities to compare them to other values of the same type.  Because `Quantity` types are just wrappers around `float`s, direct comparison might not always be desirable.

## Bug Fixes

- Fix formatting issue for `Quantity` objects with zero values.
- Fix formatting isuse for `Quantity` when the base value is float.inf or float.nan.

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
