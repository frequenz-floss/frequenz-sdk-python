# Frequenz Python SDK Release Notes

## Summary

This release ships many small improvements and bug fixes to `Quantity`s. It also depends on [channels](https://github.com/frequenz-floss/frequenz-channels-python/) v0.16.0, so users must update the dependency too.

## Upgrading

- `Channels` has been upgraded to version 0.16.0, for information on how to upgrade please read the [channels v0.16.0 release notes](visit https://github.com/frequenz-floss/frequenz-channels-python/releases/tag/v0.16.0).
- `Quantity` objects are no longer hashable.  This is because of the pitfalls of hashing `float` values.

## New Features

- Quantities

  * Add support for the unary negative operator (negation of a quantity).
  * Add `abs()`.
  * Add a `isclose()` method on quantities to compare them to other values of the same type.  Because `Quantity` types are just wrappers around `float`s, direct comparison might not always be desirable.
  * Add `zero()` constructor (which returns a singleton) to easily get a zero value.
  * Add multiplication by `Percentage` types.
  * Add a new quantity class `Frequency` for frequency values.
  * Add a new quantity class `Temperature` for temperature values.

- `FormulaEngine` arithmetics now supports scalar multiplication with `float`s and addition with `Quantity`s.
- Add a new `temperature` method for streaming average temperature values for the battery pool.

## Bug Fixes

- Fix formatting issue for `Quantity` objects with zero values.
- Fix formatting issue for `Quantity` when the base value fulfills `math.isinf()` or `math.isnan()`.
- Fix clamping to 100% for the battery pool SoC scaling calculation.
- Fix indexing for empty `MovingWindow`s (now it properly raises an `IndexError`).
