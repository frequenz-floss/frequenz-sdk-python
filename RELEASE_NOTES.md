# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- A tutorial section and a getting started tutorial
- In `OrderedRingBuffer`:
  - Rename `datetime_to_index` to `to_internal_index` to avoid confusion between the internal index and the external index.
  - Add `index_to_datetime` method to convert external index to corresponding datetime.
  - Remove `__setitem__` method to enforce usage of dedicated `update` method only.
- In `OrderedRingBuffer` and `MovingWindow`:
  - Support for integer indices is added.
  - Add `count_covered` method to count the number of elements covered by the used time range.




## Bug Fixes

- Fix rendering of diagrams in the documentation.
- The `__getitem__` magic of the `MovingWindow` is fixed to support the same functionality that the `window` method provides.
