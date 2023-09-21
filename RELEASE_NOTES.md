# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- A tutorial section and a getting started tutorial
- In `OrderedRingBuffer`:
  - Rename `datetime_to_index` to `to_internal_index` to avoid confusion between the internal index and the external index.
  - Remove `__setitem__` method to enforce usage of dedicated `update` method only.

## Bug Fixes

- Fix rendering of diagrams in the documentation.
