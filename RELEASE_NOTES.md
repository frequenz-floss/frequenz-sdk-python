# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- The logical meter has a new method that returns producer power, that is the sum of all energy producers.

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Two bugs in the ring buffer which is used by the `MovingWindow` class were fixed:
  - `len(buffer)` was not considering potentially existing gaps (areas without elements) in the buffer.
  - A off-by-one error in the gap calculation logic was fixed that recorded a
    gap when there was none if an element with a future timestamp was added that
    would create a gap of exactly 1.
