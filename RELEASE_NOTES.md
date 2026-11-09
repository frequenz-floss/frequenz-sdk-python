# Frequenz Python SDK Release Notes

## Summary

This release raises the minimum `frequenz-microgrid-component-graph` version to pick up a fix for building the component graph of a microgrid that has an unspecified or unrecognized component with no connections.

## Upgrading

- The minimum `frequenz-channels` version is now 1.13.0.

- The minimum `typing-extensions` version is now 4.16.0. `frequenz-channels` also requires `frequenz-core >= 1.6.0, < 2`.

- The SDK no longer uses the channels APIs that will be removed in channels v2.0.0. The SDK's public API, timer behavior, and channel closing are unchanged. `Broadcast` and the SDK's `new_receiver(limit=...)` arguments stay as they are.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

* A microgrid with a component of unspecified or unrecognized category that has no connections no longer fails to build its component graph. Such a component is left out of the formulas. One with a connection still fails.
