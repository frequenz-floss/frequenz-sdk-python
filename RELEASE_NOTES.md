# Frequenz Python SDK Release Notes

## Summary

This release adds support for `frequenz-channels` v2.0.0, while still supporting v1.13. It also raises the minimum `frequenz-microgrid-component-graph` version to pick up a fix for building the component graph of a microgrid that has an unspecified or unrecognized component with no connections.

## Upgrading

- The SDK now works with both `frequenz-channels` v1.13 and [v2.0.0](https://github.com/frequenz-floss/frequenz-channels-python/releases/tag/v2.0.0) (`frequenz-channels >= 1.13.0, < 3`). The minimum version is now [v1.13.0](https://github.com/frequenz-floss/frequenz-channels-python/releases/tag/v1.13.0).

- The minimum `typing-extensions` version is now 4.16.0. `frequenz-channels` also requires `frequenz-core >= 1.6.0, < 2`.

- The minimum `frequenz-microgrid-component-graph` version is now [v0.6.1](https://github.com/frequenz-floss/frequenz-microgrid-component-graph-python/releases/tag/v0.6.1).

- The SDK no longer uses the channels APIs removed in channels v2.0.0. The SDK's public API, timer behavior, and channel closing are unchanged.

## Bug Fixes

* A microgrid with a component of unspecified or unrecognized category that has no connections no longer fails to build its component graph. Such a component is left out of the formulas. If it has a connection, building the graph still fails.
