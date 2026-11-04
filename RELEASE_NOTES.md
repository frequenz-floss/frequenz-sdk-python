# Frequenz Python SDK Release Notes

## Summary

This release raises the minimum `frequenz-microgrid-component-graph` version to pick up a fix for building the component graph of a microgrid that has an unspecified or unrecognized component with no connections.

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

* A microgrid with a component of unspecified or unrecognized category that has no connections no longer fails to build its component graph. Such a component is left out of the formulas. One with a connection still fails.
