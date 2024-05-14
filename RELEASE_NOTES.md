# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `frequenz-client-microgrid` dependency was bumped to v0.4.0. If you are using the client directly in your code, you will need to upgrade too.

- Calls to `microgrid.*_pool` methods now always need to specified a priority value, corresponding to the requirements/priority of the actor making the call.

- The `microgrid.*_pool` methods would only accept keyword arguments from now on.

## New Features

- Calls to `microgrid.*_pool` methods now accept an optional `in_shifting_group` parameter.  Power requests sent to `*_pool` instances that have the `in_shifting_group` flag set, will get resolved separately, and their target power will be added to the target power calculated from regular actors, if any, which would, in effect, shift the zero for the regular actors by the target power from the shifting group.

## Bug Fixes

- When the PowerDistributor receives a zero power request for PV inverters, it now correctly sets zero power to the inverters, and no longer crashes.
