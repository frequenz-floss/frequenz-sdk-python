# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `frequenz.sdk.power` package contained the power distribution algorithm, which is for internal use in the sdk, and is no longer part of the public API.

- `PowerDistributingActor`'s result type `OutOfBound` has been renamed to `OutOfBounds`, and its member variable `bound` has been renamed to `bounds`.

## New Features

- DFS for compentent graph

## Bug Fixes

- Fixes a bug in the ring buffer updating the end timestamp of gaps when they are outdated.

- Properly handles PV configurations with no or only some meters before the PV component.

  So far we only had configurations like this: `Meter -> Inverter -> PV`. However the scenario with `Inverter -> PV` is also possible and now handled correctly.

- Fix `consumer_power()` not working certain configurations.

  In microgrids without consumers and no main meter, the formula would never return any values.

- Fix `pv_power` not working in setups with 2 grid meters by using a new reliable function to search for components in the components graph

- Fix `consumer_power` similar to `pv_power`

- Zero value requests received by the `PowerDistributingActor` will now always be accepted, even when there are non-zero exclusion bounds.
