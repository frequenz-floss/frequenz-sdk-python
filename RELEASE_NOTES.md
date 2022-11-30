# Release Notes

## Summary

This release mainly introduces the `LogicalMeter`. There should be no breaking
changes.

## New Features

- A logical meter implementation that can apply formulas on resampled component
  data streams.

- The `ComponentGraph` now supports *dangling* inverters, i.e. inverters
  without a successor. This is mainly to support PVs inverters. In the future
  *dangling* inverters will be forbidden again but only for batteries.

## Bug Fixes

- The component graph expected inverters to always have successors, and so
  wasn't able to support PV inverters, which don't have component successors.
  This is resolved by temporarily removing the requirement for inverters to have
  successors.  This will be partially reverted later by expecting just battery
  inverters to have graph successors.
