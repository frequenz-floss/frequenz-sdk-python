# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with --> 

## New Features

- A logical meter implementation that can apply formulas on resampled component
  data streams.

## Bug Fixes

- The component graph expected inverters to always have successors, and so
  wasn't able to support PV inverters, which don't have component successors.
  This is resolved by temporarily removing the requirement for inverters to have
  successors.  This will be partially reverted later by expecting just battery
  inverters to have graph successors.
