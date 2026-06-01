# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Update microgrid client to v0.18.3+, which fixes a problem with missing steam boilers on formula generation.

## New Features

* The PV inverter manager now accounts for the power measured on unreachable PV inverters when distributing power, so the reachable inverters compensate for it (matching the battery manager's behavior).

## Bug Fixes

* Fixed a resource leak in the power distributor: the formulas created for unreachable batteries were never stopped, so CPU usage slowly climbed over time until the application was restarted.
* Fixed the grid reactive-power formula being recreated and leaked on every access instead of being reused from the cache.
