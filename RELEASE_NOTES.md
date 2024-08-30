# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `frequenz-client-microgrid` dependency was bumped to `0.5.0`. This can cause dependency issues if you are using other API clients and the `frequenz-client-base` dependencies don't match.

## New Features

- Fallback components are used in generated formulas. If primary components is unavailable, formula will generate metric from fallback components. Fallback formulas are implemented for:
  - PVPowerFormula
  - ProducerPowerFormula
  - BatteryPowerFormula
  - ConsumerPowerFormula
  - GridPowerFormula

## Bug Fixes

- Allow setting `api_power_request_timeout` in `microgrid.initialize()`.

- Fix an issue where in grid meters could be identified as {pv/ev/battery/chp} meters in some component graph configurations.
