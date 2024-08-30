# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

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
