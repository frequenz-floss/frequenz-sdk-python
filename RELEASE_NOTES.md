# Frequenz Python SDK Release Notes

## Summary

This release mainly introduces a new feature that allows fallback components to be used in generated formulas, but it also fixes a few bugs and gets rid of `betterproto`/`grpclib` and goes back to Google's implementation.

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
