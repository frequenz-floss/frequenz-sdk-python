# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `frequenz.sdk.microgrid.*_pool` methods has been renamed to `new_*_pool`, to make it explicit that they create new instances of the pool classes.
  + `battery_pool` -> `new_battery_pool`
  + `ev_charger_pool` -> `new_ev_charger_pool`
  + `pv_pool` -> `new_pv_pool`

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

- Classes Bounds and SystemBounds now work with the `in` operator

## Bug Fixes

- Fixed a typing issue that occurs in some cases when composing formulas with constants.
