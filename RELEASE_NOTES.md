# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Components used to be just forgotten by the power manager when all proposals are withdrawn, leaving them at their last set power values.  This has been fixed by getting the power manager to set the components to their default powers, based on the component category (according to the table below), as the last step.


  | component category | default power                             |
  |--------------------|-------------------------------------------|
  | Battery            | 0.0                                       |
  | PV                 | Minimum power (aka max production power)  |
  | EV Chargers        | Maximum power (aka max consumption power) |
