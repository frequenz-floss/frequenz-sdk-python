# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- The log level for when components are transitioning to a `WORKING` state is lowered to `INFO`, and the log message has been improved.

- This fixes a bug in the power manager, that was causing proposals to be ignored when they were proposing bounds that were fully outside the available bounds, under some cases.
