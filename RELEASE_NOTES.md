# Frequenz Python SDK Release Notes

## Bug Fixes

- The log level for when components are transitioning to a `WORKING` state is lowered to `INFO`, and the log message has been improved.

- This fixes a bug in the power manager, that was causing proposals to be ignored when they were proposing bounds that were fully outside the available bounds, under some cases.
