# Frequenz Python SDK Release Notes

## Bug Fixes

- Fixes a bug in the ring buffer in case the updated value is missing and creates a gap in time.

- Fixed a bug that was causing the `PowerDistributor` to exit if power requests to PV inverters or EV chargers timeout.

- Modify pv formulas to use just PV inverters, not meters

- Stop PV inverter when it is not known to be working - set its power to 0