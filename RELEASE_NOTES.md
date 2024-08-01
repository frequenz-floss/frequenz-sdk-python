# Frequenz Python SDK Release Notes

## Bug Fixes

- Fix PV power distribution excluding inverters that haven't sent any data since the application started.
- Change consumer power formula to use 0 instead of None if any component is not sending data. Now power is sum of power in all working components.
