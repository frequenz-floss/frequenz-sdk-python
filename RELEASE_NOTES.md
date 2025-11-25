# Frequenz Python SDK Release Notes

## Bug Fixes

- Doesn't repeat zero commands to battery inverters anymore, to not interfere with lower level logic that might want to do things only when there are no actors trying to use the batteries.
