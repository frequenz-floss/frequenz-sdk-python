# Release Notes

## Summary

## Upgrading

- Add fine-grained Result types for the PowerDistributingActor. Previously Result was one class with many fields. Now each type of the result has its own class that derives from Result parent class.

## New Features

- Add inverter type to the Component data. Inverter type tells what kind of inverter it is (Battery, Solar etc).
- Add `FormulaGenerator` class for generating formulas from the component graph.
- Add formulas for: `grid_power`, `battery_power`, `PV power`.

## Bug Fixes

- Fix ComponentMetricResamplingActor, to not subscribe twice to the same source.
