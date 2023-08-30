# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

- Calling `microgrid.initialize()` now also initializes the microgrid's grid connection point as a singleton object of a newly added type `Grid`. This object can be obtained by calling `microgrid.grid.get()`. This object exposes the max current that can course through the grid connection point, which is useful for the power distribution algorithm. The max current is provided by the Microgrid API, and can be obtained by calling `microgrid.grid.get().fuse.max_current`.

  Note that a microgrid is allowed to have zero or one grid connection point. Microgrids configured as islands will have zero grid connection points, and microgrids configured as grid-connected will have one grid connection point.

- A new class `Fuse` has been added to represent fuses. This class has a member variable `max_current` which represents the maximum current that can course through the fuse. If the current flowing through a fuse is greater than this limit, then the fuse will break the circuit.


## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
