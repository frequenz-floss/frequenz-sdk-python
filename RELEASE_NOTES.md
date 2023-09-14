# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

- The battery pool metric methods no longer return `None` when no batteries are available. Instead, the value of the `Sample` or `PowerMetric` is set to `None`.

- The power distribution `Result` is now a union of all different types of results rather than a base class. This means we can now also use `match` to check for result types instead of only using `isinstance()`. The following example shows how `Result` can be used for matching power distribution results:

  ```python
  from typing import assert_never
  result: Result = some_operation()
  match result:
      case Success() as success:
          print(f"Power request was successful: {success}")
      case PartialFailure() as partial_failure:
          print(f"Power request was partially successful: {partial_failure}")
      case OutOfBounds() as out_of_bounds:
          print(f"Power request was out of bounds: {out_of_bounds}")
      case Error() as error:
          print(f"Power request failed: {error}")
      case _ as unreachable:
          assert_never(unreachable)
  ```

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

- Calling `microgrid.initialize()` now also initializes the microgrid's grid connection point as a singleton object of a newly added type `Grid`. This object can be obtained by calling `microgrid.grid.get()`. This object exposes the max current that can course through the grid connection point, which is useful for the power distribution algorithm. The max current is provided by the Microgrid API, and can be obtained by calling `microgrid.grid.get().fuse.max_current`.

  Note that a microgrid is allowed to have zero or one grid connection point. Microgrids configured as islands will have zero grid connection points, and microgrids configured as grid-connected will have one grid connection point.

- A new method `microgrid.frequeny()` was added to allow easy access to the current frequency of the grid.

- A new class `Fuse` has been added to represent fuses. This class has a member variable `max_current` which represents the maximum current that can course through the fuse. If the current flowing through a fuse is greater than this limit, then the fuse will break the circuit.

- `MovingWindow` and `OrderedRingBuffer`:
  - NaN values are treated as missing when gaps are determined in the `OrderedRingBuffer`.
  - Provide access to `capacity` (maximum number of elements) in `MovingWindow`.
  - Methods to retrieve oldest and newest timestamp of valid samples are added to both.
  - `MovingWindow` exposes underlying buffers `window` method.
  - `len(window)` and `len(buffer)` should be replaced with `window.count_valid()` and `buffer.count_valid()`, respectively.
  - `OrderedRingBuffer.window`:
    - By default returns a copy.
    - Can also return a view if the window contains `None` values and if `force_copy` is set to `True`.

- Now when printing `FormulaEngine` for debugging purposes the the formula will be shown in infix notation, which should be easier to read.

- The CI now runs cross-arch tests on `arm64` architectures.

- The `min` and `max` functions in the `FormulaEngine` are now public. Note that the same functions have been public in the builder.

- Drop `Averager` from `FormulaEngine`.

## Bug Fixes

- `OrderedRingBuffer.window`:
  - Fixed `force_copy` option for specific case.
  - Removed buggy enforcement of copies when None values in queried window.
  - Fixed behavior for start equals end case.
