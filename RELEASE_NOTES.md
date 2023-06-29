# Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `Sample` objects no longer hold `float`s, but rather `Quantity` or one of its subclasses, like `Power`, `Current`, `Energy`, etc. based on the type of values being streamed.

  ```python
  sample: Sample[Power] = await battery_pool.power.new_receiver().receive()
  power: float = sample.value.as_watts()
  ```

- `BatteryPool.soc` now streams values of type `Sample[Quantity]`, and `BatteryPool.capacity` now streams values of type `Sample[Energy]`.

  ```python
  battery_pool = microgrid.battery_pool()
  soc_sample: Sample[Quantity] = await battery_pool.soc.new_receiver().receive()
  soc: float = soc_sample.value.base_value

  capacity_sample: Sample[Energy] = await battery_pool.capacity.new_receiver().receive()
  capacity: float = soc_sample.value.as_watt_hours()
  ```

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- The logical meter has a new method that returns producer power, that is the sum of all energy producers.

- `Quantity` types (`Power`, `Current`, `Energy`, `Voltage`) for providing type- and unit-safety when dealing with physical quantities.

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Two bugs in the ring buffer which is used by the `MovingWindow` class were fixed:
  - `len(buffer)` was not considering potentially existing gaps (areas without elements) in the buffer.
  - A off-by-one error in the gap calculation logic was fixed that recorded a
    gap when there was none if an element with a future timestamp was added that
    would create a gap of exactly 1.
