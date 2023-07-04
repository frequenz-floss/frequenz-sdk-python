# Release Notes

## Summary

New `Quantity` types! These types can have units (power, current, voltage, etc.) and are *type- and unit-safe* in the sense that users can't accidentally sum a power with a voltage, or a power in kW with a power in W.

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

- `MicrogridApiClient.set_power` no longer returns a `protobuf.Empty` result, but a `None`.  This won't affect you unless you are using the low level APIs of the SDK.

## New Features

- The logical meter has a new method that returns producer power, that is the sum of all energy producers.

- `Quantity` types (`Power`, `Current`, `Energy`, `Voltage`) for providing type- and unit-safety when dealing with physical quantities.

## Bug Fixes

- Two bugs in the ring buffer which is used by the `MovingWindow` class were fixed:
  - `len(buffer)` was not considering potentially existing gaps (areas without elements) in the buffer.
  - A off-by-one error in the gap calculation logic was fixed that recorded a gap when there was none if an element with a future timestamp was added that would create a gap of exactly 1.

- A formula engine lifetime issue, when creating higher order formula receivers without holding on to a reference to the engine, was fixed.
