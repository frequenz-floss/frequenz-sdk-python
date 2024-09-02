# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Provides a way for the SDK to apply formulas on resampled data streams.

# Formula Engine

[`FormulaEngine`][frequenz.sdk.timeseries.formula_engine.FormulaEngine]s are used in the
SDK to calculate and stream metrics like
[`grid_power`][frequenz.sdk.timeseries.grid.Grid.power],
[`consumer_power`][frequenz.sdk.timeseries.consumer.Consumer.power], etc., which are
building blocks of the [Frequenz SDK Microgrid
Model][frequenz.sdk.microgrid--frequenz-sdk-microgrid-model].

The SDK creates the formulas by analysing the configuration of components in the
{{glossary("Component Graph")}}.

## Streaming Interface

The
[`FormulaEngine.new_receiver()`][frequenz.sdk.timeseries.formula_engine.FormulaEngine.new_receiver]
method can be used to create a [Receiver][frequenz.channels.Receiver] that streams the
[Sample][frequenz.sdk.timeseries.Sample]s calculated by the formula engine.

```python
from frequenz.sdk import microgrid

battery_pool = microgrid.new_battery_pool(priority=5)

async for power in battery_pool.power.new_receiver():
    print(f"{power=}")
```

## Composition

Composite `FormulaEngine`s can be built using arithmetic operations on
`FormulaEngine`s streaming the same type of data.

For example, if you're interested in a particular composite metric that can be
calculated by subtracting
[`new_battery_pool().power`][frequenz.sdk.timeseries.battery_pool.BatteryPool.power] and
[`new_ev_charger_pool().power`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool]
from the
[`grid().power`][frequenz.sdk.timeseries.grid.Grid.power],
we can build a `FormulaEngine` that provides a stream of this calculated metric as
follows:

```python
from frequenz.sdk import microgrid

battery_pool = microgrid.new_battery_pool(priority=5)
ev_charger_pool = microgrid.new_ev_charger_pool(priority=5)
grid = microgrid.grid()

# apply operations on formula engines to create a formula engine that would
# apply these operations on the corresponding data streams.
net_power = (
    grid.power - (battery_pool.power + ev_charger_pool.power)
).build("net_power")

async for power in net_power.new_receiver():
    print(f"{power=}")
```

# Formula Engine 3-Phase

A [`FormulaEngine3Phase`][frequenz.sdk.timeseries.formula_engine.FormulaEngine3Phase]
is similar to a
[`FormulaEngine`][frequenz.sdk.timeseries.formula_engine.FormulaEngine], except that
they stream [3-phase samples][frequenz.sdk.timeseries.Sample3Phase].  All the
current formulas (like
[`Grid.current_per_phase`][frequenz.sdk.timeseries.grid.Grid.current_per_phase],
[`EVChargerPool.current_per_phase`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.current_per_phase],
etc.) are implemented as per-phase formulas.

## Streaming Interface

The
[`FormulaEngine3Phase.new_receiver()`][frequenz.sdk.timeseries.formula_engine.FormulaEngine3Phase.new_receiver]
method can be used to create a [Receiver][frequenz.channels.Receiver] that streams the
[Sample3Phase][frequenz.sdk.timeseries.Sample3Phase] values
calculated by the formula engine.

```python
from frequenz.sdk import microgrid

ev_charger_pool = microgrid.new_ev_charger_pool(priority=5)

async for sample in ev_charger_pool.current_per_phase.new_receiver():
    print(f"Current: {sample}")
```

## Composition

`FormulaEngine3Phase` instances can be composed together, just like `FormulaEngine`
instances.

```python
from frequenz.sdk import microgrid

ev_charger_pool = microgrid.new_ev_charger_pool(priority=5)
grid = microgrid.grid()

# Calculate grid consumption current that's not used by the EV chargers
other_current = (grid.current_per_phase - ev_charger_pool.current_per_phase).build(
    "other_current"
)

async for sample in other_current.new_receiver():
    print(f"Other current: {sample}")
```
"""

from ._formula_engine import FormulaEngine, FormulaEngine3Phase

__all__ = [
    "FormulaEngine",
    "FormulaEngine3Phase",
]
