# Optimizing PV production

## Introduction

Before we start it's assumed that you have finished the first [tutorial](./getting_started.md)

In this tutorial you will write an application that optimizes the energy produced from a
[PV](intro/glossary/#pv) system with Battery for self consumption.
In order to do so you need to measure the power that flows through the
[grid connection point](../../intro/glossary/#grid) to determine excess power.

## Measure the excess power

When using the term excess power what we actually mean is the consumer excess power, that is the power that
flows from the PV system into the grid.

!!! note

    We are using the [passive sign convention](../../intro/glossary/#passive-sign-convention) and thus power
    flowing from PV is negative and consumed power is positive.

We want to measure the excess power. In order to do so you can use the SDK's data pipeline and especially
the pre defined
[consumer](../../reference/frequenz/sdk/timeseries/logical_meter/#frequenz.sdk.timeseries.logical_meter.LogicalMeter.consumer_power)
and
[producer power](../../reference/frequenz/sdk/timeseries/logical_meter/#frequenz.sdk.timeseries.logical_meter.LogicalMeter.producer_power)
formulas.

```python
async def run() -> None:
    ... # (1)!

    # negative means feed-in power due to the negative sign convention
    consumer_excess_power_engine = (
        microgrid.logical_meter().producer_power
        + microgrid.logical_meter().consumer_power
    ).build("excess_power") # (2)!
    cons_excess_power_recv = consumer_excess_power_engine.new_receiver() # (3)!
```

1. The initialization code as explained in the Getting Started tutorial.
2. Construct the consumer excess power by summing up consumer and producer power each of which having
opposite signs due to the sign convention. This returns a formula engine.
3. Request a receiver from the formula engine which will be used to consume the stream.

## Control the Battery

Now, with the constructed excess power data stream use a
[battery pool](../../reference/frequenz/sdk/timeseries/battery_pool/) to implement control logic.

```python
    ...

    battery_pool = microgrid.battery_pool() # (1)!

    async for cons_excess_power in cons_excess_power_recv: # (2)!
        cons_excess_power = cons_excess_power.value # (3)!
        if cons_excess_power is None: # (4)!
            continue
        if cons_excess_power <= Power.zero(): # (5)!
            await battery_pool.charge(-cons_excess_power)
        elif cons_excess_power > Power.zero():
            await battery_pool.discharge(cons_excess_power)
```

1. Get an instance of the battery pool.
2. Iterate asynchronously over the constructed consumer excess power stream.
3. Get the `Quantity` from the received `Sample`.
4. Do nothing if we didn't receive new data.
5. Charge the battery if there is excess power and discharge otherwise.

And that's all you need to know to write the simple application for storing PV excess power in a battery.

## Full example

Here is a copy & paste friendly version

```python
import asyncio

from datetime import timedelta
from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig
from frequenz.sdk.timeseries import Power

async def run() -> None:
    # This points to the default Frequenz microgrid sandbox
    microgrid_host = "microgrid.sandbox.api.frequenz.io"
    microgrid_port = 62060

    # Initialize the microgrid
    await microgrid.initialize(
        microgrid_host,
        microgrid_port,
        ResamplerConfig(resampling_period=timedelta(seconds=1)),
    )

    # negative means feed-in power due to the negative sign convention
    consumer_excess_power_engine = (
        microgrid.logical_meter().producer_power
        + microgrid.logical_meter().consumer_power
    ).build("excess_power") # (2)!
    cons_excess_power_recv = consumer_excess_power_engine.new_receiver() # (3)!

    battery_pool = microgrid.battery_pool() # (1)!

    async for cons_excess_power in cons_excess_power_recv: # (2)!
        cons_excess_power = cons_excess_power.value # (3)!
        if cons_excess_power is None: # (4)!
            continue
        if cons_excess_power <= Power.zero(): # (5)!
            await battery_pool.charge(-cons_excess_power)
        elif cons_excess_power > Power.zero():
            await battery_pool.discharge(discharge_power)

def main() -> None:
    asyncio.run(run())

if __name__ == "__main__":
    main()
```

## Further reading

To create more advanced applications it is suggested to read the [actors](../../intro/actors/) documentation.
