# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A {{glossary("microgrid")}} is a local electrical grid that connects a set of
electrical components together.  They are often built around a passive power consumer,
to supplement the electricity consumed from the {{glossary("grid", "public grid")}} with
on-site power generation or storage systems.

Microgrids can also function in {{glossary("island", "island-mode")}}, without a grid
connection, or without a local power consumer, but they have to have at least one of the
two, to be meaningful.

## Frequenz SDK Microgrid Model

The SDK aims to provide an abstract model of the microgrid that enables high-level
interactions with {{glossary("component", "microgrid components")}}, without having to
worry about (or even be aware of) location-specific details such as:

- where the {{glossary("meter", "meters")}} are placed,
- how many {{glossary("battery", "batteries")}},
- whether there's a grid connection or a passive consumer,
- what models the {{glossary("inverter", "inverters")}} are, etc.
- whether components are having downtimes, because {{glossary("metric", "metrics")}} and
  limits get adjusted automatically when components are having downtimes.

Users of the SDK can develop applications around this interface once and deploy
anywhere, and the SDK will take care of translating the requests and instructions to
correspond to the specific microgrid configurations.

``` mermaid
flowchart LR

subgraph Left[Measurements only]
direction LR
  grid["Grid Connection"]
  consumer["Consumer"]
  pv["PV Arrays"]
  chp["CHP"]
end

junction(( ))

subgraph Right[Measurements and control]
direction LR
  bat["Batteries"]
  ev["EV Chargers"]
end

grid --- junction
consumer --- junction
pv --- junction
chp --- junction

junction --- bat
junction --- ev
```

## Grid

This refers to a microgrid's connection to the external Grid.  The power flowing through
this connection can be streamed through
[`grid_power`][frequenz.sdk.timeseries.grid.Grid.power].

In locations without a grid connection, this method remains accessible, and streams zero
values.

## Consumer

This is the main power consumer at the site of a microgrid, and often the
{{glossary("load")}} the microgrid is built to support.  The power drawn by the consumer
is available through [`consumer_power`][frequenz.sdk.timeseries.consumer.Consumer.power]

In locations without a consumer, this method streams zero values.

## Producers: PV Arrays, CHP

The total CHP production in a site can be streamed through
[`chp_power`][frequenz.sdk.timeseries.logical_meter.LogicalMeter.chp_power].  PV Power
is available through the PV pool described below.  And total producer power is available
through [`microgrid.producer().power`][frequenz.sdk.timeseries.producer.Producer.power].

As is the case with the other methods, if PV Arrays or CHPs are not available in a
microgrid, the corresponding methods stream zero values.

## PV Arrays

The total PV power production is available through
[`pv_pool`][frequenz.sdk.microgrid.pv_pool]'s
[`power`][frequenz.sdk.timeseries.pv_pool.PVPool.power].  The PV pool by default uses
all PV inverters available at a location, but PV pool instances can be created for
subsets of PV inverters if necessary, by specifying the inverter ids.

The `pv_pool` also provides available power bounds through the
[`power_status`][frequenz.sdk.timeseries.pv_pool.PVPool.power_status] method.

The `pv_pool` also provides a control method
[`propose_power`][frequenz.sdk.timeseries.pv_pool.PVPool.propose_power], which accepts
values in the {{glossary("psc", "Passive Sign Convention")}} and supports only
production.


## Batteries

The total Battery power is available through
[`battery_pool`][frequenz.sdk.microgrid.battery_pool]'s
[`power`][frequenz.sdk.timeseries.battery_pool.BatteryPool.power].  The battery pool by
default uses all batteries available at a location, but battery pool instances can be
created for subsets of batteries if necessary, by specifying the battery ids.

The `battery_pool` also provides
[`soc`][frequenz.sdk.timeseries.battery_pool.BatteryPool.soc],
[`capacity`][frequenz.sdk.timeseries.battery_pool.BatteryPool.capacity],
[`temperature`][frequenz.sdk.timeseries.battery_pool.BatteryPool.temperature] and
available power bounds through the
[`power_status`][frequenz.sdk.timeseries.battery_pool.BatteryPool.power_status] method.

The `battery_pool` also provides control methods
[`propose_power`][frequenz.sdk.timeseries.battery_pool.BatteryPool.propose_power] (which
accepts values in the {{glossary("psc", "Passive Sign Convention")}} and supports both
charging and discharging), or through
[`propose_charge`][frequenz.sdk.timeseries.battery_pool.BatteryPool.propose_charge], or
[`propose_discharge`][frequenz.sdk.timeseries.battery_pool.BatteryPool.propose_discharge].

## EV Chargers

The [`ev_charger_pool`][frequenz.sdk.microgrid.ev_charger_pool] offers a
[`power`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.power] method that
streams the total power measured for all the {{glossary("ev-charger", "EV Chargers")}}
at a site.

The `ev_charger_pool` also provides available power bounds through the
[`power_status`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.power_status]
method.


The `ev_charger_pool` also provides a control method
[`propose_power`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.propose_power],
which accepts values in the {{glossary("psc", "Passive Sign Convention")}} and supports
only charging.
"""  # noqa: D205, D400

from ..actor import ResamplerConfig
from . import _data_pipeline, connection_manager
from ._data_pipeline import (
    battery_pool,
    consumer,
    ev_charger_pool,
    frequency,
    grid,
    logical_meter,
    producer,
    pv_pool,
    voltage,
)


async def initialize(host: str, port: int, resampler_config: ResamplerConfig) -> None:
    """Initialize the microgrid connection manager and the data pipeline.

    Args:
        host: Host to connect to, to reach the microgrid API.
        port: port to connect to.
        resampler_config: Configuration for the resampling actor.
    """
    await connection_manager.initialize(host, port)
    await _data_pipeline.initialize(resampler_config)


__all__ = [
    "initialize",
    "consumer",
    "battery_pool",
    "ev_charger_pool",
    "grid",
    "frequency",
    "logical_meter",
    "producer",
    "pv_pool",
    "voltage",
]
