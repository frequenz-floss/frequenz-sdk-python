# Glossary

This glossary provides definitions for common terminology used in the SDK,
focusing on microgrid components, metrics, measurements, and power-related
terms.

## Microgrid

### Component

A device within the [microgrid](#microgrid), such as an inverter, battery, meter, and more.

### Component ID

A numeric identifier uniquely representing an instance of a [component](#component). It is always of type `int`.

For example, a battery an have a component ID **5**.

## Component Data

### Metric

A quantifiable attribute of a [component](#component).

For example, the metric **capacity** of a battery.

### Measurement

An individual numeric value obtained from a [metric](#metric) of a [component](#component) instance. It is consistently of type `float`, but it is often expressed in specific units.

In the context of a sample, this is commonly referred to as a *sample value*.

For example, a measurement of the capacity of a battery with component ID 5 can be **400**, typically measured in Watt-hours (Wh).

### Timestamp

A specific point in time, always represented as a `datetime` with a `timezone` attached.

For example, **2022-01-01 22:00:00.000 UTC**.

### Sample

A [measurement](#measurement) recorded at a particular [timestamp](#timestamp), typically represented as a tuple `(timestamp, value)`.

For example, recording a measurement of 400 from the capacity of a battery at 2022-01-01 22:00:00.000 UTC would constitute a sample **`(2022-01-01 22:00:00.000 UTC, 400)`**.

#### Sample Value

A [measurement](#measurement) stored in a [sample](#sample).

### Time Series

A sequence of [samples](#sample), often organized by [timestamp](#timestamp) and typically with regular intervals. However, irregular (event-based) time series are also possible.

For example, a time series representing measurements of a battery's capacity at 2022-01-01 22:00:00.000 UTC every second for 5 seconds would appear as follows:

```
(2022-01-01 22:00:00.000 UTC, 400)
(2022-01-01 22:00:01.000 UTC, 401)
(2022-01-01 22:00:02.000 UTC, 403)
(2022-01-01 22:00:03.000 UTC, 402)
(2022-01-01 22:00:04.000 UTC, 403)
```

### Timeseries

Same as [time series](#time-series).

### Metric ID

An identifier for a [component](#component)'s [metric](#metric), typically a string (`str`).

For example, the metric ID for the capacity of a battery is simply **`capacity`**.

### Time Series ID

An identifier for a [time series](#time-series) originating from a [metric](#metric) of a specific component. Typically a string (`str`) derived from the tuple **([component ID](#component-id), [metric ID](#metric-id))** for [components](#component).

For example, a time series for the capacity of a battery with component ID 5 has the ID **(component_id, metric_id)** (or **`f"{component_id}_{metric_id}"`**).

### Timeseries ID

Same as [time series ID](#time-series-id).

## Metrics

### Gross Consumption

Consumption before accounting for any local generation from solar, wind or CHP.

### Net Consumption

This term traditionally refers to the difference between the [gross consumption](#gross-consumption) and the local generation (like PV production). It iss the electricity consumption that needs to be met by the battery or from the main grid.

### Net Load

Same as [net consumption](#net-consumption).

### Residual Consumption

In [microgrid](#microgrid) context sometimes used as the remaining difference between the net consumption and the battery power, i.e. what we define as grid power.

### Residual Load

Same as [residual consumption](#residual-consumption).

### SoC (State of Charge)

The level of charge of a battery relative to its capacity, expressed in percentage points. Calculated as the ratio between the remaining energy in the battery at a given time and the maximum possible energy under similar health conditions. [Source](https://epicpower.es/wp-content/uploads/2020/08/AN028_SoC-SoH-SoP-definitions_v3.pdf)

### SoP (State of Power)

The ratio of peak power to nominal power. Peak power is the maximum power that can be sustained for a specific duration without violating preset operational design limits on battery voltage, SsC, power, or current.

This indicator is crucial to ensure that charge or discharge power remains within specific limits, optimizing the battery's lifespan. It is particularly useful in peak power applications to define battery conditions for substantial charges or discharges.

The state of power depends on the state of charge, battery capacity, initial characteristics, chemistry, and battery voltage. [Source](https://epicpower.es/wp-content/uploads/2020/08/AN028_SoC-SoH-SoP-definitions_v3.pdf)

## Microgrid Power Terminology

Within the SDK, the terminology used for [measurements](#measurement), such as the power of individual [microgrid](#microgrid) [components](#component), follow the pattern `{component}_{quantity}` for total values and `{component}_{consumption,production}_{quantity}` for clipped values.

Valid components include `grid`, `battery`, `ev_charger`, `consumer`, and active components `pv`, `chp`, and `wind`.

The SDK provides the following power metrics IDs.

### `grid_power`

Can be positive (indicating consumption from the grid) or negative (indicating production into the grid).

### `{battery,ev_charger}_power`

Can be positive (indicating battery charging) or negative (indicating battery discharging). This also applies to EV charging stations.

### `{pv,chp,wind}_power`

Pertaining to active [components](#component), this represents power generated from local (renewable) sources. It is negative when electricity is generated, otherwise zero or positive in case of self-consumption. Alternative terms include generation, supply, and source.

### `consumer_power`

Aggregates components not covered above. Under typical circumstances, this corresponds to the site's gross consumption, excluding active parts and the battery. The term 'consumer' implies that this component predominantly consumes electric power. It can support negative values for exotic site topologies or short-term effects.

### `{component}_consumption_power`

Positive power value if `{component}_power` is positive, otherwise zero.

### `{component}_production_power`

Positive power value if `{component}_power` is negative, otherwise zero.
