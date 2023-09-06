## Microgrid

### Component

A device in the microgrid. For example, inverter, battery, meter, etc.

### Component ID

A way to identify a particular instance of a component. It is always a `int`.

For example: Battery with component ID 5.

## Component data

### Metric

A measurable characteristic of a component.

For example: the capacity of a battery.

### Measurement  / Sample value

An individual value measured from a component instance metric. It is always `float` but it is expressed in some particular unit.

In the context of a Sample (see below) this is usually referred to as a *sample value*.

For example: A measurement of the capacity of battery with component ID 5 can be 400. The unit is typically Watt-hour (Wh).

### Timestamp

A point in time. It is always a `datetime`.

### Sample

A measurement taken at a particular point in time: a tuple `(timestamp, measurement)`, or simply `(timestamp, value)`.

For example: Getting the measurement of 400 from the capacity of a battery at 2022-01-01 22:00:00.000 UTC would be a sample (2022-01-01 22:00:00.000 UTC, 400).

### Timeseries

A sequence of *Samples*. Normally time series should be sorted by timestamp and timestamps should be separated at regular intervals, but there can be also irregular (event-based) time series (in this case there could even possibly be multiple values with the same timestamp).

For example: Getting the measurement from the capacity of a battery at 2022-01-01 22:00:00.000 UTC every second for 5 seconds would be a timeseries like:

```
(2022-01-01 22:00:00.000 UTC, 400)
(2022-01-01 22:00:01.000 UTC, 401)
(2022-01-01 22:00:02.000 UTC, 403)
(2022-01-01 22:00:03.000 UTC, 402)
(2022-01-01 22:00:04.000 UTC, 403)
```

### Metric ID

A way to identify a component's metric. Usually a `str`.

For example, the metric ID of the capacity of a battery is just `capacity`.

### Timeseries ID

A way to identify a timeseries that comes from a metric of a particular component instance. Usually a `str`, but coming from the tuple (component ID, metric ID) for components.

For example: A timeseries for the capacity of battery with component ID 5 has ID (component_id, metric_id) (or `f"{component_id}_{metric_id}"`).

## Metrics

### SoC - State of Charge

The level of charge of a battery relative to its capacity. The units of SoC are percentage points and it is calculated as the ratio
between the remaining energy in the battery at a given time and the maximum possible energy with the same state of health conditions. [Source](https://epicpower.es/wp-content/uploads/2020/08/AN028_SoC-SoH-SoP-definitions_v3.pdf)

### SoP - State of Power

The ratio of peak power to nominal power. The peak power, based on present battery-pack conditions, is the maximum power that may be maintained constant for T seconds without violating preset operational design limits on battery voltage, SOC, power, or current.

This indicator is very important to ensure that the charge or discharge power does not exceed certain limits with the aim of using the battery as good as possible to extend its life expectancy. Also, in peak power applications this indicator can turn useful to define conditions in the battery to be able to make big charges or discharges.

The state of power depends highly on the state of charge, the capacity of the battery and its initial features, chemistry and battery voltage.  [Source](https://epicpower.es/wp-content/uploads/2020/08/AN028_SoC-SoH-SoP-definitions_v3.pdf)

## Microgrid Power Terminology
In the SDK, the terminology used for quantities such as power of individual microgrid components follows `{component}_{quantity}` for total quantities and `{component}_{consumption,production}_{quantity}` for clipped quantities, respectively. Valid components are: `grid`, `battery`, `ev_charger`, `consumer` and active components `pv`, `chp` and `wind`.

The SDK exposes following power metrics:

* **grid_power**: Can be positive (consumption from the grid) or negative (production into the grid).

* **{battery,ev_charger}_power**: Can be positive (charge battery) or negative (discharge battery). Equivalently for EV charging station.

* **{pv,chp,wind}_power**: Active components, the power from local (renewable) generation. Negative, if electric power is generated, otherwise zero or positive in case of self-consumption of the generator. Alternative terms: generation, supply, source.

* **consumer_power**: This aggregates the remaining parts not covered by the above components. Under normal circumstances this is expected to correspond to the gross consumption of the site excluding active parts and battery. The implementation still supports negative values to also handle exotic site topologies (e.g. unidentified generators) and sudden short-term effects.  The term `consumer` was deliberately chosen to indicate that this component is expected to predominantly consume electric power. Under normal circumstances there should be no active sources in this component and the metrics `consumer_{consumption,production}_power` would not be used. Alternative terms: load, demand, sink.


In addition to that, for each of the above there will two additional metrics:

* **{component}_consumption_power**: Positive power value if {component}_power is positive, otherwise zero.
* **{component}_production_power**: Positive power value if {component}_power is negative, otherwise zero.

Other terminology found in microgrid literature but not exposed by the SDK:

* **_Gross consumption_**: Consumption before accounting for any local generation from solar, wind or CHP.
* **_Net consumption/load_**: This term traditionally refers to the difference between the gross electricity consumption and the local generation (like PV production). This is the electricity consumption that needs to be met by the battery or from the main grid. This means `consumer_power + {pv,chp,wind}_power` (note that latter is negative when electricity is produced). Since this term caused confusion in the past, it's not exposed by the SDK.
* **_Residual consumption/load_**: In microgrid context sometimes used as the remaining difference between the net consumption and the battery power, i.e. what we define as grid power.
