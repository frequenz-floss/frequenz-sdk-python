# Glossary

This glossary provides definitions for common terminology used in the Frequenz SDK,
focusing on microgrid components, metrics, measurements, and power-related terms.

## Common Acronyms

### AC

Alternating current. See the [Wikipedia
article](https://en.wikipedia.org/wiki/Alternating_current) for more details.

### BMS

Battery management system. See the [Wikipedia
article](https://en.wikipedia.org/wiki/Battery_management_system) for more
details.

### CHP

Combined heat and power. See the [Wikipedia
article](https://en.wikipedia.org/wiki/Combined_heat_and_power) for more
details.

### DC

Direct current. See the [Wikipedia
article](https://en.wikipedia.org/wiki/Direct_current) for more details.

### EV

Electric vehicle. See the [Wikipedia
article](https://en.wikipedia.org/wiki/Electric_vehicle) for more details.

### PSC

[Passive sign convention](#passive-sign-convention). See the [Wikipedia
article](https://en.wikipedia.org/wiki/Passive_sign_convention) for more
details.

### PV

Photovoltaic. See the [Wikipedia
article](https://en.wikipedia.org/wiki/Photovoltaics) for more details.

In the SDK it is normally used as a synonym for [solar panel](#solar-panel).

### SoC

[State of charge](#state-of-charge). See the [Wikipedia
article](https://en.wikipedia.org/wiki/State_of_charge) for more details.

### SoP

[State of power](#state-of-power).

## Microgrid

A local electrical grid that connects a set of different [types of
components](#component-category) together. It can be connected to the public
[grid](#grid), or be completely isolated, in which case it is known as an
island.

### Component Category

The category [components](#component) of a [microgrid](#microgrid) belong to.

[Components](#component) of the same category have the same characteristics
(for example offer the same set of [metrics](#metric)).

#### Battery

A storage system for electrical energy.

#### CHP Plant

A generator that produces combined heat and power ([CHP](#chp)). Usually
powered via combustion of some form of fuel.

#### Converter

Generally refers to [DC-to-DC converter](#dc-to-dc-converter).

#### DC-to-DC Converter

An electronic circuit or electromechanical device that converts a source of
[DC](#dc) from one voltage level to another.

#### EV Charger

A station for charging [EVs](#ev).

#### Electrolyzer

A device that converts water into hydrogen and oxygen.

#### Grid

A point where the local [microgrid](#microgrid) is connected to the public
electricity grid.

#### Inverter

A device or circuitry that converts [DC](#dc) electricity to [AC](#ac)
electricity.

#### Meter

A device for measuring electrical [metrics](#metrics) (for example current,
voltage, etc.).

#### PV Array

A collection of [PV](#pv) panels.

#### Pre-charge module

A device that gradually ramp the [DC](#dc) voltage up to prevent any potential
damage to sensitive electrical components, like capacitors.

While many [inverters](#inverter) and [batteries](#battery) come equipped with
in-built pre-charging mechanisms, some may lack this feature. In such cases,
external pre-charging modules can be used.

#### Relay

A device that generally have two states: open (connected) and closed
(disconnected).

They are generally placed in front of another [component](#component), e.g., an
[inverter](#inverter), to control whether the component is connected to the
[microgrid](#microgrid) or not.

#### Sensor

A device for [measuring](#measurement] ambient [metrics](#metric) (for example
temperature, humidity, etc.).

#### Solar Panel

A panel with [PV](#pv) cells that generates [DC](#dc) electricity from
sunlight.

#### Wind Turbine

A device that converts the wind's kinetic energy into electrical energy.

### Component

A device (of a particular [category](#component-category)) within
a [microgrid](#microgrid).

### Component ID

A numeric identifier uniquely representing an instance of
a [component](#component) in a particular [microgrid](#microgrid). It is always
of type `int`.

For example, a battery with a component ID of **5**.

### Component Graph

A [graph](https://en.wikipedia.org/wiki/Graph_(abstract_data_type))
representation of the configuration in which the electrical components in a
microgrid are connected with each other.  Some of the ways in which the SDK uses
the component graph are:

  - figure out how to calculate high level metrics like
[`grid_power`][frequenz.sdk.timeseries.logical_meter.LogicalMeter.grid_power],
[`consumer_power`][frequenz.sdk.timeseries.logical_meter.LogicalMeter.consumer_power],
etc. for a microgrid, using the available components.
  - identify the available {{glossary("battery", "batteries")}} or
    {{glossary("EV charger", "EV chargers")}} at a site that can be controlled.

### Island

A [microgrid](#microgrid) that is not connected to the public electricity
[grid](#grid).

### Passive Sign Convention

A convention for the direction of power flow in a circuit. When the electricity
is flowing into a [component](#component) the value is positive, and when it is
flowing out of a component the value is negative.

In microgrids that have a grid connection, power flowing away from the grid is
positive, and power flowing towards the grid is negative.

## Component Data

### Metric

A quantifiable attribute of a [component](#component).

For example, the metric **capacity** of a battery.

### Measurement

An individual numeric value obtained from a [metric](#metric) of
a [component](#component) instance. It is consistently of type `float`, but it
is often expressed in specific units.

In the context of a sample, this is commonly referred to as a *sample value*.

For example, a measurement of the capacity of a battery with component ID 5 can
be **400**, typically measured in Watt-hours (Wh).

### Timestamp

A specific point in time, always represented as a `datetime` with a `timezone`
attached.

For example, **2022-01-01 22:00:00.000 UTC**.

### Sample

A [measurement](#measurement) recorded at a particular [timestamp](#timestamp),
typically represented as a tuple `(timestamp, value)`.

For example, recording a measurement of 400 from the capacity of a battery at
2022-01-01 22:00:00.000 UTC would constitute a sample **`(2022-01-01
22:00:00.000 UTC, 400)`**.

#### Sample Value

A [measurement](#measurement) stored in a [sample](#sample).

### Time Series

A sequence of [samples](#sample), often organized by [timestamp](#timestamp)
and typically with regular intervals. However, irregular (event-based) time
series are also possible.

For example, a time series representing measurements of a battery's capacity at
2022-01-01 22:00:00.000 UTC every second for 5 seconds would appear as follows:

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

An identifier for a [component](#component)'s [metric](#metric), typically
a string (`str`).

Components belonging to the same [category](#component-category) have the same
set of metric IDs.

For example, the metric ID for the capacity of a [battery](#battery) is simply
**`capacity`**.

### Time Series ID

An identifier for a [time series](#time-series) originating from
a [metric](#metric) of a specific component. Typically a string (`str`) derived
from the tuple **([component ID](#component-id), [metric ID](#metric-id))** for
[components](#component).

For example, a time series for the capacity of a battery with component ID
5 has the ID **(component_id, metric_id)** (or
**`f"{component_id}_{metric_id}"`**).

### Timeseries ID

Same as [time series ID](#time-series-id).

## Metrics

All metrics related to power, energy, current, etc. use the [PSC](#psc) to
determine the sign of the value.

### Consumption

The amount of electricity flowing into a [component](#component). It is the
clipped positive value when using the [PSC](#psc), so if the electricity is
flowing out of the component instead, this will be zero.

### Gross Consumption

[Consumption](#consumption) before accounting for any local generation from
[solar](#solar-panel), [wind](#wind-turbine) or [CHP](#chp-plant).

### Instantaneous Power

Same as [power](#power).

### Load

Typically refers to a device that [consumes](#consumption) electricity, but also
to the amount of electricity [consumed](#consumption) by such a device. In
a [microgrid](#microgrid) context, it is often used to refer to all the
electrical devices that are doing active work, for example a light bulb,
a motor, a cooler, etc.

When using the [PSC](#psc), this is the same as [consumption](#consumption) and
it is a positive value. If a *load* [generates](#production) electricity
instead, it is a negative value but it wouldn't typically be called a *load*.

### Net Consumption

This term traditionally refers to the difference between the [gross
consumption](#gross-consumption) and the local generation (like [PV](#pv)
[production](#production)). It is the electricity [consumption](#consumption)
that needs to be provided by the [battery](#battery) or from the public
[grid](#grid).

### Net Load

Same as [net consumption](#net-consumption).

### Power

The rate of energy transfer, i.e. the amount of energy transferred per unit of
time. It is typically measured in Watts (W).

For [AC](#ac) electricity, there are three types of power:
[active](#active-power), [reactive](#reactive-power), and
[apparent](#apparent-power) (P, Q and |S| respectively in [power
triangle](#power-triangle)).

See the [Wikipedia article](https://en.wikipedia.org/wiki/AC_power) for more
information.

#### Power Triangle

The visual representation of the relationship between the three types of
[AC](#ac) [power](#power) and [phase of voltage relative to
current](#phase-of-voltage-relative-to-current).

![Power triangle](../img/power-triangle.svg)

([CC-BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/deed.en),
[Wikimedia Commons](https://commons.wikimedia.org/wiki/File:Cmplxpower.svg))

#### Active Power

The [AC](#ac) power that is actually consumed by the [load](#load). It is the
real part of the [apparent power](#apparent-power), P in the [power
triangle](#power-triangle).

#### Apparent Power

The [AC](#ac) power that is actually supplied to the [load](#load). The
magnitude of the vector sum of the [active](#active-power) and
[reactive](#reactive-power) power, |S| in the
[power-triangle](#power-triangle).

#### Phase of Voltage Relative to Current

The angle of difference (in degrees) between current and voltage in an
[AC](#ac) circuit, φ in the [power triangle](#power-triangle).

#### Power Factor

The ratio of real power to apparent power in an [AC](#ac) circuit. Can be
obtained by computing the cosine of
[φ](#phase-of-voltage-relative-to-current), in the [power
triangle](#power-triangle). See the [Wikipedia
article](https://en.wikipedia.org/wiki/Power_factor) for more details.

#### Reactive Power

The [AC](#ac) power that is not consumed by the [load](#load), but is
alternately stored and returned to the source. It is the imaginary part of the
[apparent power](#apparent-power), Q in the [power triangle](#power-triangle).

#### Real Power

Same as [active power](#active-power).

### Production

The amount of electricity flowing out of a [component](#component). It is the
clipped negative value when using the [PSC](#psc), so if the electricity is
flowing into the component instead, this will be zero.

### Residual Consumption

In [microgrid](#microgrid) context sometimes used as the remaining difference
between the [net consumption](#net-consumption) and the [battery](#battery)
power, i.e. what we define as [grid power](#grid_power).

### Residual Load

Same as [residual consumption](#residual-consumption).

### State of Charge

The level of charge of a [battery](#battery) relative to its capacity,
expressed in percentage points. Calculated as the ratio between the remaining
energy in the battery at a given time and the maximum possible energy under
similar health conditions.
[Source](https://epicpower.es/wp-content/uploads/2020/08/AN028_SoC-SoH-SoP-definitions_v3.pdf)

### State of Power

The ratio of peak power to nominal power. Peak power is the maximum power that
can be sustained for a specific duration without violating preset operational
design limits on battery voltage, SoC, power, or current.

This indicator is crucial to ensure that charge or discharge power remains
within specific limits, optimizing the battery's lifespan. It is particularly
useful in peak power applications to define battery conditions for substantial
charges or discharges.

The state of power depends on the state of charge, battery capacity, initial
characteristics, chemistry, and battery voltage, as well as external factors
like temperature and humidity, which can also have a significant impact on it.
[Source](https://epicpower.es/wp-content/uploads/2020/08/AN028_SoC-SoH-SoP-definitions_v3.pdf)
