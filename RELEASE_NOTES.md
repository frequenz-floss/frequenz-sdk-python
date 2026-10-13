# Frequenz Python SDK Release Notes

## Summary

This release updates the microgrid component graph library to v0.6.0, which stops clamping the consumer and producer formulas.  Consumer power can now be negative, and a producer that draws power now makes the producer total less negative instead of counting as zero.

## Upgrading

- The minimum supported version of [`frequenz-microgrid-component-graph`](https://github.com/frequenz-floss/frequenz-microgrid-component-graph-python) is now [v0.6.0](https://github.com/frequenz-floss/frequenz-microgrid-component-graph-python/releases/tag/v0.6.0), which stops clamping the consumer and producer formulas. This changes the values streamed by `microgrid.consumer().power` and `microgrid.producer().power`.
  - Consumer power used to be clamped at zero. It can now be negative, for example when unmodeled production or a measurement mismatch is larger than the consumption. To get the old result, use `consumer.power.max(Power.zero()).build("consumer_power")`.
  - Producer power used to clamp each producer at zero. A producer that draws power now adds a positive value instead of zero, so the total can be positive. For example, PV producing 10 kW and a CHP drawing 2 kW used to give -10 kW and now give -8 kW. There is one exception: when the two share a meter below the grid meter, that meter sends data, and `disable_fallback_components` is off, the meter measures them together, so they gave -8 kW before too. `producer.power.min(Power.zero()).build("producer_power")` clamps the total at zero, but it still differs from the old result when one producer draws power while another produces.
  - With `ComponentGraphConfig(include_phantom_loads_in_consumer_formula=True)`, consumer power is unchanged: it still clamps each of its terms at zero.

## Bug Fixes

* Make the PowerManager fall back to zero instead of changing the requested power direction when a target is snapped outside asymmetric exclusion bounds.
* The battery pool no longer drops the cached data of batteries that stop working. A battery that started working again was left out of the pool's metrics until its next data sample arrived. When every working battery had just started working again, `system_power_bounds` briefly reported no bounds, and if an actor had requested power, the power manager set the target power to zero.
* After a power request to a battery's inverter fails or times out, the next request that includes that inverter is now always sent, even if it is 0 W. Before, a 0 W request was skipped when the last successful request to that inverter was also 0 W. The failed request may have been applied anyway, so the battery could keep running at that request's power. As before, a battery whose request failed is left out of new requests for a while: 1 second at first, and up to 30 seconds if its requests keep failing. This only happens while another battery in the new request is working. If none is, the failed battery is used again right away.
