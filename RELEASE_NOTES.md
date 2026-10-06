# Frequenz Python SDK Release Notes

## Summary

This release updates the microgrid component graph library to v0.5.2, which stops clamping the consumer and producer formulas.  Consumer power can now be negative, and a producer that draws power now makes the producer total less negative instead of counting as zero.

## Upgrading

- The minimum supported version of [`frequenz-microgrid-component-graph`](https://github.com/frequenz-floss/frequenz-microgrid-component-graph-python) is now [v0.5.2](https://github.com/frequenz-floss/frequenz-microgrid-component-graph-python/releases/tag/v0.5.2), which stops clamping the consumer and producer formulas. This changes the values streamed by `microgrid.consumer().power` and `microgrid.producer().power`.
  - Consumer power used to be clamped at zero. It can now be negative, for example when unmodeled production or a measurement mismatch is larger than the consumption. To get the old result, use `consumer.power.max(Power.zero()).build("consumer_power")`.
  - Producer power used to clamp each producer at zero. A producer that draws power now adds a positive value instead of zero, so the total can be positive. For example, PV producing 10 kW and a CHP drawing 2 kW used to give -10 kW and now give -8 kW. There is one exception: when the two share a meter below the grid meter, that meter sends data, and `disable_fallback_components` is off, the meter measures them together, so they gave -8 kW before too. `producer.power.min(Power.zero()).build("producer_power")` clamps the total at zero, but it still differs from the old result when one producer draws power while another produces.
  - With `ComponentGraphConfig(include_phantom_loads_in_consumer_formula=True)`, consumer power is unchanged: it still clamps each of its terms at zero.

