# Release Notes

## Summary

This release improves the performance of the `ComponentMetricsResamplingActor`,
and fixes a bug in the `LogicalMeter` that was causing it to crash.

## Bugfixes

- The Resampling actor sends None values out when there is no data from a
  component. The logical meter used to raise an exception if it saw a `None`
  value from any of its streams.  The logical meter is now able to handle `None`
  values by propagating the `None` values,  or treating them as `0`s.
