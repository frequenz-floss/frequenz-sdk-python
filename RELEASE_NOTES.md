# Frequenz Python SDK Release Notes

## Bug Fixes

- The resampler now properly handles zero values.

  A bug made the resampler interpret zero values as `None` (or NaN), producing wrong resampled averages.

  In extreme cases where a long stream of zero values longer than the buffer size was sent, the resampler would just start producing `None` values.
