# Frequenz Python SDK Release Notes

## Bug Fixes

- The resampler now properly handles sending zero values.

  A bug made the resampler interpret zero values as `None` when generating new samples, so if the result of the resampling is zero, the resampler would just produce `None` values.
