# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""A resampler configuration for examples and quick experiments."""

from datetime import timedelta
from typing import Final

from ..timeseries import ResamplerConfig3
from ._resampling_functions import latest_window_mean

EXAMPLE_RESAMPLER_CONFIG: Final[ResamplerConfig3] = ResamplerConfig3(
    timedelta(seconds=1),
    max_data_age_in_periods=3.0,
    resampling_function=latest_window_mean,
)
"""A resampler configuration for examples and quick experiments.

Applications should pass their own configuration to
[`initialize()`][frequenz.sdk.microgrid.initialize], but this one is a good
baseline for an application that controls a microgrid.

To control a microgrid, tolerating short gaps in the data is usually more
important than having each value cover only its own window. A small glitch in a
data stream, or a device that sends data less often than the resampling period,
should not make resampled values `None`. This is why this configuration uses
[`latest_window_mean()`][frequenz.sdk.microgrid.latest_window_mean] with a
[`max_data_age_in_periods`][frequenz.sdk.timeseries.ResamplerConfig3.max_data_age_in_periods]
of 3:

- Each value is the mean of the samples in its window. If the window has no
  samples, the newest of the two previous windows with samples is used instead,
  so its values are reused with a newer timestamp.
- The maximum data age is relative to the input period for devices that send
  data less often than the resampling period, so their last value is repeated
  while it is not older than three input periods.
- Values are noisier than the mean of several windows, as each value only uses
  the samples of one window.

The maximum data age doesn't guarantee that gaps of that size are covered. Until
the resampler has estimated the input period of a device, which needs
[`input_period_estimation_samples`][frequenz.sdk.timeseries.ResamplerConfig3.input_period_estimation_samples]
samples after a start, the maximum data age is relative to the resampling
period, so resampled values of slow devices can be `None`.

The resampling period of 1 second keeps the resampled data close to real time.
The other settings are the defaults of
[`ResamplerConfig3`][frequenz.sdk.timeseries.ResamplerConfig3], so each value
covers its window from its start, included, to its end, excluded, and its
timestamp is the start of the window.

To change some of the values, for example the resampling period, use
[`dataclasses.replace()`][dataclasses.replace]:

```python
import dataclasses
from datetime import timedelta

from frequenz.sdk import microgrid

config = dataclasses.replace(
    microgrid.EXAMPLE_RESAMPLER_CONFIG, resampling_period=timedelta(seconds=5)
)
```
"""
