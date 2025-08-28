# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Handling of timeseries streams.

A timeseries is a stream (normally an async iterator) of
[`Sample`][frequenz.sdk.timeseries.Sample]s.

# Periodicity and alignment

All the data produced by this package is always periodic, in UTC, and aligned to the
[Epoch](https://en.wikipedia.org/wiki/Epoch_(computing)) (by default).

Classes normally take a (re)sampling period as and argument and, optionally, an
`align_to` argument.

This means timestamps are always separated exactly by a period, and that this
timestamp falls always at multiples of the period, starting at the `align_to`.

This ensures that the data is predictable and consistent among restarts.

Example:
    If we have a period of 10 seconds, and are aligning to the UNIX
    epoch. Assuming the following timeline starts in 1970-01-01 00:00:00
    UTC and our current `now` is 1970-01-01 00:00:32 UTC, then the next
    timestamp will be at 1970-01-01 00:00:40 UTC:

    ```
    align_to = 1970-01-01 00:00:00         next event = 1970-01-01 00:00:40
    |                                       |
    |---------|---------|---------|-|-------|---------|---------|---------|
    0        10        20        30 |      40        50        60        70
                                   now = 1970-01-01 00:00:32
    ```
"""

from .._internal._channels import ReceiverFetcher
from ._base_types import Bounds, Sample, Sample3Phase
from ._fuse import Fuse
from ._moving_window import MovingWindow
from ._periodic_feature_extractor import PeriodicFeatureExtractor
from ._resampling._base_types import Sink, Source, SourceProperties
from ._resampling._config import (
    DEFAULT_BUFFER_LEN_INIT,
    DEFAULT_BUFFER_LEN_MAX,
    DEFAULT_BUFFER_LEN_WARN,
    ResamplerConfig,
    ResamplerConfig2,
    ResamplingFunction,
    ResamplingFunction2,
)
from ._resampling._exceptions import ResamplingError, SourceStoppedError
from ._resampling._wall_clock_timer import (
    ClocksInfo,
    TickInfo,
    WallClockTimer,
    WallClockTimerConfig,
)

__all__ = [
    "Bounds",
    "ClocksInfo",
    "DEFAULT_BUFFER_LEN_INIT",
    "DEFAULT_BUFFER_LEN_MAX",
    "DEFAULT_BUFFER_LEN_WARN",
    "Fuse",
    "MovingWindow",
    "PeriodicFeatureExtractor",
    "ReceiverFetcher",
    "ResamplerConfig",
    "ResamplerConfig2",
    "ResamplingError",
    "ResamplingFunction",
    "ResamplingFunction2",
    "Sample",
    "Sample3Phase",
    "Sink",
    "Source",
    "SourceProperties",
    "SourceStoppedError",
    "TickInfo",
    "WallClockTimer",
    "WallClockTimerConfig",
]
