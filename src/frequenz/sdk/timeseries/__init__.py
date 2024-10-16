# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Handling of timeseries streams.

A timeseries is a stream (normally an async iterator) of
[`Sample`][frequenz.sdk.timeseries.Sample]s.

# Periodicity and alignment

All the data produced by this package is always periodic and aligned to the
`UNIX_EPOCH` (by default).

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
from ._base_types import UNIX_EPOCH, Bounds, Sample, Sample3Phase
from ._fuse import Fuse
from ._moving_window import MovingWindow
from ._periodic_feature_extractor import PeriodicFeatureExtractor
from ._resampling import ResamplerConfig

__all__ = [
    "Bounds",
    "Fuse",
    "MovingWindow",
    "PeriodicFeatureExtractor",
    "ResamplerConfig",
    "ReceiverFetcher",
    "Sample",
    "Sample3Phase",
    "UNIX_EPOCH",
]
