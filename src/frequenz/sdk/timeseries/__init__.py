"""
Handling of timeseries streams.

A timeseries is a stream (normally an async iterator) of
[samples][frequenz.sdk.timeseries.Sample].

This module provides tools to operate on timeseries.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from ._resampler import GroupResampler, Resampler, ResamplingFunction
from .sample import Sample

__all__ = [
    "GroupResampler",
    "Resampler",
    "ResamplingFunction",
    "Sample",
]
