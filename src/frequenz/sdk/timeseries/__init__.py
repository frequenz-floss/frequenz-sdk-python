# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Handling of timeseries streams.

A timeseries is a stream (normally an async iterator) of
[`Sample`][frequenz.sdk.timeseries.Sample]s.
"""

from ._base_types import Sample

__all__ = [
    "Sample",
]
