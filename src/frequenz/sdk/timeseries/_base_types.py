# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries basic types."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# Ordering by timestamp is a bit arbitrary, and it is not always what might be
# wanted. We are using this order now because usually we need to do binary
# searches on sequences of samples, and the Python `bisect` module doesn't
# support providing a key until Python 3.10.
@dataclass(frozen=True, order=True)
class Sample:
    """A measurement taken at a particular point in time.

    The `value` could be `None` if a component is malfunctioning or data is
    lacking for another reason, but a sample still needs to be sent to have a
    coherent view on a group of component metrics for a particular timestamp.
    """

    timestamp: datetime = field(compare=True)
    """The time when this sample was generated."""

    value: Optional[float] = field(compare=False, default=None)
    """The value of this sample."""


@dataclass(frozen=True)
class Sample3Phase:
    """A 3-phase measurement made at a particular point in time.

    Each of the `value` fields could be `None` if a component is malfunctioning
    or data is lacking for another reason, but a sample still needs to be sent
    to have a coherent view on a group of component metrics for a particular
    timestamp.
    """

    timestamp: datetime
    """The time when this sample was generated."""
    value_p1: Optional[float]
    """The value of the 1st phase in this sample."""

    value_p2: Optional[float]
    """The value of the 2nd phase in this sample."""

    value_p3: Optional[float]
    """The value of the 3rd phase in this sample."""
