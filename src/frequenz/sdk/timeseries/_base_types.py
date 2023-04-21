# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries basic types."""

from __future__ import annotations

import functools
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterator, Optional, overload

UNIX_EPOCH = datetime.fromtimestamp(0.0, tz=timezone.utc)
"""The UNIX epoch (in UTC)."""


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

    def __iter__(self) -> Iterator[float | None]:
        """Return an iterator that yields values from each of the phases.

        Yields:
            Per-phase measurements one-by-one.
        """
        yield self.value_p1
        yield self.value_p2
        yield self.value_p3

    @overload
    def max(self, default: float) -> float:
        ...

    @overload
    def max(self, default: None = None) -> float | None:
        ...

    def max(self, default: float | None = None) -> float | None:
        """Return the max value among all phases, or default if they are all `None`.

        Args:
            default: value to return if all phases are `None`.

        Returns:
            Max value among all phases, if available, default value otherwise.
        """
        if not any(self):
            return default
        value: float = functools.reduce(
            lambda x, y: x if x > y else y,
            filter(None, self),
        )
        return value

    @overload
    def min(self, default: float) -> float:
        ...

    @overload
    def min(self, default: None = None) -> float | None:
        ...

    def min(self, default: float | None = None) -> float | None:
        """Return the min value among all phases, or default if they are all `None`.

        Args:
            default: value to return if all phases are `None`.

        Returns:
            Min value among all phases, if available, default value otherwise.
        """
        if not any(self):
            return default
        value: float = functools.reduce(
            lambda x, y: x if x < y else y,
            filter(None, self),
        )
        return value

    def map(
        self, function: Callable[[float], float], default: float | None = None
    ) -> Sample3Phase:
        """Apply the given function on each of the phase values and return the result.

        If a phase value is `None`, replace it with `default` instead.

        Args:
            function: The function to apply on each of the phase values.
            default: The value to apply if a phase value is `None`.

        Returns:
            A new `Sample3Phase` instance, with the given function applied on values
                for each of the phases.
        """
        return Sample3Phase(
            timestamp=self.timestamp,
            value_p1=default if self.value_p1 is None else function(self.value_p1),
            value_p2=default if self.value_p2 is None else function(self.value_p2),
            value_p3=default if self.value_p3 is None else function(self.value_p3),
        )
