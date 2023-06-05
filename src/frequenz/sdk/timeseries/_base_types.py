# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries basic types."""

import functools
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Generic, Iterator, Self, overload

from ._quantities import QuantityT

UNIX_EPOCH = datetime.fromtimestamp(0.0, tz=timezone.utc)
"""The UNIX epoch (in UTC)."""


@dataclass(frozen=True, order=True)
class Sample(Generic[QuantityT]):
    """A measurement taken at a particular point in time.

    The `value` could be `None` if a component is malfunctioning or data is
    lacking for another reason, but a sample still needs to be sent to have a
    coherent view on a group of component metrics for a particular timestamp.
    """

    timestamp: datetime
    """The time when this sample was generated."""

    value: QuantityT | None = None
    """The value of this sample."""


@dataclass(frozen=True)
class Sample3Phase(Generic[QuantityT]):
    """A 3-phase measurement made at a particular point in time.

    Each of the `value` fields could be `None` if a component is malfunctioning
    or data is lacking for another reason, but a sample still needs to be sent
    to have a coherent view on a group of component metrics for a particular
    timestamp.
    """

    timestamp: datetime
    """The time when this sample was generated."""
    value_p1: QuantityT | None
    """The value of the 1st phase in this sample."""

    value_p2: QuantityT | None
    """The value of the 2nd phase in this sample."""

    value_p3: QuantityT | None
    """The value of the 3rd phase in this sample."""

    def __iter__(self) -> Iterator[QuantityT | None]:
        """Return an iterator that yields values from each of the phases.

        Yields:
            Per-phase measurements one-by-one.
        """
        yield self.value_p1
        yield self.value_p2
        yield self.value_p3

    @overload
    def max(self, default: QuantityT) -> QuantityT:
        ...

    @overload
    def max(self, default: None = None) -> QuantityT | None:
        ...

    def max(self, default: QuantityT | None = None) -> QuantityT | None:
        """Return the max value among all phases, or default if they are all `None`.

        Args:
            default: value to return if all phases are `None`.

        Returns:
            Max value among all phases, if available, default value otherwise.
        """
        if not any(self):
            return default
        value: QuantityT = functools.reduce(
            lambda x, y: x if x > y else y,
            filter(None, self),
        )
        return value

    @overload
    def min(self, default: QuantityT) -> QuantityT:
        ...

    @overload
    def min(self, default: None = None) -> QuantityT | None:
        ...

    def min(self, default: QuantityT | None = None) -> QuantityT | None:
        """Return the min value among all phases, or default if they are all `None`.

        Args:
            default: value to return if all phases are `None`.

        Returns:
            Min value among all phases, if available, default value otherwise.
        """
        if not any(self):
            return default
        value: QuantityT = functools.reduce(
            lambda x, y: x if x < y else y,
            filter(None, self),
        )
        return value

    def map(
        self,
        function: Callable[[QuantityT], QuantityT],
        default: QuantityT | None = None,
    ) -> Self:
        """Apply the given function on each of the phase values and return the result.

        If a phase value is `None`, replace it with `default` instead.

        Args:
            function: The function to apply on each of the phase values.
            default: The value to apply if a phase value is `None`.

        Returns:
            A new instance, with the given function applied on values for each of the
                phases.
        """
        return self.__class__(
            timestamp=self.timestamp,
            value_p1=default if self.value_p1 is None else function(self.value_p1),
            value_p2=default if self.value_p2 is None else function(self.value_p2),
            value_p3=default if self.value_p3 is None else function(self.value_p3),
        )
