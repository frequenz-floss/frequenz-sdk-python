# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries basic types."""

import dataclasses
import functools
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Generic, Protocol, Self, TypeVar, cast, overload

from frequenz.quantities import Power, Quantity

UNIX_EPOCH = datetime.fromtimestamp(0.0, tz=timezone.utc)
"""The UNIX epoch (in UTC)."""

QuantityT = TypeVar("QuantityT", bound=Quantity)
"""Type variable for representing various quantity types."""


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
    def max(self, default: QuantityT) -> QuantityT: ...

    @overload
    def max(self, default: None = None) -> QuantityT | None: ...

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
    def min(self, default: QuantityT) -> QuantityT: ...

    @overload
    def min(self, default: None = None) -> QuantityT | None: ...

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


class Comparable(Protocol):
    """A protocol that requires the implementation of comparison methods.

    This protocol is used to ensure that types can be compared using
    the less than or equal to (`<=`) and greater than or equal to (`>=`)
    operators.
    """

    def __le__(self, other: Any, /) -> bool:
        """Return whether this instance is less than or equal to `other`."""

    def __ge__(self, other: Any, /) -> bool:
        """Return whether this instance is greater than or equal to `other`."""


_T = TypeVar("_T", bound=Comparable | None)


@dataclass(frozen=True)
class Bounds(Generic[_T]):
    """Lower and upper bound values."""

    lower: _T
    """Lower bound."""

    upper: _T
    """Upper bound."""

    def __contains__(self, item: _T) -> bool:
        """
        Check if the value is within the range of the container.

        Args:
            item: The value to check.

        Returns:
            bool: True if value is within the range, otherwise False.
        """
        if self.lower is None and self.upper is None:
            return True
        if self.lower is None:
            return item <= self.upper
        if self.upper is None:
            return self.lower <= item

        return cast(Comparable, self.lower) <= item <= cast(Comparable, self.upper)


@dataclass(frozen=True, kw_only=True)
class SystemBounds:
    """Internal representation of system bounds for groups of components."""

    # compare = False tells the dataclass to not use name for comparison methods
    timestamp: datetime = dataclasses.field(compare=False)
    """Timestamp of the metrics."""

    inclusion_bounds: Bounds[Power] | None
    """Total inclusion power bounds for all components of a pool.

    This is the range within which power requests would be allowed by the pool.

    When exclusion bounds are present, they will exclude a subset of the inclusion
    bounds.
    """

    exclusion_bounds: Bounds[Power] | None
    """Total exclusion power bounds for all components of a pool.

    This is the range within which power requests are NOT allowed by the pool.
    If present, they will be a subset of the inclusion bounds.
    """

    def __contains__(self, item: Power) -> bool:
        """
        Check if the value is within the range of the container.

        Args:
            item: The value to check.

        Returns:
            bool: True if value is within the range, otherwise False.
        """
        if not self.inclusion_bounds or item not in self.inclusion_bounds:
            return False
        if self.exclusion_bounds and item in self.exclusion_bounds:
            return False
        return True
