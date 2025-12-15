# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""A peekable iterator implementation."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Generic, TypeVar

from typing_extensions import override

_T = TypeVar("_T")


class Peekable(Generic[_T], Iterator[_T]):
    """Create a Peekable iterator from an existing iterator."""

    def __init__(self, iterator: Iterator[_T]):
        """Initialize this instance.

        Args:
            iterator: The underlying iterator to wrap.
        """
        self._iterator: Iterator[_T] = iterator
        self._buffer: _T | None = None

    @override
    def __iter__(self) -> Peekable[_T]:
        """Return the iterator itself."""
        return self

    @override
    def __next__(self) -> _T:
        """Return the next item from the iterator."""
        if self._buffer is not None:
            item = self._buffer
            self._buffer = None
            return item
        return next(self._iterator)

    def peek(self) -> _T | None:
        """Return the next item without advancing the iterator.

        Returns:
            The next item, or `None` if the iterator is exhausted.
        """
        if self._buffer is None:
            try:
                self._buffer = next(self._iterator)
            except StopIteration:
                return None
        return self._buffer
