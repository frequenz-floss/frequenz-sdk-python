# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries resampler buffer handling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, TypeVar, overload

T = TypeVar("T")


@dataclass
class _Uninitialized:
    """A sentinel class to mark uninitialized data in the buffer."""


_UNINITIALIZED = _Uninitialized()
"""A uninitialized sentinel instance."""


class _BufferView(Sequence[T]):
    """A read-only view into a Buffer.

    This is a ring buffer with the following structure:

    * It is a fixed size buffer.
    * If the buffer isn't full, elements not yet filled up are represented with
      a `_UNINITIALIZED` instance.
    * The `tail` pointer points to the first element in the view.
    * The `head` pointer points past the last element in the view.
    * The ring buffer can, of course, wrap around, so `head` can be smaller than `tail`.
    * When `head` == `tail`, the view is empty, unless `_is_full` is `True`, in
      which case the view covers the whole buffer.
    """

    def __init__(
        self, *, buffer: list[_Uninitialized | T], full: bool, tail: int, head: int
    ) -> None:
        """Create an instance.

        Args:
            buffer: The internal buffer that needs to be viewed.
            full: Whether this is a view on a full buffer.
            tail: The index of the first element of the view.
            head: The index of the last element of the view.
        """
        super().__init__()
        self._buffer: list[_Uninitialized | T] = buffer
        self._is_full: bool = full
        self._tail: int = tail  # Points to the oldest element in the buffer
        self._head: int = head  # Points to the newest element in the buffer

    @property
    def capacity(self) -> int:
        """Return the capacity of the internal buffer.

        Returns:
            The capacity of the internal buffer.
        """
        return len(self._buffer)

    def _adjust(self, position: int, *, offset: int = 0) -> int:
        """Adjust a position to point to a valid index in the buffer.

        An optional `offset` can be passed if the position needs to be also
        updated using some offset.

        Args:
            position: The position index to adjust.
            offset: The offset to use to calculate the new position.

        Returns:
            The adjusted position index.
        """
        return (position + offset) % self.capacity

    def _index(self, /, index: int) -> T:
        """Get the nth element in this view.

        Args:
            index: The index of the element to get.

        Returns:
            The element in the nth position.
        """
        if index < 0:
            index += len(self)
        if index < 0:
            raise IndexError("Buffer index out of range")
        if index >= len(self):
            raise IndexError("Buffer index out of range")
        index = self._adjust(self._tail, offset=index)
        item = self._buffer[index]
        assert not isinstance(item, _Uninitialized)
        return item

    def _slice(self, /, slice_: slice) -> Sequence[T]:
        """Get a slice on this view.

        Args:
            slice_: The parameters of the slice to get.

        Returns:
            The slice on this view.
        """
        start, stop, step = slice_.indices(len(self))
        # This is a bit expensive, but making all the calculation each time
        # an item is accessed would also be expensive. This way the slice
        # might be expensive to construct but then is super cheap to use,
        # which should probably be the most common sense. We also avoid
        # having to implement a lot of weird corner cases.
        if step != 1:
            new_slice: list[T] = []
            for i in range(start, stop, step):
                new_slice.append(self[i])
            return new_slice

        # If we have a step of 1, then things are very simple, and we can
        # use another view on the same buffer. This is also probably the
        # most common slicing case.

        # If the requested size is empty, then just return an empty tuple
        if start >= stop:
            return ()

        tail = self._adjust(self._tail, offset=start)
        head = self._adjust(self._tail, offset=stop)
        slice_is_full = stop - start == self.capacity
        return _BufferView(
            buffer=self._buffer, full=slice_is_full, head=head, tail=tail
        )

    @overload
    def __getitem__(self, index: int) -> T:
        ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[T]:
        ...

    def __getitem__(self, index: int | slice) -> T | Sequence[T]:
        """Get an item or slice on this view.

        Args:
            index: The index of the element or parameters of the slice to get.

        Returns:
            The element or slice on this view.
        """
        if isinstance(index, slice):
            return self._slice(index)

        return self._index(index)

    def __len__(self) -> int:
        """Get the length of this view.

        Returns:
            The length of this view.
        """
        if self._is_full:
            return self.capacity
        if self._head == self._tail:
            return 0
        offset = self.capacity if self._head < self._tail else 0
        return offset + self._head - self._tail

    def __repr__(self) -> str:
        """Get the string representation of this view.

        Returns:
            The string representation of this view.
        """
        items = []
        if self._tail != self._head or self._is_full:
            tail = self._tail
            for _ in range(len(self)):
                items.append(self._buffer[tail])
                tail = self._adjust(tail, offset=1)
        return f"{self.__class__.__name__}({items})"

    def _debug_repr(self) -> str:
        """Get the debug string representation of this view.

        Returns:
            The debug string representation of this view.
        """
        return (
            f"{self.__class__.__name__}(buffer={self._buffer!r}, "
            "full={self._is_full!r}, tail={self._tail!r}, head={self._head!r})"
        )

    def __eq__(self, __o: object) -> bool:
        """Compare this view to another object.

        Returns:
            `True` if the other object is also a `Sequence` and all the
                elements compare equal, `False` otherwise.
        """
        if isinstance(__o, Sequence):
            len_self = len(self)
            if len_self != len(__o):
                return False
            for i in range(len_self):
                if self[i] != __o[i]:
                    return False
            return True
        return super().__eq__(__o)


class Buffer(_BufferView[T]):
    """A push-only, fixed-size ring buffer.

    Elements can be `push`ed to the buffer, when the buffer gets full, newer
    elements will replace older elements. The buffer can be `clear`ed.
    """

    def __init__(self, /, capacity: int, initial_values: Sequence[T] = ()) -> None:
        """Create an instance.

        Args:
            capacity: The capacity of the buffer.
            initial_values: The initial values to fill the buffer with. If it
                has more items than the capacity, then only the last items will
                be added.
        """
        if capacity <= 0:
            raise ValueError("The buffer capacity must be larger than 0")
        if len(initial_values) > capacity:
            initial_values = initial_values[-capacity:]
        buffer: list[_Uninitialized | T] = list(initial_values)
        is_full = False
        if len(initial_values) < capacity:
            buffer += [_UNINITIALIZED] * (capacity - len(initial_values))
            head = len(initial_values)
        else:
            is_full = True
            head = 0
        super().__init__(buffer=buffer, full=is_full, tail=0, head=head)

    def clear(self) -> None:
        """Clear the buffer."""
        self._head = 0
        self._tail = 0
        self._is_full = False

    def push(self, element: T) -> None:
        """Push a new element to the buffer.

        If the buffer is full, the oldest element will be dropped.

        Args:
            element: Element to push.
        """
        self._buffer[self._head] = element
        self._head = self._adjust(self._head, offset=1)
        if self._is_full:
            self._tail = self._adjust(self._tail, offset=1)
        if self._head == self._tail and not self._is_full:
            self._is_full = True
