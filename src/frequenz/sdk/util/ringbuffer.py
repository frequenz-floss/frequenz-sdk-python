# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Ringbuffer implementation with focus on time & memory efficiency."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Generic, Sequence, TypeVar, Union

import numpy as np

T = TypeVar("T")

Container = Union[list, np.ndarray]


class RingBuffer(Generic[T]):
    """A ring buffer with a fixed size.

    Should work with most backends, tested with list and np.ndarrays.
    """

    def __init__(self, container: Container) -> None:
        """Initialize the ring buffer with the given container.

        Args:
            container: Container to store the data in.
        """
        self._container = container
        self._write_index = 0
        self._read_index = 0
        self._len = 0

    def __len__(self) -> int:
        """Get current amount of elements.

        Returns:
            The amount of items that this container currently holds.
        """
        return self._len

    @property
    def maxlen(self) -> int:
        """Get the max length.

        Returns:
            The max amount of items this container can hold.
        """
        return len(self._container)

    def push(self, value: T) -> int:
        """Push a new value into the ring buffer.

        Args:
            value: Value to push into the ring buffer.

        Returns:
            The index in the ringbuffer.
        """
        if self._len == len(self._container):
            # Move read position one forward, dropping the oldest written value
            self._read_index = self._wrap(self._read_index + 1)
        else:
            self._len += 1

        self._container[self._write_index] = value
        value_index = self._write_index
        self._write_index = self._wrap(self._write_index + 1)

        return value_index

    def pop(self) -> T:
        """Remove the oldest value from the ring buffer and return it.

        Raises:
            IndexError: when no elements exist to pop.

        Returns:
            Oldest value found in the ring buffer.
        """
        if self._len == 0:
            raise IndexError()

        val = self._container[self._read_index]
        self._read_index = (self._read_index + 1) % len(self._container)
        self._len -= 1

        return val

    @property
    def is_full(self) -> bool:
        """Check if the container is full.

        Returns:
            True when the container is full.
        """
        return len(self) == len(self._container)

    def __setitem__(self, index: int | slice, value: T | Sequence[T]) -> None:
        """Write the given value to the requested position.

        Args:
            index: Position to write the value to.
            value: Value to write.
        """
        if isinstance(index, int):
            self._container[self._wrap(index + self._read_index)] = value
        else:
            wstart = self._wrap(index.start)
            wstop = self._wrap(index.stop)
            if wstop < wstart:
                self._container[wstart:] = value[:self.maxlen-wstart]
                self._container[:wstop] = value[self.maxlen-wstart:]
            else
                self._container[wstart:wstop] = value



    def __getitem__(self, index_or_slice: int | slice) -> T | Container:
        """Request a value or slice.

        Does not support wrap-around or copying of data.

        Args:
            index_or_slice: Index or slice specification of the requested data

        Returns:
            the value at the given index or value range at the given slice.
        """
        return self._container[index_or_slice]

    def _wrap(self, index: int) -> int:
        return index % len(self._container)


class OrderedRingBuffer(Generic[T]):
    """Time aware ringbuffer that keeps its entries sorted time."""

    def __init__(
        self,
        buffer: Any,
        sample_rate: int,
        window_border: datetime = datetime(1, 1, 1),
    ) -> None:
        """Initialize the time aware ringbuffer.

        Args:
            buffer: instance of a buffer container to use internally
            sample_rate: resolution of the incoming timestamps in
                seconds
            window_border: datetime depicting point in time to use as border
                beginning, useful to make data start at the beginning of the day or
                hour.
        """
        self._buffer = buffer
        self._sample_rate = sample_rate
        self._window_start = window_border

        self._missing_windows = []
        self._datetime_newest = datetime.min
        self._datetime_oldest = datetime.max

    @property
    def maxlen(self) -> int:
        """Get the max length.

        Returns:
            the max amount of items this container can hold.
        """
        return len(self._buffer)

    def update(self, timestamp: datetime, value: T, missing: bool = False) -> None:
        """Update the buffer with a new value for the given timestamp.

        Args:
            timestamp: Timestamp of the new value
            value: value to add
            missing: if true, the given timestamp will be recorded as missing.
                The value will still be written.

            Returns:
                Nothing.
        """
        # Update timestamps
        self._datetime_newest = max(self._datetime_newest, timestamp)
        self._datetime_oldest = min(self._datetime_oldest, timestamp)

        if self._datetime_oldest < self._datetime_newest - timedelta(
            seconds=len(self._buffer) * self._sample_rate
        ):
            self._datetime_oldest = self._datetime_newest - timedelta(
                len(self._buffer) * self._sample_rate
            )

        # Update data
        insert_index = self.datetime_to_index(timestamp)

        self._buffer[insert_index] = value

        # Update list of missing windows
        #
        # We always append to the last pending window.
        # A window is pending when end is None
        if missing:
            # Create new if no pending window
            if (
                len(self._missing_windows) == 0
                or self._missing_windows[-1].end is not None
            ):
                self._missing_windows.append({"start": timestamp, "end": None})
        elif len(self._missing_windows) > 0:
            # Finalize a pending window
            if self._missing_windows[-1].end is None:
                self._missing_windows[-1].end = timestamp

        # Delete out-to-date windows
        if len(self._missing_windows) > 0 and self._missing_windows[0].end is not None:
            if self._missing_windows[0].end <= self._datetime_oldest:
                self._missing_windows = self._missing_windows[1:]

    def datetime_to_index(self, timestamp: datetime) -> int:
        """Convert the given timestamp to an index.

        Throws an index error when the timestamp is not found within this
        buffer.

        Args:
            timestamp: Timestamp to convert.

        Raises:
            IndexError: when requesting a timestamp outside the range this container holds

        Returns:
            index where the value for the given timestamp can be found.
        """
        if self._datetime_newest < timestamp or timestamp < self._datetime_oldest:
            raise IndexError(
                f"Requested timestamp {timestamp} is is "
                f"outside the range [{self._datetime_oldest} - {self._datetime_newest}]"
            )

        return self._wrap(int(abs((self._window_start - timestamp).total_seconds())))

    def window(self, start: datetime, end: datetime, force_copy: bool = False) -> Container:
        """Request a view on the data between start timestamp and end timestamp.

        Will return a copy in the following cases:
        * The requested time period is crossing the start/end of the buffer
        * The requested time period contains missing entries.
        * The force_copy parameter was set to True (default False)

        This means, if the caller needs to modify the data to account for
        missing entries, they can safely do so.

        Args:
            start: start time of the window
            end: end time of the window

        Returns:
            the requested window
        """
        assert start < end

        start_index = self.datetime_to_index(start)
        end_index = self.datetime_to_index(end)

        # Requested window wraps around the ends
        if start_index > end_index:
            window = self._buffer[start_index:]

            if end_index > 0:
                if isinstance(self._buffer, list):
                    window += self._buffer[0:end_index]
                else:
                    window = np.concatenate((window, self._buffer[0:end_index]))
            return window

        def in_window(window):
            if window.start <= start < window.end:
                return True
            if window.start <= end < window.end:
                return True

            return False

        # Return a copy if there are none-values in the data
        if any(map(in_window, self._missing_windows)) or force_copy:
            return deepcopy(self._buffer[start_index:end_index])

        return self._buffer[start_index:end_index]

    def _wrap(self, index: int) -> int:
        """Normalize the given index to fit in the buffer by wrapping it around.

        Args:
            index: index to normalize.

        Returns:
            an index that will be within maxlen.
        """
        return index % self.maxlen

    def __getitem__(self, index_or_slice: int | slice) -> T | Sequence[T]:
        """Get item or slice at requested position.

        No wrapping of the index will be done.

        Args:
            index_or_slice: Index or slice specification of the requested data.

        Returns:
            The requested value or slice.
        """
        return self._buffer[index_or_slice]

    def __len__(self) -> int:
        """Return the amount of items that this container currently holds.

        Returns:
            The length.
        """
        if self._datetime_newest == datetime.min:
            return 0

        start_index = self.datetime_to_index(self._datetime_oldest)
        end_index = self.datetime_to_index(self._datetime_newest)

        if end_index < start_index:
            return len(self._buffer) - start_index + end_index
        return start_index - end_index
