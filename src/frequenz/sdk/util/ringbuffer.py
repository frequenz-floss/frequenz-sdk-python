# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Ringbuffer implementation with focus on time & memory efficiency."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Generic, Sequence, TypeVar, Union

import numpy as np

T = TypeVar("T")

Container = Union[list, np.ndarray]


class OrderedRingBuffer(Generic[T]):
    """Time aware ringbuffer that keeps its entries sorted by time."""

    def __init__(
        self,
        buffer: Container,
        resampling_period_s: float,
        time_index_alignment: datetime = datetime(1, 1, 1),
    ) -> None:
        """Initialize the time aware ringbuffer.

        Args:
            buffer: Instance of a buffer container to use internally.
            resampling_period_s: Resampling period in seconds.
            time_index_alignment: Arbitary point in time used to align
                timestamped data with the index position in the buffer.
                Used to make the data stored in the buffer align with the
                beginning and end of the buffer borders.

                For example, if the `time_index_alignment` is set to
                "0001-01-01 12:00:00", and the `resampling_period_s` is set to
                3600 (1 hour) and the length of the buffer is 24, then the data
                stored in the buffer could correspond to the time range from
                "2022-01-01 12:00:00" to "2022-01-02 12:00:00" (date choosen
                arbitrarily here).
        """
        self._buffer = buffer
        self._resampling_period_s = resampling_period_s
        self._time_index_alignment = time_index_alignment

        self._missing_windows = []
        self._datetime_newest = datetime.min
        self._datetime_oldest = datetime.max

    @property
    def missing_windows(self) -> list:
        """Get the list of missing windows.

        The list of missing of windows consists of dicts with the members
        "start" and "end" which are describing the time ranges of gaps, or in
        other words, where we received data with `missing = True`.

        Returns:
            list of missing windows.
        """
        return self._missing_windows

    @property
    def maxlen(self) -> int:
        """Get the max length.

        Returns:
            the max amount of items this container can hold.
        """
        return len(self._buffer)

    def update(self, timestamp: datetime, value: T, missing: bool = False) -> bool:
        """Update the buffer with a new value for the given timestamp.

        Args:
            timestamp: Timestamp of the new value
            value: value to add
            missing: if true, the given timestamp will be recorded as missing.
                The value will still be written.

                Note: When relying on your own values to mark missing data (e.g.
                `math.nan`), be advised that when `update()` is called with
                timestamps newer than the current time + `resampling_period_s`
                (as is the case for loading historical data after an app
                restart), a gap of missing data exists.
                This gap does not contain NaN values but simply the old invalid values.
                The `missing_windows` will reflect this.

        Returns:
            True if the entry was added. False if it was discarded for being too old.
        """
        # Don't add outdated entries
        if timestamp < self._datetime_oldest and self._datetime_oldest != datetime.max:
            return False

        # Update timestamps
        prev_newest = self._datetime_newest
        self._datetime_newest = max(self._datetime_newest, timestamp)
        self._datetime_oldest = self._datetime_newest - timedelta(
            seconds=(len(self._buffer) - 1) * self._resampling_period_s
        )

        assert self.datetime_to_index(self._datetime_newest) != self.datetime_to_index(
            self._datetime_oldest
        )

        # Update data
        insert_index = self.datetime_to_index(timestamp)
        self._buffer[insert_index] = value

        prev_newest_index = self.datetime_to_index(
            prev_newest, allow_outside_range=True
        )

        # Update missing windows
        if insert_index - prev_newest_index >= self.maxlen:
            self._missing_windows.append(
                {"start": self._datetime_oldest, "end": self._datetime_newest}
            )
        elif prev_newest_index + 1 < insert_index:
            self._missing_windows.append(
                {"start": prev_newest, "end": self._datetime_newest}
            )

        # Update list of missing windows
        #
        # We always append to the last pending window.
        # A window is pending when end is None
        if missing:
            # Create new if no pending window
            if len(self._missing_windows) == 0 or "end" in self._missing_windows[-1]:
                self._missing_windows.append({"start": timestamp, "end": None})
        elif len(self._missing_windows) > 0:
            # Finalize a pending window
            if "end" not in self._missing_windows[-1]:
                self._missing_windows[-1]["end"] = timestamp

        # Delete out-of-date windows
        if len(self._missing_windows) > 0 and "end" in self._missing_windows[0]:
            if self._missing_windows[0]["end"] <= self._datetime_oldest:
                self._missing_windows = self._missing_windows[1:]

        return True

    def datetime_to_index(
        self, timestamp: datetime, allow_outside_range: bool = False
    ) -> int:
        """Convert the given timestamp to an index.

        Args:
            timestamp: Timestamp to convert.
            allow_outside_range: If True, don't throw an exception when the
                timestamp is outside our bounds

        Raises:
            IndexError: when requesting a timestamp outside the range this container holds

        Returns:
            index where the value for the given timestamp can be found.
        """
        if not allow_outside_range and (
            self._datetime_newest + timedelta(seconds=1 * self._resampling_period_s)
            < timestamp
            or timestamp < self._datetime_oldest
        ):
            raise IndexError(
                f"Requested timestamp {timestamp} is is "
                f"outside the range [{self._datetime_oldest} - {self._datetime_newest}]"
            )

        return self._wrap(
            int(
                abs((self._time_index_alignment - timestamp).total_seconds())
                / self._resampling_period_s
            )
        )

    def window(
        self, start: datetime, end: datetime, force_copy: bool = False
    ) -> Container:
        """Request a view on the data between start timestamp and end timestamp.

        If the data is not used immediately it could be overwritten.
        Always request a copy if you keep the data around for longer.

        Will return a copy in the following cases:
        * The requested time period is crossing the start/end of the buffer.
        * The requested time period contains missing entries.
        * The force_copy parameter was set to True (default False).

        This means, if the caller needs to modify the data to account for
        missing entries, they can safely do so.

        Args:
            start: start time of the window.
            end: end time of the window.
            force_copy: optional, default False. If True, will always create a
                copy of the data.

        Returns:
            the requested window
        """
        assert start < end

        start_index = self.datetime_to_index(start)
        end_index = self.datetime_to_index(end)

        # Requested window wraps around the ends
        if start_index >= end_index:
            window = self._buffer[start_index:]

            if end_index > 0:
                if isinstance(self._buffer, list):
                    window += self._buffer[0:end_index]
                else:
                    window = np.concatenate((window, self._buffer[0:end_index]))
            return window

        def in_window(window):
            window_end = window["end"] if "end" in window else self._datetime_newest
            if window["start"] <= start < window_end:
                return True
            if window["start"] <= end < window_end:
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
        Create a feature request if you require this function.

        Args:
            index_or_slice: Index or slice specification of the requested data.

        Returns:
            The requested value or slice.
        """
        return self._buffer.__getitem__(index_or_slice)

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
