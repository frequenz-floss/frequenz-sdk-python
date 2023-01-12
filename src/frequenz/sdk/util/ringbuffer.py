# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Ringbuffer implementation with focus on time & memory efficiency."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from typing import Generic, Sequence, TypeVar

import numpy as np

T = TypeVar("T")

Container = TypeVar("Container", list, np.ndarray)


class OrderedRingBuffer(Generic[T, Container]):
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
        self._time_range = timedelta(
            seconds=(len(self._buffer) - 1) * self._resampling_period_s
        )

    def normalize_timestamp(self, timestamp: datetime) -> datetime:
        """Normalize the given timestamp to fall exactly on the resampling period.

        Args:
            timestamp: The timestamp to normalize.

        Returns:
            The normalized timestamp.
        """
        normalized_timestamp = self._time_index_alignment + timedelta(
            seconds=round(
                (timestamp - self._time_index_alignment).total_seconds()
                / self._resampling_period_s
            )
            * self._resampling_period_s
        )
        return normalized_timestamp

    @property
    def missing_windows(self) -> list:
        """Get the list of missing windows.

        The list of missing of windows consists of dicts with the members
        "start" and "end" which are describing the time ranges of gaps or in
        other words, where we received data with `missing = True`.

        "end" is excluding, meaning when "start" is 14:00 and "end" is 14:10
        with a resolution of 300 (5 minutes) we are missing the values for 14:00
        and 14:05.

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
            timestamp: Timestamp of the new value.
            value: value to add.
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

        print(f"Normalizing {timestamp} to {self.normalize_timestamp(timestamp)}")

        # adjust timestamp to be exactly on the sample period time point
        timestamp = self.normalize_timestamp(timestamp)

        # Update timestamps
        prev_newest = self._datetime_newest
        self._datetime_newest = max(self._datetime_newest, timestamp)
        self._datetime_oldest = self._datetime_newest - self._time_range

        # Update data
        self._buffer[self.datetime_to_index(timestamp)] = value

        self._update_missing_windows(timestamp, prev_newest, missing)

        return True

    def _update_missing_windows_add_gap(
        self, timestamp: datetime, newest: datetime
    ) -> None:
        # Does one of the following:
        # * Merge existing windows
        # * enlarge existing window
        # * create new window

        one_step = timedelta(seconds=self._resampling_period_s)

        def adjacent(window: tuple[int, dict]) -> bool:
            return timestamp in (window[1]["start"] - one_step,
                                 window[1]['end'])

        adjacent_windows = list(filter(adjacent, enumerate(self._missing_windows)))
        sorted(adjacent_windows, key=lambda x: x[1]["start"].timestamp())

        # Can't have more than two adjacent windows
        assert len(adjacent_windows) <= 2

        # Adjacent to 2 windows means we merge them
        if len(adjacent_windows) == 2:
            assert (
                adjacent_windows[0][1]["end"] + one_step
                == adjacent_windows[1][1]["start"]
            )
            adjacent_windows[0][1]["end"] = adjacent_windows[1][1]["end"]
            del self._missing_windows[adjacent_windows[1][0]]
        # Otherwise enlarge the window
        elif len(adjacent_windows) == 1:
            if adjacent_windows[0][1]["start"] == timestamp + one_step:
                adjacent_windows[0][1]["start"] = timestamp
            elif adjacent_windows[0][1]["end"] == timestamp:
                adjacent_windows[0][1]["end"] = timestamp + one_step
        # No adjacent windows, add new entry
        else:
            # Create new if no pending window
            if (
                len(self._missing_windows) == 0
                or self._missing_windows[-1]["end"] is not None
            ):
                self._missing_windows.append(
                    {"start": timestamp, "end": timestamp + one_step}
                )

    def _update_missing_windows_remove_gap(
        self, timestamp: datetime, newest: datetime
    ) -> None:
        # Replace all missing windows with one if we went far into then future
        if self._datetime_newest - newest >= self._time_range:
            self._missing_windows = [
                {"start": self._datetime_oldest, "end": self._datetime_newest}
            ]
            return

        missing_window_index, missing_window = next(
            filter(
                lambda window: self._in_window(window[1], timestamp),
                enumerate(self._missing_windows),
            ),
            (0, None),
        )

        if missing_window is None:
            return

        one_step = timedelta(seconds=self._resampling_period_s)

        if missing_window["start"] == timestamp:
            # Is the whole window consisting only of the timestamp?
            if missing_window["end"] == timestamp + one_step:
                del self._missing_windows[missing_window_index]
            # Otherwise, make the window smaller
            else:
                missing_window["start"] = timestamp + one_step
        # Is the timestamp at the end? Shrinken by one then
        elif (
            missing_window["end"] is not None
            and missing_window["end"] - one_step == timestamp
        ):
            missing_window["end"] = timestamp
        # Otherwise it must be in the middle and we need to create a new
        # missing window
        else:
            new_window = deepcopy(missing_window)
            missing_window["end"] = timestamp
            new_window["start"] = timestamp + one_step
            assert not self.is_missing(timestamp)
            self._missing_windows.append(new_window)

    def _update_missing_windows(
        self, timestamp: datetime, newest: datetime, missing: bool
    ) -> None:
        currently_missing = self.is_missing(timestamp)

        # New missing entry that is not already in a window?
        if missing:
            if not currently_missing:
                self._update_missing_windows_add_gap(timestamp, newest)
        elif len(self._missing_windows) > 0:
            if currently_missing:
                self._update_missing_windows_remove_gap(timestamp, newest)

        # Delete out-to-date windows
        if len(self._missing_windows) > 0:
            self._missing_windows[:] = filter(
                lambda x: x["start"] >= self._datetime_oldest, self._missing_windows
            )

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

        timestamp = self.normalize_timestamp(timestamp)

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
            round(
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
            return self._in_window(window, start) or self._in_window(window, end)

        # Return a copy if there are none-values in the data
        if force_copy or any(map(in_window, self._missing_windows)):
            return deepcopy(self._buffer[start_index:end_index])

        return self._buffer[start_index:end_index]

    def is_missing(self, timestamp: datetime) -> bool:
        """Check if the given timestamp falls within a missing window.

        Args:
            timestamp: The timestamp to check for missing data.

        Returns:
            bool: True if the given timestamp falls within a missing window, False otherwise.
        """
        return any(
            map(
                lambda window: self._in_window(window, timestamp), self._missing_windows
            )
        )

    def _in_window(self, window: dict, timestamp: datetime):
        window_end = (
            window["end"] if window["end"] is not None else self._datetime_newest
        )
        if window["start"] <= timestamp < window_end:
            return True

        return False

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
