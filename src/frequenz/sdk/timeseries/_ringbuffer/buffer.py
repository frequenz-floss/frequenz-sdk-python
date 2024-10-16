# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Ringbuffer implementation with focus on time & memory efficiency."""


from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, SupportsIndex, TypeVar, overload

import numpy as np
import numpy.typing as npt

from .._base_types import UNIX_EPOCH, QuantityT, Sample

FloatArray = TypeVar("FloatArray", list[float], npt.NDArray[np.float64])
"""Type variable of the buffer container."""


@dataclass
class Gap:
    """A gap defines the range for which we haven't received values yet."""

    start: datetime
    """Start timestamp of the range, inclusive."""
    end: datetime
    """End timestamp of the range, exclusive."""

    def contains(self, timestamp: datetime) -> bool:
        """Check if a given timestamp is inside this gap.

        Args:
            timestamp: Timestamp to check.

        Returns:
            True if the timestamp is in the gap.
        """
        if self.start <= timestamp < self.end:
            return True

        return False


class OrderedRingBuffer(Generic[FloatArray]):
    """Time aware ringbuffer that keeps its entries sorted by time."""

    _TIMESTAMP_MIN = datetime.min.replace(tzinfo=timezone.utc)
    """The minimum representable timestamp."""

    _TIMESTAMP_MAX = datetime.max.replace(tzinfo=timezone.utc)
    """The maximum representable timestamp."""

    def __init__(
        self,
        buffer: FloatArray,
        sampling_period: timedelta,
        align_to: datetime = UNIX_EPOCH,
    ) -> None:
        """Initialize the time aware ringbuffer.

        Args:
            buffer: Instance of a buffer container to use internally.
            sampling_period: Timedelta of the desired sampling period.
            align_to: Arbitrary point in time used to align
                timestamped data with the index position in the buffer.
                Used to make the data stored in the buffer align with the
                beginning and end of the buffer borders.

                For example, if the `align_to` is set to
                "0001-01-01 12:00:00", and the `sampling_period` is set to
                1 hour and the length of the buffer is 24, then the data
                stored in the buffer could correspond to the time range from
                "2022-01-01 12:00:00" to "2022-01-02 12:00:00" (date chosen
                arbitrarily here).
        """
        assert len(buffer) > 0, "The buffer capacity must be higher than zero"

        self._buffer: FloatArray = buffer
        self._sampling_period: timedelta = sampling_period
        self._time_index_alignment: datetime = align_to

        self._gaps: list[Gap] = []
        self._timestamp_newest: datetime = self._TIMESTAMP_MIN
        self._timestamp_oldest: datetime = self._TIMESTAMP_MAX
        self._full_time_range: timedelta = len(self._buffer) * self._sampling_period

    @property
    def sampling_period(self) -> timedelta:
        """Return the sampling period of the ring buffer.

        Returns:
            Sampling period of the ring buffer.
        """
        return self._sampling_period

    @property
    def gaps(self) -> list[Gap]:
        """Get the list of ranges for which no values were provided.

        See definition of dataclass @Gaps for more info.

        Returns:
            List of gaps.
        """
        return self._gaps

    def has_value(self, sample: Sample[QuantityT]) -> bool:
        """Check if a sample has a value and it's not NaN.

        Args:
            sample: sample to check.

        Returns:
            True if the sample has a value and it's not NaN.
        """
        return not (sample.value is None or sample.value.isnan())

    @property
    def maxlen(self) -> int:
        """Get the max length.

        Returns:
            The max amount of items this container can hold.
        """
        return len(self._buffer)

    def update(self, sample: Sample[QuantityT]) -> None:
        """Update the buffer with a new value for the given timestamp.

        Missing values are written as NaN. Be advised that when
        `update()` is called with samples newer than the current time
        + `sampling_period` (as is the case for loading historical data
        after an app restart), a gap of missing data exists. This gap
        does not contain NaN values but simply the old invalid values.
        The list of gaps returned by `gaps()` will reflect this and
        should be used as the only source of truth for unwritten data.

        Args:
            sample: Sample to add to the ringbuffer

        Raises:
            IndexError: When the timestamp to be added is too old.
        """
        # adjust timestamp to be exactly on the sample period time point
        timestamp = self.normalize_timestamp(sample.timestamp)

        # Don't add outdated entries
        if (
            timestamp < self._timestamp_oldest
            and self._timestamp_oldest != self._TIMESTAMP_MAX
        ):
            raise IndexError(
                f"Timestamp {timestamp} too old (cut-off is at {self._timestamp_oldest})."
            )

        # Update timestamps
        prev_newest = self._timestamp_newest
        self._timestamp_newest = max(self._timestamp_newest, timestamp)
        self._timestamp_oldest = self._timestamp_newest - (
            self._full_time_range - self._sampling_period
        )

        # Update data
        if self.has_value(sample):
            assert sample.value is not None
            value = sample.value.base_value
        else:
            value = np.nan
        self._buffer[self.to_internal_index(timestamp)] = value

        self._update_gaps(timestamp, prev_newest, not self.has_value(sample))

    @property
    def time_bound_oldest(self) -> datetime:
        """
        Return the time bounds of the ring buffer.

        Returns:
            The timestamp of the oldest sample of the ring buffer.
        """
        return self._timestamp_oldest

    @property
    def time_bound_newest(self) -> datetime:
        """
        Return the time bounds of the ring buffer.

        Returns:
            The timestamp of the newest sample of the ring buffer
            or None if the buffer is empty.
        """
        return self._timestamp_newest

    @property
    def oldest_timestamp(self) -> datetime | None:
        """Return the oldest timestamp in the buffer.

        Returns:
            The oldest timestamp in the buffer
            or None if the buffer is empty.
        """
        if self.count_valid() == 0:
            return None

        if self.is_missing(self.time_bound_oldest):
            return min(g.end for g in self.gaps)

        return self.time_bound_oldest

    @property
    def newest_timestamp(self) -> datetime | None:
        """Return the newest timestamp in the buffer.

        Returns:
            The newest timestamp in the buffer.
        """
        if self.count_valid() == 0:
            return None

        return self.time_bound_newest

    def to_internal_index(
        self, timestamp: datetime, allow_outside_range: bool = False
    ) -> int:
        """Convert the given timestamp to the position in the buffer.

        !!! Note: This method is meant for advanced use cases and should not generally be used.

        Args:
            timestamp: Timestamp to convert.
            allow_outside_range: If True, don't throw an exception when the
                timestamp is outside our bounds

        Raises:
            IndexError: When requesting a timestamp outside the range this container holds.

        Returns:
            Index where the value for the given timestamp can be found.
        """
        timestamp = self.normalize_timestamp(timestamp)

        if not allow_outside_range and (
            self._timestamp_newest + self._sampling_period < timestamp
            or timestamp < self._timestamp_oldest
        ):
            raise IndexError(
                f"Requested timestamp {timestamp} is "
                f"outside the range [{self._timestamp_oldest} - {self._timestamp_newest}]"
            )

        return self.wrap(
            round(
                (timestamp - self._time_index_alignment).total_seconds()
                / self._sampling_period.total_seconds()
            )
        )

    def get_timestamp(self, index: int) -> datetime | None:
        """Convert the given index to the underlying timestamp.

        Index 0 corresponds to the oldest timestamp in the buffer.
        If negative indices are used, the newest timestamp is used as reference.

        !!!warning

            The resulting timestamp can be outside the range of the buffer.

        Args:
            index: Index to convert.

        Returns:
            Timestamp where the value for the given index can be found.
                Or None if the buffer is empty.
        """
        if self.oldest_timestamp is None:
            return None
        assert self.newest_timestamp is not None
        ref_ts = (
            self.oldest_timestamp
            if index >= 0
            else self.newest_timestamp + self._sampling_period
        )
        return ref_ts + index * self._sampling_period

    def _to_covered_indices(
        self, start: int | None, end: int | None = None
    ) -> tuple[int, int]:
        """Project the given indices via slice onto the covered range.

        Args:
            start: Start index.
            end: End index. Optional, defaults to None.

        Returns:
            tuple of start and end indices on the range currently covered by the buffer.
        """
        return slice(start, end).indices(self.count_covered())[:2]

    def window(
        self,
        start: datetime | int | None,
        end: datetime | int | None,
        *,
        force_copy: bool = True,
        fill_value: float | None = np.nan,
    ) -> FloatArray:
        """Request a copy or view on the data between start timestamp and end timestamp.

        Always request a copy if you keep the data around for longer.
        Otherwise, if the data is not used immediately it could be overwritten.

        Will return a copy in the following cases:
        * The requested time period is crossing the start/end of the buffer.
        * The force_copy parameter was set to True (default True).

        The first case can be avoided by using the appropriate
        `align_to` value in the constructor so that the data lines up
        with the start/end of the buffer.

        This means, if the caller needs to modify the data to account for
        missing entries, they can safely do so.

        Args:
            start: start timestamp of the window.
            end: end timestamp of the window.
            force_copy: optional, default True. If True, will always create a
                copy of the data.
            fill_value: If not None, will use this value to fill missing values.
                If missing values should be filled, force_copy must be True.
                Defaults to NaN to avoid returning outdated data unexpectedly.

        Raises:
            IndexError: When start and end are not both datetime or index.
            ValueError: When fill_value is not None and force_copy is False.

        Returns:
            The requested window
        """
        # We don't want to modify the original buffer
        if fill_value is not None and not force_copy:
            raise ValueError("fill_value only supported for force_copy=True")

        if self.count_covered() == 0:
            return np.array([]) if isinstance(self._buffer, np.ndarray) else []

        # If both are indices or None convert to datetime
        if not isinstance(start, datetime) and not isinstance(end, datetime):
            start, end = self._to_covered_indices(start, end)
            start = self.get_timestamp(start)
            end = self.get_timestamp(end)

        # Here we should have both as datetime
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            raise IndexError(
                f"start ({start}) and end ({end}) must both be either datetime or index."
            )

        # Ensure that the window is within the bounds of the buffer
        assert self.oldest_timestamp is not None and self.newest_timestamp is not None
        start = max(start, self.oldest_timestamp)
        end = min(end, self.newest_timestamp + self._sampling_period)

        if start >= end:
            return np.array([]) if isinstance(self._buffer, np.ndarray) else []

        start_pos = self.to_internal_index(start)
        end_pos = self.to_internal_index(end)

        window = self._wrapped_buffer_window(
            self._buffer, start_pos, end_pos, force_copy
        )

        if fill_value is not None:
            window = self._fill_gaps(window, fill_value, start, self.gaps)
        return window

    def _fill_gaps(
        self,
        data: FloatArray,
        fill_value: float,
        oldest_timestamp: datetime,
        gaps: list[Gap],
    ) -> FloatArray:
        """Fill the gaps in the data with the given fill_value.

        Args:
            data: The data to fill.
            fill_value: The value to fill the gaps with.
            oldest_timestamp: The oldest timestamp in the data.
            gaps: List of gaps to fill.

        Returns:
            The filled data.
        """
        assert isinstance(
            data, (np.ndarray, list)
        ), f"Unsupported data type {type(data)}"
        for gap in gaps:
            start_index = (gap.start - oldest_timestamp) // self._sampling_period
            end_index = (gap.end - oldest_timestamp) // self._sampling_period
            start_index = max(start_index, 0)
            end_index = min(end_index, len(data))
            if start_index < end_index:
                if isinstance(data, np.ndarray):
                    data[start_index:end_index] = fill_value
                elif isinstance(data, list):
                    data[start_index:end_index] = [fill_value] * (
                        end_index - start_index
                    )
        return data

    @staticmethod
    def _wrapped_buffer_window(
        buffer: FloatArray,
        start_pos: int,
        end_pos: int,
        force_copy: bool = True,
    ) -> FloatArray:
        """Get a wrapped window from the given buffer.

        If start_pos == end_pos, the full wrapped buffer is returned starting at start_pos.

        Copies can only be avoided for numpy arrays and when the window is not wrapped.
        Lists of floats are always copies.

        Args:
            buffer: The buffer to get the window from.
            start_pos: The start position of the window in the buffer.
            end_pos: The end position of the window in the buffer (exclusive).
            force_copy: If True, will always create a copy of the data.

        Returns:
            The requested window.
        """
        # Requested window wraps around the ends
        if start_pos >= end_pos:
            if isinstance(buffer, list):
                return buffer[start_pos:] + buffer[0:end_pos]
            assert isinstance(
                buffer, np.ndarray
            ), f"Unsupported buffer type: {type(buffer)}"
            if end_pos > 0:
                return np.concatenate((buffer[start_pos:], buffer[0:end_pos]))
            arr = buffer[start_pos:]
        else:
            arr = buffer[start_pos:end_pos]

        if force_copy:
            return deepcopy(arr)
        return arr

    def is_missing(self, timestamp: datetime) -> bool:
        """Check if the given timestamp falls within a gap.

        Args:
            timestamp: The timestamp to check for missing data.

        Returns:
            True if the given timestamp falls within a gap, False otherwise.
        """
        return any(map(lambda gap: gap.contains(timestamp), self._gaps))

    def _update_gaps(
        self, timestamp: datetime, newest: datetime, record_as_missing: bool
    ) -> None:
        """Update gap list with new timestamp.

        Args:
            timestamp: Timestamp of the new value.
            newest: Timestamp of the newest value before the current update.
            record_as_missing: if `True`, the given timestamp will be recorded as missing.
        """
        found_in_gaps = self.is_missing(timestamp)

        if not record_as_missing:
            # Replace all gaps with one if we went far into then future
            if self._timestamp_newest - newest >= self._full_time_range:
                self._gaps = [
                    Gap(start=self._timestamp_oldest, end=self._timestamp_newest)
                ]
                return

            # Check if we created a gap with the addition of the new value
            if not found_in_gaps and timestamp > newest + self._sampling_period:
                self._gaps.append(
                    Gap(start=newest + self._sampling_period, end=timestamp)
                )

        # New missing entry that is not already in a gap?
        if record_as_missing:
            if not found_in_gaps:
                # If there are no gaps and the new value is not subsequent to the
                # newest value, we need to start the new gap after the newest value
                start_gap = min(newest + self._sampling_period, timestamp)
                self._gaps.append(
                    Gap(start=start_gap, end=timestamp + self._sampling_period)
                )
        elif len(self._gaps) > 0:
            if found_in_gaps:
                self._remove_gap(timestamp)

        self._cleanup_gaps()

    def _cleanup_gaps(self) -> None:
        """Clean up the list of gaps.

        * Merge existing gaps
        * remove overlaps
        * delete outdated gaps
        """
        self._gaps = sorted(self._gaps, key=lambda x: x.start.timestamp())

        i = 0
        while i < len(self._gaps):
            w_1 = self._gaps[i]
            if i < len(self._gaps) - 1:
                w_2 = self._gaps[i + 1]
            else:
                w_2 = None

            # Delete out-of-date gaps
            if w_1.end <= self._timestamp_oldest:
                del self._gaps[i]
            # Update start of gap if it's rolled out of the buffer
            elif w_1.start < self._timestamp_oldest:
                self._gaps[i].start = self._timestamp_oldest
            # If w2 is a subset of w1 we can delete it
            elif w_2 and w_1.start <= w_2.start and w_1.end >= w_2.end:
                del self._gaps[i + 1]
            # If the gaps are direct neighbors, merge them
            elif w_2 and w_1.end >= w_2.start:
                w_1.end = w_2.end
                del self._gaps[i + 1]
            else:
                i += 1

    def _remove_gap(self, timestamp: datetime) -> None:
        """Update the list of gaps to not contain the given timestamp.

        Args:
            timestamp: Timestamp that is no longer missing.
        """
        gap_index, gap = next(
            filter(
                lambda gap: gap[1].contains(timestamp),
                enumerate(self._gaps),
            ),
            (0, None),
        )

        if gap is None:
            return

        if gap.start == timestamp:
            # Is the whole gap consisting only of the timestamp?
            if gap.end == timestamp + self._sampling_period:
                del self._gaps[gap_index]
            # Otherwise, make the gap smaller
            else:
                gap.start = timestamp + self._sampling_period
        # Is the timestamp at the end? Shrink by one then
        elif gap.end - self._sampling_period == timestamp:
            gap.end = timestamp
        # Otherwise it must be in the middle and we need to create a new
        # gap
        else:
            new_gap = deepcopy(gap)
            gap.end = timestamp
            new_gap.start = timestamp + self._sampling_period
            self._gaps.append(new_gap)

    def normalize_timestamp(self, timestamp: datetime) -> datetime:
        """Normalize the given timestamp to fall exactly on the resampling period.

        Args:
            timestamp: The timestamp to normalize.

        Returns:
            The normalized timestamp.
        """
        num_samples, remainder = divmod(
            (timestamp - self._time_index_alignment), self._sampling_period
        )

        # Round towards the closer number and towards the even one in case of
        # equal distance
        if remainder != timedelta(0) and (
            self._sampling_period / 2 == remainder
            and num_samples % 2 != 0
            or self._sampling_period / 2 < remainder
        ):
            num_samples += 1

        normalized_timestamp = (
            self._time_index_alignment + num_samples * self._sampling_period
        )

        return normalized_timestamp

    def wrap(self, index: int) -> int:
        """Normalize the given index to fit in the buffer by wrapping it around.

        Args:
            index: index to normalize.

        Returns:
            An index that will be within maxlen.
        """
        return index % self.maxlen

    @overload
    def __getitem__(self, index_or_slice: SupportsIndex) -> float:
        """Get item at requested position.

        No wrapping of the index will be done.
        Create a feature request if you require this function.

        Args:
            index_or_slice: Index of the requested data.

        Returns:
            The requested value.
        """

    @overload
    def __getitem__(self, index_or_slice: slice) -> FloatArray:
        """Get the data described by the given slice.

        No wrapping of the index will be done.
        Create a feature request if you require this function.

        Args:
            index_or_slice: Slice specification of where the requested data is.

        Returns:
            The requested slice.
        """

    def __getitem__(self, index_or_slice: SupportsIndex | slice) -> float | FloatArray:
        """Get item or slice at requested position.

        No wrapping of the index will be done.
        Create a feature request if you require this function.

        Args:
            index_or_slice: Index or slice specification of the requested data.

        Returns:
            The requested value or slice.
        """
        return self._buffer.__getitem__(index_or_slice)

    def _covered_time_range(self) -> timedelta:
        """Return the time range that is covered by the oldest and newest valid samples.

        Returns:
            The time range between the oldest and newest valid samples or 0 if
                there are is no time range covered.
        """
        if not self.oldest_timestamp:
            return timedelta(0)

        assert (
            self.newest_timestamp is not None
        ), "Newest timestamp cannot be None here."
        return self.newest_timestamp - self.oldest_timestamp + self._sampling_period

    def count_covered(self) -> int:
        """Count the number of samples that are covered by the oldest and newest valid samples.

        Returns:
            The count of samples between the oldest and newest (inclusive) valid samples
                or 0 if there are is no time range covered.
        """
        return int(
            self._covered_time_range().total_seconds()
            // self._sampling_period.total_seconds()
        )

    def count_valid(self) -> int:
        """Count the number of valid items that this buffer currently holds.

        Returns:
            The number of valid items in this buffer.
        """
        if self._timestamp_newest == self._TIMESTAMP_MIN:
            return 0

        # Sum of all elements in the gap ranges
        sum_missing_entries = max(
            0,
            sum(
                (
                    gap.end
                    # Don't look further back than oldest timestamp
                    - max(gap.start, self._timestamp_oldest)
                )
                // self._sampling_period
                for gap in self._gaps
            ),
        )

        start_pos = self.to_internal_index(self._timestamp_oldest)
        end_pos = self.to_internal_index(self._timestamp_newest)

        if end_pos < start_pos:
            return len(self._buffer) - start_pos + end_pos + 1 - sum_missing_entries

        return end_pos + 1 - start_pos - sum_missing_entries
