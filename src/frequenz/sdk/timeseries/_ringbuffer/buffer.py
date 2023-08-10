# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Ringbuffer implementation with focus on time & memory efficiency."""

from __future__ import annotations

from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, List, SupportsFloat, SupportsIndex, TypeVar, overload

import numpy as np
import numpy.typing as npt

from .._base_types import UNIX_EPOCH, Sample
from .._quantities import QuantityT

FloatArray = TypeVar("FloatArray", List[float], npt.NDArray[np.float64])


@dataclass
class Gap:
    """A gap defines the range for which we haven't received values yet."""

    start: datetime
    """Start of the range, inclusive."""
    end: datetime
    """End of the range, exclusive."""

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

    _DATETIME_MIN = datetime.min.replace(tzinfo=timezone.utc)
    _DATETIME_MAX = datetime.max.replace(tzinfo=timezone.utc)

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
        self._datetime_newest: datetime = self._DATETIME_MIN
        self._datetime_oldest: datetime = self._DATETIME_MAX
        self._full_time_range: timedelta = len(self._buffer) * self._sampling_period

    @property
    def sampling_period(self) -> timedelta:
        """Return the sampling period of the ring buffer.

        Returns:
            Sampling period of the ring buffer.
        """
        return self._sampling_period

    @property
    def gaps(self) -> List[Gap]:
        """Get the list of ranges for which no values were provided.

        See definition of dataclass @Gaps for more info.

        Returns:
            List of gaps.
        """
        return self._gaps

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
            timestamp < self._datetime_oldest
            and self._datetime_oldest != self._DATETIME_MAX
        ):
            raise IndexError(
                f"Timestamp {timestamp} too old (cut-off is at {self._datetime_oldest})."
            )

        # Update timestamps
        prev_newest = self._datetime_newest
        self._datetime_newest = max(self._datetime_newest, timestamp)
        self._datetime_oldest = self._datetime_newest - (
            self._full_time_range - self._sampling_period
        )

        # Update data
        value: float = np.nan if sample.value is None else sample.value.base_value
        self._buffer[self.datetime_to_index(timestamp)] = value

        self._update_gaps(timestamp, prev_newest, sample.value is None)

    @property
    def time_bound_oldest(self) -> datetime:
        """
        Return the time bounds of the ring buffer.

        Returns:
            The timestamp of the oldest sample of the ring buffer.
        """
        return self._datetime_oldest

    @property
    def time_bound_newest(self) -> datetime:
        """
        Return the time bounds of the ring buffer.

        Returns:
            The timestamp of the newest sample of the ring buffer.
        """
        return self._datetime_newest

    def datetime_to_index(
        self, timestamp: datetime, allow_outside_range: bool = False
    ) -> int:
        """Convert the given timestamp to an index.

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
            self._datetime_newest + self._sampling_period < timestamp
            or timestamp < self._datetime_oldest
        ):
            raise IndexError(
                f"Requested timestamp {timestamp} is "
                f"outside the range [{self._datetime_oldest} - {self._datetime_newest}]"
            )

        return self.wrap(
            round(
                (timestamp - self._time_index_alignment).total_seconds()
                / self._sampling_period.total_seconds()
            )
        )

    def window(
        self, start: datetime, end: datetime, force_copy: bool = False
    ) -> FloatArray:
        """Request a view on the data between start timestamp and end timestamp.

        If the data is not used immediately it could be overwritten.
        Always request a copy if you keep the data around for longer.

        Will return a copy in the following cases:
        * The requested time period is crossing the start/end of the buffer.
        * The requested time period contains missing entries.
        * The force_copy parameter was set to True (default False).

        The first case can be avoided by using the appropriate
        `align_to` value in the constructor so that the data lines up
        with the start/end of the buffer.

        This means, if the caller needs to modify the data to account for
        missing entries, they can safely do so.

        Args:
            start: start time of the window.
            end: end time of the window.
            force_copy: optional, default False. If True, will always create a
                copy of the data.

        Raises:
            IndexError: When requesting a window with invalid timestamps.

        Returns:
            The requested window
        """
        if start > end:
            raise IndexError(
                f"end parameter {end} has to predate start parameter {start}"
            )

        start_index = self.datetime_to_index(start)
        end_index = self.datetime_to_index(end)

        # Requested window wraps around the ends
        if start_index >= end_index:
            if end_index > 0:
                if isinstance(self._buffer, list):
                    return self._buffer[start_index:] + self._buffer[0:end_index]
                if isinstance(self._buffer, np.ndarray):
                    return np.concatenate(
                        (self._buffer[start_index:], self._buffer[0:end_index])
                    )
                assert False, f"Unknown _buffer type: {type(self._buffer)}"
            return self._buffer[start_index:]

        # Return a copy if there are none-values in the data
        if force_copy or any(
            map(lambda gap: gap.contains(start) or gap.contains(end), self._gaps)
        ):
            return deepcopy(self[start_index:end_index])

        return self[start_index:end_index]

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
            if self._datetime_newest - newest >= self._full_time_range:
                self._gaps = [
                    Gap(start=self._datetime_oldest, end=self._datetime_newest)
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
                self._gaps.append(
                    Gap(start=timestamp, end=timestamp + self._sampling_period)
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
            if w_1.end <= self._datetime_oldest:
                del self._gaps[i]
            # Update start of gap if it's rolled out of the buffer
            elif w_1.start < self._datetime_oldest:
                self._gaps[i].start = self._datetime_oldest
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
        # Is the timestamp at the end? Shrinken by one then
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
    def __setitem__(self, index_or_slice: slice, value: Iterable[float]) -> None:
        """Set values at the request slice positions.

        No wrapping of the index will be done.
        Create a feature request if you require this function.

        Args:
            index_or_slice: Slice specification of the requested data.
            value: Sequence of value to set at the given range.
        """

    @overload
    def __setitem__(self, index_or_slice: SupportsIndex, value: float) -> None:
        """Set value at requested index.

        No wrapping of the index will be done.
        Create a feature request if you require this function.

        Args:
            index_or_slice: Index of the data.
            value: Value to set at the given position.
        """

    def __setitem__(
        self, index_or_slice: SupportsIndex | slice, value: float | Iterable[float]
    ) -> None:
        """Set item or slice at requested position.

        No wrapping of the index will be done.
        Create a feature request if you require this function.

        Args:
            index_or_slice: Index or slice specification of the requested data.
            value: Value to set at the given position.
        """
        # There seem to be 2 different mypy bugs at play here.
        # First we need to check that the combination of input arguments are
        # correct to make the type checker happy (I guess it could be inferred
        # from the @overloads, but it's not currently working without this
        # hack).
        # Then we need to ignore a no-untyped-call error, for some reason it
        # can't get the type for self._buffer.__setitem__()
        if isinstance(index_or_slice, SupportsIndex) and isinstance(
            value, SupportsFloat
        ):
            self._buffer.__setitem__(index_or_slice, value)  # type: ignore[no-untyped-call]
        elif isinstance(index_or_slice, slice) and isinstance(value, Iterable):
            self._buffer.__setitem__(index_or_slice, value)  # type: ignore[no-untyped-call]
        else:
            assert (
                False
            ), f"Incompatible input arguments: {type(index_or_slice)=} {type(value)=}"

    @overload
    def __getitem__(self, index_or_slice: SupportsIndex) -> float:
        """Get item at requested position.

        No wrapping of the index will be done.
        Create a feature request if you require this function.

        Args:
            index_or_slice: Index of the requested data.
        """

    @overload
    def __getitem__(self, index_or_slice: slice) -> FloatArray:
        """Get the data described by the given slice.

        No wrapping of the index will be done.
        Create a feature request if you require this function.

        Args:
            index_or_slice: Slice specification of where the requested data is.
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

    def __len__(self) -> int:
        """Return the amount of items that this container currently holds.

        Returns:
            The length.
        """
        if self._datetime_newest == self._DATETIME_MIN:
            return 0

        # Sum of all elements in the gap ranges
        sum_missing_entries = max(
            0,
            sum(
                (
                    gap.end
                    # Don't look further back than oldest timestamp
                    - max(gap.start, self._datetime_oldest)
                )
                // self._sampling_period
                for gap in self._gaps
            ),
        )

        start_index = self.datetime_to_index(self._datetime_oldest)
        end_index = self.datetime_to_index(self._datetime_newest)

        if end_index < start_index:
            return len(self._buffer) - start_index + end_index + 1 - sum_missing_entries

        return end_index + 1 - start_index - sum_missing_entries
