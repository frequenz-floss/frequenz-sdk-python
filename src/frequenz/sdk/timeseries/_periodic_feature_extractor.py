# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""
A feature extractor for historical timeseries data.

It creates a profile from periodically occurring windows in a buffer of
historical data.

The profile is created out of all windows that are fully contained in the
underlying buffer, e.g. the moving window, with the same start and end time
modulo a fixed period.
"""


import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
from numpy.typing import NDArray

from .._internal._math import is_close_to_zero
from ._moving_window import MovingWindow
from ._ringbuffer import OrderedRingBuffer

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RelativePositions:
    """
    Holds the relative positions of a window in a buffer.

    When calculating a profile the user has to pass two datetime objects which
    are determining a window. It's allowed that this window is not contained in
    the historical data buffer.
    In order to calculate the profile the window is shifted to the first
    position in the buffer where it occurs first and represented by indexes
    relative to the oldest sample in the buffer.
    """

    start: int
    """The relative start position of the window."""
    end: int
    """The relative end position of the window."""
    next: int
    """The relative position of the next incoming sample."""


class PeriodicFeatureExtractor:
    """
    A feature extractor for historical timeseries data.

    This class is creating a profile from periodically occurring windows in a
    buffer of historical data.

    The profile is created out of all windows that are fully contained in the
    underlying buffer with the same start and end time modulo a fixed period.

    Consider for example a timeseries $T$ of historical data and sub-series
    $S_i$ of $T$ all having the same size $l$ and the same fixed distance $p$
    called period, where period of two sub-windows is defined as the distance
    of two points at the same position within the sub-windows.

    This class calculates a statistical profile $S$ over all $S_i$, i.e. the
    value of $S$ at position $i$ is calculated by performing a certain
    calculation, e.g. an average, over all values of $S_i$ at position $i$.

    Note:
        The oldest window or the window that is currently overwritten in the
        `MovingWindow` is not considered in the profile.

    Note:
        When constructing a `PeriodicFeatureExtractor` object the
        `MovingWindow` size has to be a integer multiple of the period.

    Example:
        ```python
        from frequenz.sdk import microgrid
        from datetime import datetime, timedelta, timezone

        async with MovingWindow(
            size=timedelta(days=35),
            resampled_data_recv=microgrid.grid().power.new_receiver(),
            input_sampling_period=timedelta(seconds=1),
        ) as moving_window:
            feature_extractor = PeriodicFeatureExtractor(
                moving_window=moving_window,
                period=timedelta(days=7),
            )

            now = datetime.now(timezone.utc)

            # create a daily weighted average for the next 24h
            avg_24h = feature_extractor.avg(
                now,
                now + timedelta(hours=24),
                weights=[0.1, 0.2, 0.3, 0.4]
            )

            # create a daily average for Thursday March 23 2023
            th_avg_24h = feature_extractor.avg(datetime(2023, 3, 23), datetime(2023, 3, 24))
        ```
    """

    def __init__(
        self,
        moving_window: MovingWindow,
        period: timedelta,
    ) -> None:
        """
        Initialize a PeriodicFeatureExtractor object.

        Args:
            moving_window: The MovingWindow that is used for the average calculation.
            period: The distance between two succeeding intervals.

        Raises:
            ValueError: If the MovingWindow size is not a integer multiple of the period.
        """
        self._moving_window = moving_window

        self._sampling_period = self._moving_window.sampling_period
        """The sampling_period as float to use it for indexing of samples."""

        self._period = int(period / self._sampling_period)
        """Distance between two succeeding intervals in samples."""

        _logger.debug("Initializing PeriodicFeatureExtractor!")
        _logger.debug("MovingWindow size: %i", self._moving_window.count_valid())
        _logger.debug(
            "Period between two succeeding intervals (in samples): %i",
            self._period,
        )

        if not self._moving_window.count_valid() % self._period == 0:
            raise ValueError(
                "The MovingWindow size is not a integer multiple of the period."
            )

        if not is_close_to_zero(self._period - period / self._sampling_period):
            raise ValueError(
                "The period is not a multiple of the sampling period. "
                "This might result in unexpected behaviour."
            )

    @property
    def _buffer(self) -> OrderedRingBuffer[NDArray[np.float64]]:
        return self._moving_window._buffer  # pylint: disable=protected-access

    def _timestamp_to_rel_index(self, timestamp: datetime) -> int:
        """
        Get the index of a timestamp relative to the oldest sample in the MovingWindow.

        In other word consider an integer axis where the zero is defined as the
        oldest element in the MovingWindow. This function returns the index of
        the given timestamp an this axis.

        This method can return negative values.

        Args:
            timestamp: A timestamp that we want to shift into the window.

        Returns:
            The index of the timestamp shifted into the MovingWindow.
        """
        # align timestamp to the sampling period
        timestamp = self._buffer.normalize_timestamp(timestamp)

        # distance between the input ts and the ts of oldest known samples (in samples)
        dist_to_oldest = int(
            (timestamp - self._buffer.time_bound_oldest) / self._sampling_period
        )

        _logger.debug("Shifting ts: %s", timestamp)
        _logger.debug("Oldest timestamp in buffer: %s", self._buffer.time_bound_oldest)
        _logger.debug("Distance to the oldest sample: %i", dist_to_oldest)

        return dist_to_oldest

    def _reshape_np_array(
        self, array: NDArray[np.float_], window_size: int
    ) -> NDArray[np.float_]:
        """
        Reshape a numpy array to a 2D array where each row represents a window.

        There are three cases to consider

        1. The array size is a multiple of window_size + period,
           i.e. num_windows is integer and we can simply reshape.
        2. The array size is NOT a multiple of window_size + period and
           it has length n * period + m, where m < window_size.
        3. The array size is NOT a multiple of window_size + period and
           it has length n * period + m, where m >= window_size.

        Note that in the current implementation of this class we have the restriction
        that period is a multiple integer of the size of the MovingWindow and hence
        only case 1 can occur.

        Args:
            array: The numpy array to reshape.
            window_size: The size of the window in samples.

        Returns:
            The reshaped 2D array.

        Raises:
            ValueError: If the array is smaller or equal to the given period.
        """
        # Not using the num_windows function here because we want to
        # differentiate between the three cases.
        if len(array) < self._period:
            raise ValueError(
                f"The array (length:{len(array)}) is too small to be reshaped."
            )

        num_windows = len(array) // self._period

        # Case 1:
        if len(array) - num_windows * self._period == 0:
            resized_array = array
        # Case 2
        elif len(array) - num_windows * self._period < window_size:
            resized_array = array[: num_windows * self._period]
        # Case 3
        else:
            num_windows += 1
            resized_array = np.resize(array, num_windows * self._period)

        return resized_array.reshape(num_windows, self._period)

    def _get_relative_positions(
        self, start: datetime, window_size: int
    ) -> RelativePositions:
        """
        Return relative positions of the MovingWindow.

        This method calculates the shifted relative positions of the start
        timestamp, the end timestamps as well as the next position that is
        overwritten in the ringbuffer.
        Shifted in that context means that the positions are moved as close
        assume possible to the oldest sample in the MovingWindow.

        Args:
            start: The start timestamp of the window.
            window_size: The size of the window in samples.

        Returns:
            The relative positions of the start, end and next samples.
        """
        # The number of usable windows can change, when the current position of
        # the ringbuffer is inside one of the windows inside the MovingWindow.
        # Since this is possible, we assume that one window is always not used
        # for the average calculation.
        #
        # We are ignoring either the window that is currently overwritten if
        # the current position is inside that window or the window that would
        # be overwritten next.
        #
        # Move the window to its first appearance in the MovingWindow relative
        # to the oldest sample stored in the MovingWindow.
        #
        # In other words the oldest stored sample is considered to have index 0.
        #
        # Note that the returned value is a index not a timestamp
        rel_start_sample = self._timestamp_to_rel_index(start) % self._period
        rel_end_sample = rel_start_sample + window_size

        # check if the newest time bound, i.e. the sample that is currently written,
        # is inside the interval
        rb_current_position = self._buffer.time_bound_newest
        rel_next_position = (
            self._timestamp_to_rel_index(rb_current_position) + 1
        ) % self._period
        # fix the rel_next_position if modulo period the next position
        # is smaller then the start sample position
        if rel_next_position < rel_start_sample:
            rel_next_position += self._period

        rel_next_position += self._period * (window_size // self._period)

        _logger.debug("current position of the ringbuffer: %s", rb_current_position)
        _logger.debug("relative start_sample: %s", rel_start_sample)
        _logger.debug("relative end_sample: %s", rel_end_sample)
        _logger.debug("relative next_position: %s", rel_next_position)

        return RelativePositions(rel_start_sample, rel_end_sample, rel_next_position)

    def _get_buffer_bounds(
        self, start: datetime, end: datetime
    ) -> tuple[int, int, int]:
        """
        Get the bounds of the ringbuffer used for further operations.

        This method uses the given start and end timestamps to calculate the
        part of the ringbuffer that can be used for further operations, like
        average or min/max.

        Here we cut out the oldest window or the window that is currently
        overwritten in the MovingWindow such that it is not considered in any
        further operation.

        Args:
            start: The start timestamp of the window.
            end: The end timestamp of the window.

        Returns:
            The bounds of the to be used buffer and the window size.

        Raises:
            ValueError: If the start timestamp is after the end timestamp.
        """
        window_size = self._timestamp_to_rel_index(end) - self._timestamp_to_rel_index(
            start
        )
        if window_size <= 0:
            raise ValueError("Start timestamp must be before end timestamp")
        if window_size > self._period:
            raise ValueError(
                "The window size must be smaller or equal than the period."
            )

        rel_pos = self._get_relative_positions(start, window_size)

        if window_size > self._moving_window.count_valid():
            raise ValueError(
                "The window size must be smaller than the size of the `MovingWindow`"
            )

        # shifted distance between the next incoming sample and the start of the window
        dist_to_start = rel_pos.next - rel_pos.start

        # get the start and end position inside the ringbuffer
        end_pos = (
            self._timestamp_to_rel_index(self._buffer.time_bound_newest) + 1
        ) - dist_to_start

        # Note that these check is working since we are using the positions
        # relative to the oldest sample stored in the MovingWindow.
        if rel_pos.start <= rel_pos.next < rel_pos.end:
            # end position is start_position of the window that is currently written
            # that's how end_pos is currently set
            _logger.debug("Next sample will be inside the window time interval!")
        else:
            _logger.debug("Next sample will be outside the window time interval!")
            # end position is start_position of the window that
            # is overwritten next, hence we adding period.
            end_pos += self._period

        # add the offset to the oldest sample in the ringbuffer and wrap around
        # to get the start and end positions in the ringbuffer
        rb_offset = self._buffer.to_internal_index(self._buffer.time_bound_oldest)
        start_pos = self._buffer.wrap(end_pos + self._period + rb_offset)
        end_pos = self._buffer.wrap(end_pos + rb_offset)

        _logger.debug("start_pos in ringbuffer: %s", start_pos)
        _logger.debug("end_pos in ringbuffer: %s", end_pos)

        return (start_pos, end_pos, window_size)

    def _get_reshaped_np_array(
        self, start: datetime, end: datetime
    ) -> tuple[NDArray[np.float_], int]:
        """
        Create a reshaped numpy array from the MovingWindow.

        The reshaped array is a two dimensional array, where one dimension is
        the window_size and the other the number of windows returned by the
        `_get_buffer_bounds` method.

        Args:
            start: The start timestamp of the window.
            end: The end timestamp of the window.

        Returns:
            A tuple containing the reshaped numpy array and the window size.
        """
        (start_pos, end_pos, window_size) = self._get_buffer_bounds(start, end)

        if start_pos >= end_pos:
            window_start = self._buffer[start_pos : self._moving_window.count_valid()]
            window_end = self._buffer[0:end_pos]
            # make the linter happy
            assert isinstance(window_start, np.ndarray)
            assert isinstance(window_end, np.ndarray)
            window_array = np.concatenate((window_start, window_end))
        else:
            window_array = self._buffer[start_pos:end_pos]

        return (self._reshape_np_array(window_array, window_size), window_size)

    def avg(
        self, start: datetime, end: datetime, weights: list[float] | None = None
    ) -> NDArray[np.float_]:
        """
        Create the average window out of the window defined by `start` and `end`.

        This method calculates the average of a window by averaging over all
        windows fully inside the MovingWindow having the period
        `self.period`.

        Args:
            start: The start of the window to average over.
            end: The end of the window to average over.
            weights: The weights to use for the average calculation (oldest first).

        Returns:
            The averaged timeseries window.
        """
        (reshaped, window_size) = self._get_reshaped_np_array(start, end)
        return np.average(  # type: ignore[no-any-return]
            reshaped[:, :window_size], axis=0, weights=weights
        )
