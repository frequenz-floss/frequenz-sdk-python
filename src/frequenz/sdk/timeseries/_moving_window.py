# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A data window that moves with the latest datapoints of a data stream."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import SupportsIndex, overload

import numpy as np
from frequenz.channels import Receiver
from numpy.typing import ArrayLike

from .._internal.asyncio import cancel_and_await
from . import Sample
from ._ringbuffer import OrderedRingBuffer

log = logging.getLogger(__name__)


class MovingWindow:
    """
    A data window that moves with the latest datapoints of a data stream.

    After initialization the `MovingWindow` can be accessed by an integer
    index or a timestamp. A sub window can be accessed by using a slice of
    integers or timestamps.

    Note that a numpy ndarray is returned and thus users can use
    numpys operations directly on a window.

    The window uses an ringbuffer for storage and the first element is aligned to
    a fixed defined point in time. Since the moving nature of the window, the
    date of the first and the last element are constantly changing and therefore
    the point in time that defines the alignment can be outside of the time window.
    Modulo arithmetic is used to move the `window_alignment` timestamp into the
    latest window.
    If for example the `window_alignment` parameter is set to `datetime(1, 1, 1)`
    and the window size is bigger than one day then the first element will always
    be aligned to the midnight. For further information see also the
    [`OrderedRingBuffer`][frequenz.sdk.timeseries._ringbuffer.OrderedRingBuffer]
    documentation.


    **Example1** (calculating the mean of a time interval):

    ```
    window = MovingWindow(
        size=timedelta(minutes=5),
        resampled_data_recv=resampled_data_recv,
    )

    time_start = datetime.now()
    time_end = time_start + timedelta(minutes=5)

    # ... wait for 5 minutes until the buffer is filled
    await asyncio.sleep(5)

    # return an numpy array from the window
    a = window[time_start:time_end]
    # and use it to for example calculate the mean
    mean = a.mean()
    '''

    **Example2** (create a polars data frame from a `MovingWindow`):

    ```
    import polars as pl

    # create a window that stores two days of data
    # starting at 1.1.23 with samplerate=1
    window = MovingWindow(size=timedelta(days=2), sample_receiver)

    # wait for one full day until the buffer is filled
    asyncio.sleep(60*60*24)

    # create a polars series with one full day of data
    time_start = datetime(2023, 1, 1)
    time_end = datetime(2023, 1, 2)
    s = pl.Series("Jan_1", mv[time_start:time_end])
    ```
    """

    def __init__(
        self,
        size: timedelta,
        resampled_data_recv: Receiver[Sample],
        sampling_period: timedelta,
        window_alignment: datetime = datetime(1, 1, 1),
    ) -> None:
        """
        Initialize the MovingWindow.

        This method creates the underlying ringbuffer and starts a
        new task that updates the ringbuffer with new incoming samples.
        The task stops running only if the channel receiver is closed.

        Args:
            size: The time span of the moving window over which samples will be stored.
            resampled_data_recv: A receiver that delivers samples with a
                given sampling period.
            sampling_period: The sampling period.
            window_alignment: A datetime object that defines a point in time to which
                the window is aligned to modulo window size.
                (default is midnight 01.01.01)
                For further information, consult the class level documentation.

        Raises:
            asyncio.CancelledError: when the task gets cancelled.
        """
        assert (
            sampling_period.total_seconds() > 0
        ), "The sampling period should be greater than zero."
        assert (
            sampling_period <= size
        ), "The sampling period should be equal to or lower than the window size."

        # Sampling period might not fit perfectly into the window size.
        num_samples = math.ceil(size / sampling_period)

        self._resampled_data_recv = resampled_data_recv
        self._buffer = OrderedRingBuffer(
            np.empty(shape=num_samples, dtype=float),
            sampling_period=sampling_period,
            time_index_alignment=window_alignment,
        )

        self._update_window_task: asyncio.Task[None] = asyncio.create_task(
            self._run_impl()
        )
        log.debug("Cancelling MovingWindow task: %s", __name__)

    async def _run_impl(self) -> None:
        """Awaits samples from the receiver and updates the underlying ringbuffer."""
        try:
            async for sample in self._resampled_data_recv:
                log.debug("Received new sample: %s", sample)
                self._buffer.update(sample)
        except asyncio.CancelledError:
            log.info("MovingWindow task has been cancelled.")
            return

        log.error("Channel has been closed")

    async def stop(self) -> None:
        """Cancel the running task and stop the MovingWindow."""
        await cancel_and_await(self._update_window_task)

    def __len__(self) -> int:
        """
        Return the size of the `MovingWindow`s underlying buffer.

        Returns:
            The size of the `MovingWindow`.
        """
        return len(self._buffer)

    @overload
    def __getitem__(self, key: SupportsIndex) -> float:
        """See the main __getitem__ method.

        # noqa: DAR101 key
        """

    @overload
    def __getitem__(self, key: datetime) -> float:
        """See the main __getitem__ method.

        # noqa: DAR101 key
        """

    @overload
    def __getitem__(self, key: slice) -> ArrayLike:
        """See the main __getitem__ method.

        # noqa: DAR101 key
        """

    def __getitem__(self, key: SupportsIndex | datetime | slice) -> float | ArrayLike:
        """
        Return a sub window of the `MovingWindow`.

        The `MovingWindow` is accessed either by timestamp or by index
        or by a slice of timestamps or integers.

        * If the key is an integer, the float value of that key
          at the given position is returned.
        * If the key is a datetime object, the float value of that key
          that corresponds to the timestamp is returned.
        * If the key is a slice of timestamps or integers, an ndarray is returned,
          where the bounds correspond to the slice bounds.
          Note that a half open interval, which is open at the end, is returned.

        Args:
            key: Either an integer or a timestamp or a slice of timestamps or integers.

        Raises:
            IndexError: when requesting an out of range timestamp or index
            TypeError: when the key is not a datetime or slice object.

        Returns:
            A float if the key is a number or a timestamp.
            an numpy array if the key is a slice.
        """
        if isinstance(key, slice):
            log.debug("Returning slice for [%s:%s].", key.start, key.stop)
            # we are doing runtime typechecks since there is no abstract slice type yet
            # see also (https://peps.python.org/pep-0696)
            if isinstance(key.start, datetime) and isinstance(key.stop, datetime):
                return self._buffer.window(key.start, key.stop)
            if isinstance(key.start, int) and isinstance(key.stop, int):
                return self._buffer[key]
        elif isinstance(key, datetime):
            log.debug("Returning value at time %s ", key)
            return self._buffer[self._buffer.datetime_to_index(key)]
        elif isinstance(key, SupportsIndex):
            return self._buffer[key]

        raise TypeError(
            "Key has to be either a timestamp or an integer "
            "or a slice of timestamps or integers"
        )


# We need to register the class as a subclass of Sequence like this because
# otherwise type-checking fails complaining that MovingWindow has more
# overloads of __getitem__() than Sequence (which doesn't have an overload with
# a datetime key)
Sequence.register(MovingWindow)
