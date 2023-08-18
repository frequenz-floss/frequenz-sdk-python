# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""
Benchmarks for the PeriodicFeatureExtractor class.

This module contains benchmarks that are comparing
the performance of a numpy implementation with a python
implementation.
"""

from __future__ import annotations

import asyncio
import collections.abc
import contextlib
import logging
from datetime import datetime, timedelta, timezone
from functools import partial
from timeit import timeit
from typing import List

import numpy as np
from frequenz.channels import Broadcast
from numpy.random import default_rng
from numpy.typing import NDArray

from frequenz.sdk.timeseries import MovingWindow, PeriodicFeatureExtractor, Sample
from frequenz.sdk.timeseries._quantities import Quantity


@contextlib.asynccontextmanager
async def init_feature_extractor(
    period: int,
) -> collections.abc.AsyncIterator[PeriodicFeatureExtractor]:
    """Initialize the PeriodicFeatureExtractor class."""
    # We only need the moving window to initialize the PeriodicFeatureExtractor class.
    lm_chan = Broadcast[Sample[Quantity]]("lm_net_power")
    async with MovingWindow(
        timedelta(seconds=1), lm_chan.new_receiver(), timedelta(seconds=1)
    ) as moving_window:
        await lm_chan.new_sender().send(
            Sample(datetime.now(tz=timezone.utc), Quantity(0))
        )

        # Initialize the PeriodicFeatureExtractor class with a period of period seconds.
        # This works since the sampling period is set to 1 second.
        yield PeriodicFeatureExtractor(moving_window, timedelta(seconds=period))


def _calculate_avg_window(
    feature_extractor: PeriodicFeatureExtractor,
    window: NDArray[np.float_],
    window_size: int,
) -> NDArray[np.float_]:
    """
    Reshapes the window and calculates the average.

    This method calculates the average of a window by averaging over all
    windows fully inside the passed numpy array having the period
    `self.period`.

    Args:
        feature_extractor: The instance of the PeriodicFeatureExtractor to use.
        window: The window to calculate the average over.
        window_size: The size of the window to calculate the average over.
        weights: The weights to use for the average calculation.

    Returns:
        The averaged window.
    """
    reshaped = feature_extractor._reshape_np_array(  # pylint: disable=protected-access
        window, window_size
    )
    # ignoring the type because np.average returns Any
    return np.average(reshaped[:, :window_size], axis=0)  # type: ignore[no-any-return]


def _calculate_avg_window_py(
    feature_extractor: PeriodicFeatureExtractor,
    window: NDArray[np.float_],
    window_size: int,
    weights: List[float] | None = None,
) -> NDArray[np.float_]:
    """
    Plain python version of the average calculator.

    This method avoids copying in any case but is 15 to 600 slower then the
    numpy version.

    This method is only used in these benchmarks.

    Args:
        feature_extractor: The instance of the PeriodicFeatureExtractor to use.
        window: The window to calculate the average over.
        window_size: The size of the window to calculate the average over.
        weights: The weights to use for the average calculation.

    Returns:
        The averaged window.
    """

    def _num_windows(
        window: NDArray[np.float_] | MovingWindow, window_size: int, period: int
    ) -> int:
        """
        Get the number of windows that are fully contained in the MovingWindow.

        This method calculates how often a given window, defined by it size, is
        fully contained in the MovingWindow at its current state or any numpy
        ndarray given the period between two window neighbors.

        Args:
            window: The buffer that is used for the average calculation.
            window_size: The size of the window in samples.

        Returns:
            The number of windows that are fully contained in the MovingWindow.
        """
        num_windows = len(window) // period
        if len(window) - num_windows * period >= window_size:
            num_windows += 1

        return num_windows

    period = feature_extractor._period  # pylint: disable=protected-access

    num_windows = _num_windows(
        window,
        window_size,
        period,
    )

    res = np.empty(window_size)

    for i in range(window_size):
        assert num_windows * period - len(window) <= period
        summe = 0
        for j in range(num_windows):
            if weights is None:
                summe += window[i + (j * period)]
            else:
                summe += weights[j] * window[i + (j * period)]

        if not weights:
            res[i] = summe / num_windows
        else:
            res[i] = summe / np.sum(weights)

    return res


def run_benchmark(
    array: NDArray[np.float_],
    window_size: int,
    feature_extractor: PeriodicFeatureExtractor,
) -> None:
    """Run the benchmark for the given ndarray and window size."""

    def run_avg_np(
        array: NDArray[np.float_],
        window_size: int,
        feature_extractor: PeriodicFeatureExtractor,
    ) -> None:
        """
        Run the FeatureExtractor.

        The return value is discarded such that it can be used by timit.

        Args:
            a: The array containing all data.
            window_size: The size of the window.
            feature_extractor: An instance of the PeriodicFeatureExtractor.
        """
        _calculate_avg_window(feature_extractor, array, window_size)

    def run_avg_py(
        array: NDArray[np.float_],
        window_size: int,
        feature_extractor: PeriodicFeatureExtractor,
    ) -> None:
        """
        Run the FeatureExtractor.

        The return value is discarded such that it can be used by timit.

        Args:
            a: The array containing all data.
            window_size: The size of the window.
            feature_extractor: An instance of the PeriodicFeatureExtractor.
        """
        _calculate_avg_window_py(feature_extractor, array, window_size)

    time_np = timeit(
        partial(run_avg_np, array, window_size, feature_extractor), number=10
    )
    time_py = timeit(
        partial(run_avg_py, array, window_size, feature_extractor), number=10
    )
    print(time_np)
    print(time_py)
    print(f"Numpy is {time_py / time_np} times faster!")


DAY_S = 24 * 60 * 60


async def main() -> None:
    """
    Run the benchmarks.

    The benchmark are comparing the numpy
    implementation with the python implementation.
    """
    # initialize random number generator
    rng = default_rng()

    # create a random ndarray with 29 days -5 seconds of data
    days_29_s = 29 * DAY_S
    async with init_feature_extractor(10) as feature_extractor:
        data = rng.standard_normal(days_29_s)
        run_benchmark(data, 4, feature_extractor)

        days_29_s = 29 * DAY_S + 3
        data = rng.standard_normal(days_29_s)
        run_benchmark(data, 4, feature_extractor)

        # create a random ndarray with 29 days +5 seconds of data
        data = rng.standard_normal(29 * DAY_S + 5)

    async with init_feature_extractor(7 * DAY_S) as feature_extractor:
        # TEST one day window and 6 days distance. COPY (Case 3)
        run_benchmark(data, DAY_S, feature_extractor)
        # benchmark one day window and 6 days distance. NO COPY (Case 1)
        run_benchmark(data[: 28 * DAY_S], DAY_S, feature_extractor)


logging.basicConfig(level=logging.DEBUG)
asyncio.run(main())
