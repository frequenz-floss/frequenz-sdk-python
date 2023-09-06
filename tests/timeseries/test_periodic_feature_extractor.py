# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the timeseries averager."""

import contextlib
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import List

import numpy as np
import pytest
from frequenz.channels import Broadcast

from frequenz.sdk.timeseries import (
    UNIX_EPOCH,
    MovingWindow,
    PeriodicFeatureExtractor,
    Sample,
)
from frequenz.sdk.timeseries._quantities import Quantity
from tests.timeseries.test_moving_window import (
    init_moving_window,
    push_logical_meter_data,
)


@contextlib.asynccontextmanager
async def init_feature_extractor(
    data: list[float], period: timedelta
) -> AsyncIterator[PeriodicFeatureExtractor]:
    """
    Initialize a PeriodicFeatureExtractor with a `MovingWindow` that contains the data.

    Args:
        data: The data that is pushed into the moving window.
        period: The distance between two successive windows.

    Yields:
        PeriodicFeatureExtractor
    """
    window, sender = init_moving_window(timedelta(seconds=len(data)))
    async with window:
        await push_logical_meter_data(sender, data)
        yield PeriodicFeatureExtractor(moving_window=window, period=period)


@contextlib.asynccontextmanager
async def init_feature_extractor_no_data(
    period: int,
) -> AsyncIterator[PeriodicFeatureExtractor]:
    """
    Initialize a PeriodicFeatureExtractor with a `MovingWindow` that contains no data.

    Args:
        period: The distance between two successive windows.

    Yields:
        PeriodicFeatureExtractor
    """
    # We only need the moving window to initialize the PeriodicFeatureExtractor class.
    lm_chan = Broadcast[Sample[Quantity]]("lm_net_power")
    moving_window = MovingWindow(
        timedelta(seconds=1), lm_chan.new_receiver(), timedelta(seconds=1)
    )
    async with moving_window:
        await lm_chan.new_sender().send(
            Sample(datetime.now(tz=timezone.utc), Quantity(0))
        )

        # Initialize the PeriodicFeatureExtractor class with a period of period seconds.
        # This works since the sampling period is set to 1 second.
        yield PeriodicFeatureExtractor(moving_window, timedelta(seconds=period))


async def test_interval_shifting() -> None:
    """Test if a interval is properly shifted into a moving window."""
    async with init_feature_extractor(
        [1, 2, 2, 1, 1, 1, 2, 2, 1, 1], timedelta(seconds=5)
    ) as feature_extractor:
        # Test if the timestamp is not shifted
        timestamp = datetime(2023, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        index_not_shifted = (
            feature_extractor._timestamp_to_rel_index(  # pylint: disable=protected-access
                timestamp
            )
            % feature_extractor._period  # pylint: disable=protected-access
        )
        assert index_not_shifted == 1

        # Test if a timestamp in the window is shifted to the first appearance of the window
        timestamp = datetime(2023, 1, 1, 0, 0, 6, tzinfo=timezone.utc)
        index_shifted = (
            feature_extractor._timestamp_to_rel_index(  # pylint: disable=protected-access
                timestamp
            )
            % feature_extractor._period  # pylint: disable=protected-access
        )
        assert index_shifted == 1

        # Test if a timestamp outside the window is shifted
        timestamp = datetime(2023, 1, 1, 0, 0, 11, tzinfo=timezone.utc)
        index_shifted = (
            feature_extractor._timestamp_to_rel_index(  # pylint: disable=protected-access
                timestamp
            )
            % feature_extractor._period  # pylint: disable=protected-access
        )
        assert index_shifted == 1


async def test_feature_extractor() -> None:  # pylint: disable=too-many-statements
    """Test the feature extractor with a moving window that contains data."""
    start = UNIX_EPOCH + timedelta(seconds=1)
    end = start + timedelta(seconds=2)

    data: list[float] = [1, 2, 2.5, 1, 1, 1, 2, 2, 1, 1, 2, 2]

    async with init_feature_extractor(data, timedelta(seconds=3)) as feature_extractor:
        assert np.allclose(feature_extractor.avg(start, end), [5 / 3, 4 / 3])

    async with init_feature_extractor(data, timedelta(seconds=4)) as feature_extractor:
        assert np.allclose(feature_extractor.avg(start, end), [1, 2])

    data: list[float] = [1, 2, 2.5, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1]  # type: ignore[no-redef]

    async with init_feature_extractor(data, timedelta(seconds=5)) as feature_extractor:
        assert np.allclose(feature_extractor.avg(start, end), [1.5, 1.5])

    async def _test_fun(  # pylint: disable=too-many-arguments
        data: list[float],
        period: int,
        start: int,
        end: int,
        expected: list[float],
        weights: list[float] | None = None,
    ) -> None:
        async with init_feature_extractor(
            data, timedelta(seconds=period)
        ) as feature_extractor:
            ret = feature_extractor.avg(
                UNIX_EPOCH + timedelta(seconds=start),
                UNIX_EPOCH + timedelta(seconds=end),
                weights=weights,
            )
            assert np.allclose(ret, expected)

    async def test_09(
        period: int,
        start: int,
        end: int,
        expected: list[float],
        weights: list[float] | None = None,
    ) -> None:
        data: list[float] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        await _test_fun(data, period, start, end, expected, weights)

    async def test_011(
        period: int,
        start: int,
        end: int,
        expected: list[float],
        weights: list[float] | None = None,
    ) -> None:
        data: list[float] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        await _test_fun(data, period, start, end, expected, weights)

    # empty time period
    with pytest.raises(ValueError):
        await test_09(period=5, start=1, end=1, expected=[4711])

    # moving window not multiple of period
    with pytest.raises(ValueError):
        await test_09(period=3, start=1, end=1, expected=[4711])

    # time period in moving window
    await test_09(period=2, start=0, end=1, expected=[5])
    await test_09(period=2, start=0, end=2, expected=[5, 6])
    await test_09(period=2, start=5, end=7, expected=[4, 5])
    await test_09(period=2, start=8, end=10, expected=[5, 6])
    await test_09(period=5, start=0, end=1, expected=[5])
    await test_09(period=5, start=5, end=6, expected=[5])

    # time period outside of moving window in future
    await test_09(period=5, start=10, end=11, expected=[5])
    # time period outside of moving window in past
    await test_09(period=5, start=-5, end=-4, expected=[5])

    await test_09(period=5, start=0, end=2, expected=[5, 6])
    await test_09(period=5, start=0, end=3, expected=[5, 6, 7])
    await test_09(period=5, start=0, end=4, expected=[5, 6, 7, 8])
    await test_09(period=5, start=1, end=5, expected=[6, 7, 8, 9])

    # No full time period in moving window, expect to throw
    await test_09(period=5, start=0, end=5, expected=[5, 6, 7, 8, 9])
    with pytest.raises(Exception):
        await test_09(period=5, start=0, end=6, expected=[5])
    with pytest.raises(Exception):
        await test_09(period=5, start=0, end=7, expected=[5, 6])
    with pytest.raises(Exception):
        await test_09(period=5, start=0, end=8, expected=[5, 6, 7])
    with pytest.raises(Exception):
        await test_09(period=5, start=0, end=9, expected=[5, 6, 7, 8])
    with pytest.raises(Exception):
        await test_09(period=5, start=0, end=10, expected=[4711])
    with pytest.raises(Exception):
        await test_09(period=5, start=0, end=11, expected=[5])
    with pytest.raises(Exception):
        await test_09(period=5, start=0, end=12, expected=[5, 6])

    # time period outside window but more matches
    await test_09(
        period=5, start=8, end=11, expected=[3, 4, 5]
    )  # First occurence [-2, -1, 0] partly inside window

    # time period larger than period
    with pytest.raises(Exception):
        await test_09(period=2, start=8, end=11, expected=[5])
    with pytest.raises(Exception):
        await test_09(period=2, start=0, end=3, expected=[5])

    # Weights
    await test_011(period=4, start=0, end=2, expected=[6, 7])
    await test_011(period=4, start=0, end=2, expected=[6, 7], weights=None)
    await test_011(period=4, start=0, end=2, expected=[6, 7], weights=[1, 1])
    await test_011(
        period=4, start=0, end=2, expected=[4, 5], weights=[1, 0]
    )  # oldest weight first
    await test_011(period=4, start=0, end=2, expected=[6, 7], weights=[1, 1])
    with pytest.raises(ValueError):
        await test_011(
            period=4, start=0, end=2, expected=[4711, 4711], weights=[1, 1, 1]
        )
    with pytest.raises(ValueError):
        await test_011(period=4, start=0, end=2, expected=[4711, 4711], weights=[1])


async def test_profiler_calculate_np() -> None:
    """Test calculating the average with numpy and a pure python version.

    Calculate the average using a numpy array and compare the run time against the pure
    python method with the same functionality.
    """
    data = np.array([2, 2.5, 1, 1, 1, 2])
    async with init_feature_extractor_no_data(4) as feature_extractor:
        window_size = 2
        reshaped = (
            feature_extractor._reshape_np_array(  # pylint: disable=protected-access
                data, window_size
            )
        )
        result = np.average(reshaped[:, :window_size], axis=0)
        assert np.allclose(result, np.array([1.5, 2.25]))

    data = np.array([2, 2, 1, 1, 2])
    async with init_feature_extractor_no_data(5) as feature_extractor:
        reshaped = (
            feature_extractor._reshape_np_array(  # pylint: disable=protected-access
                data, window_size
            )
        )
        result = np.average(reshaped[:, :window_size], axis=0)
        assert np.allclose(result, np.array([2, 2]))
