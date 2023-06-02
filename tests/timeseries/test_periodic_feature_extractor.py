# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the timeseries averager."""

from datetime import datetime, timedelta, timezone
from typing import List

import numpy as np
from frequenz.channels import Broadcast

from frequenz.sdk.timeseries import (
    UNIX_EPOCH,
    MovingWindow,
    PeriodicFeatureExtractor,
    Sample,
)
from tests.timeseries.test_moving_window import (
    init_moving_window,
    push_logical_meter_data,
)


async def init_feature_extractor(
    data: List[float], period: timedelta
) -> PeriodicFeatureExtractor:
    """
    Initialize a PeriodicFeatureExtractor with a `MovingWindow` that contains the data.

    Args:
        data: The data that is pushed into the moving window.
        period: The distance between two successive windows.

    Returns:
        PeriodicFeatureExtractor
    """
    window, sender = init_moving_window(timedelta(seconds=len(data)))
    await push_logical_meter_data(sender, data)

    return PeriodicFeatureExtractor(moving_window=window, period=period)


async def init_feature_extractor_no_data(period: int) -> PeriodicFeatureExtractor:
    """
    Initialize a PeriodicFeatureExtractor with a `MovingWindow` that contains no data.

    Args:
        period: The distance between two successive windows.

    Returns:
        PeriodicFeatureExtractor
    """
    # We only need the moving window to initialize the PeriodicFeatureExtractor class.
    lm_chan = Broadcast[Sample]("lm_net_power")
    moving_window = MovingWindow(
        timedelta(seconds=1), lm_chan.new_receiver(), timedelta(seconds=1)
    )

    await lm_chan.new_sender().send(Sample(datetime.now(tz=timezone.utc), 0))

    # Initialize the PeriodicFeatureExtractor class with a period of period seconds.
    # This works since the sampling period is set to 1 second.
    return PeriodicFeatureExtractor(moving_window, timedelta(seconds=period))


async def test_interval_shifting() -> None:
    """
    Test if a interval is properly shifted into a moving window
    """
    feature_extractor = await init_feature_extractor(
        [1, 2, 2, 1, 1, 1, 2, 2, 1, 1], timedelta(seconds=5)
    )

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


async def test_feature_extractor() -> None:
    """Test the feature extractor with a moving window that contains data."""
    start = UNIX_EPOCH + timedelta(seconds=1)
    end = start + timedelta(seconds=2)

    data: List[float] = [1, 2, 2.5, 1, 1, 1, 2, 2, 1, 1, 2, 2]

    feature_extractor = await init_feature_extractor(data, timedelta(seconds=3))
    assert np.allclose(feature_extractor.avg(start, end), [5 / 3, 4 / 3])

    feature_extractor = await init_feature_extractor(data, timedelta(seconds=4))
    assert np.allclose(feature_extractor.avg(start, end), [1, 2])

    data: List[float] = [1, 2, 2.5, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1]  # type: ignore[no-redef]

    feature_extractor = await init_feature_extractor(data, timedelta(seconds=5))
    assert np.allclose(feature_extractor.avg(start, end), [1.5, 1.5])


async def test_profiler_calculate_np() -> None:
    """
    Test the calculation of the average using a numpy array and compare it
    against the pure python method with the same functionality.
    """
    data = np.array([2, 2.5, 1, 1, 1, 2])
    feature_extractor = await init_feature_extractor_no_data(4)
    window_size = 2
    reshaped = feature_extractor._reshape_np_array(  # pylint: disable=protected-access
        data, window_size
    )
    result = np.average(reshaped[:, :window_size], axis=0)
    assert np.allclose(result, np.array([1.5, 2.25]))

    data = np.array([2, 2, 1, 1, 2])
    feature_extractor = await init_feature_extractor_no_data(5)
    reshaped = feature_extractor._reshape_np_array(  # pylint: disable=protected-access
        data, window_size
    )
    result = np.average(reshaped[:, :window_size], axis=0)
    assert np.allclose(result, np.array([2, 2]))
