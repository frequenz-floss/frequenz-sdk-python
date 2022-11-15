"""
Tests for the `TimeSeriesResampler`

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from datetime import datetime, timedelta, timezone
from typing import Sequence

import time_machine

from frequenz.sdk.data_pipeline import Sample
from frequenz.sdk.timeseries.resampler import (
    ComponentMetricGroupResampler,
    ComponentMetricResampler,
)


# pylint: disable=unused-argument
def resampling_function_sum(
    samples: Sequence[Sample], resampling_period_s: float
) -> float:
    """Calculate sum of the provided values.

    Args:
        samples: sequences of samples to apply the average to
        resampling_period_s: value describing how often resampling should be performed,
            in seconds

    Returns:
        sum of all the sample values

    Raises:
        AssertionError if there are no provided samples
    """
    assert len(samples) > 0, "Avg function cannot be given an empty list of samples"
    return sum(sample.value for sample in samples if sample.value is not None)


@time_machine.travel(0, tick=False)
def test_component_metric_resampler_remove_outdated_samples() -> None:
    """Test if outdated samples are being properly removed."""
    resampler = ComponentMetricResampler(
        resampling_period_s=0.2,
        max_data_age_in_periods=1.0,
        resampling_function=resampling_function_sum,
    )

    timestamp = datetime.now(timezone.utc)
    sample1 = Sample(timestamp, value=5.0)
    sample2 = Sample(timestamp + timedelta(seconds=1), value=12.0)
    resampler.add_sample(sample1)
    resampler.add_sample(sample2)

    resampler.remove_outdated_samples(threshold=timestamp)
    assert list(resampler._buffer) == [
        sample1,
        sample2,
    ]  # pylint: disable=protected-access

    resampler.remove_outdated_samples(threshold=timestamp + timedelta(seconds=0.5))
    assert list(resampler._buffer) == [sample2]  # pylint: disable=protected-access

    resampler.remove_outdated_samples(threshold=timestamp + timedelta(seconds=1.01))
    assert len(resampler._buffer) == 0  # pylint: disable=protected-access


@time_machine.travel(0, tick=False)
def test_component_metric_resampler_resample() -> None:
    """Test if resampling function works as expected."""
    resampler = ComponentMetricResampler(
        resampling_period_s=0.2,
        max_data_age_in_periods=5.0,
        resampling_function=resampling_function_sum,
    )

    now = datetime.now(timezone.utc)
    timestamp1 = now - timedelta(seconds=0.5)
    timestamp2 = now - timedelta(seconds=0.2)

    value1 = 5.0
    value2 = 15.0

    sample1 = Sample(timestamp1, value=value1)
    sample2 = Sample(timestamp2, value=value2)

    resampler.add_sample(sample1)
    resampler.add_sample(sample2)

    value = resampler.resample()
    assert value is not None
    assert value == sum([value1, value2])

    value = resampler.resample(now + timedelta(seconds=0.6))
    assert value is not None
    assert value == value2


@time_machine.travel(0, tick=False)
def test_component_metric_resampler_resample_with_outdated_samples() -> None:
    """Test that resampling function doesn't take outdated samples into account."""
    resampler = ComponentMetricResampler(
        resampling_period_s=0.2,
        max_data_age_in_periods=5.0,
        resampling_function=resampling_function_sum,
    )

    timestamp = datetime.now(timezone.utc)

    value1 = 100.0
    value2 = 15.0
    value3 = 5.0

    sample1 = Sample(timestamp - timedelta(seconds=1.01), value=value1)
    sample2 = Sample(timestamp - timedelta(seconds=0.7), value=value2)
    sample3 = Sample(timestamp - timedelta(seconds=0.5), value=value3)

    resampler.add_sample(sample1)
    resampler.add_sample(sample2)
    resampler.add_sample(sample3)

    value = resampler.resample()
    assert value is not None
    assert value == sum([value2, value3])


@time_machine.travel(0, tick=False)
def test_component_metric_group_resampler() -> None:
    """Test if resampling is properly delegated to component metric resamplers."""
    resampler = ComponentMetricGroupResampler(
        resampling_period_s=0.2,
        max_data_age_in_periods=5.0,
        initial_resampling_function=resampling_function_sum,
    )

    time_series_id_1 = "123_active_power"
    time_series_id_2 = "99_active_power"

    resampler.add_time_series(time_series_id=time_series_id_1)
    resampler.add_time_series(time_series_id=time_series_id_2)

    now = datetime.now(timezone.utc)

    value11 = 5.0
    value12 = 15.0
    value21 = 100.0
    value22 = 999.0

    sample11 = Sample(now - timedelta(seconds=0.7), value=value11)
    sample12 = Sample(now - timedelta(seconds=0.5), value=value12)
    sample21 = Sample(now - timedelta(seconds=5.05), value=value21)
    sample22 = Sample(now - timedelta(seconds=0.99), value=value22)

    resampler.add_sample(time_series_id=time_series_id_1, sample=sample11)
    resampler.add_sample(time_series_id=time_series_id_1, sample=sample12)
    resampler.add_sample(time_series_id=time_series_id_2, sample=sample21)
    resampler.add_sample(time_series_id=time_series_id_2, sample=sample22)

    resampled_samples = dict(resampler.resample())

    assert resampled_samples[time_series_id_1].timestamp >= now
    assert resampled_samples[time_series_id_1].value == sum([value11, value12])

    assert resampled_samples[time_series_id_2].timestamp >= now
    assert resampled_samples[time_series_id_2].value == value22

    timestamp = now + timedelta(seconds=0.5)
    resampled_samples = dict(resampler.resample(timestamp))

    assert resampled_samples[time_series_id_1].timestamp == timestamp
    assert resampled_samples[time_series_id_1].value == value12

    assert resampled_samples[time_series_id_2].timestamp == timestamp
    assert resampled_samples[time_series_id_2].value is None
