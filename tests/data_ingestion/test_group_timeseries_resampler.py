"""
Tests for the `ComponentMetricGroupResampler`

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
from datetime import datetime, timedelta
from typing import Sequence

import pytz
import time_machine

from frequenz.sdk.data_ingestion.resampling.component_metric_group_resampler import (
    ComponentMetricGroupResampler,
)
from frequenz.sdk.data_pipeline import Sample


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
    assert len(samples) > 0, "Sum cannot be given an empty list of samples"
    return sum(sample.value for sample in samples if sample.value is not None)


@time_machine.travel(datetime.now())
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

    timestamp = datetime.now(tz=pytz.UTC)

    value1 = 5.0
    value2 = 15.0
    value3 = 100.0
    value4 = 999.0

    sample1 = Sample(timestamp - timedelta(seconds=0.5), value=value1)
    sample2 = Sample(timestamp - timedelta(seconds=0.7), value=value2)
    sample3 = Sample(timestamp - timedelta(seconds=5.05), value=value3)
    sample4 = Sample(timestamp - timedelta(seconds=0.99), value=value4)

    resampler.add_sample(time_series_id=time_series_id_1, sample=sample1)
    resampler.add_sample(time_series_id=time_series_id_1, sample=sample2)
    resampler.add_sample(time_series_id=time_series_id_2, sample=sample3)
    resampler.add_sample(time_series_id=time_series_id_2, sample=sample4)

    resampled_samples = dict(resampler.resample())

    assert resampled_samples[time_series_id_1].timestamp >= timestamp
    assert resampled_samples[time_series_id_1].value == sum([value1, value2])

    assert resampled_samples[time_series_id_2].timestamp >= timestamp
    assert resampled_samples[time_series_id_2].value == value4
