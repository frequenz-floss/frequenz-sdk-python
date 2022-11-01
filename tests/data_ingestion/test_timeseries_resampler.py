"""
Tests for the `TimeSeriesResampler`

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
from datetime import datetime, timedelta
from typing import Sequence

import pytz
import time_machine

from frequenz.sdk.data_ingestion.resampling.component_metric_resampler import (
    ComponentMetricResampler,
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
    assert len(samples) > 0, "Avg function cannot be given an empty list of samples"
    return sum(sample.value for sample in samples if sample.value is not None)


@time_machine.travel(datetime.now())
def test_component_metric_resampler_remove_outdated_samples() -> None:
    """Test if outdated samples are being properly removed."""
    resampler = ComponentMetricResampler(
        resampling_period_s=0.2,
        max_data_age_in_periods=1.0,
        resampling_function=resampling_function_sum,
    )

    timestamp = datetime.now(tz=pytz.UTC)
    sample1 = Sample(timestamp, value=5.0)
    sample2 = Sample(timestamp + timedelta(seconds=1), value=12.0)
    resampler.add_sample(sample1)
    resampler.add_sample(sample2)

    resampler.remove_outdated_samples(threshold=timestamp + timedelta(seconds=0.5))
    assert list(resampler._buffer) == [sample2]  # pylint: disable=protected-access

    resampler.remove_outdated_samples(threshold=timestamp + timedelta(seconds=1.01))
    assert len(resampler._buffer) == 0  # pylint: disable=protected-access


@time_machine.travel(datetime.now())
def test_component_metric_resampler_resample() -> None:
    """Test if resampling function works as expected."""
    resampler = ComponentMetricResampler(
        resampling_period_s=0.2,
        max_data_age_in_periods=5.0,
        resampling_function=resampling_function_sum,
    )

    timestamp = datetime.now(tz=pytz.UTC) - timedelta(seconds=0.5)

    value1 = 5.0
    value2 = 15.0

    sample1 = Sample(timestamp, value=value1)
    sample2 = Sample(timestamp, value=value2)

    resampler.add_sample(sample1)
    resampler.add_sample(sample2)

    value = resampler.resample()
    assert value is not None
    assert value == sum([value1, value2])


@time_machine.travel(datetime.now())
def test_component_metric_resampler_resample_with_outdated_samples() -> None:
    """Test that resampling function doesn't take outdated samples into account."""
    resampler = ComponentMetricResampler(
        resampling_period_s=0.2,
        max_data_age_in_periods=5.0,
        resampling_function=resampling_function_sum,
    )

    timestamp = datetime.now(tz=pytz.UTC)

    value3 = 100.0
    value1 = 5.0
    value2 = 15.0

    sample3 = Sample(timestamp - timedelta(seconds=1.01), value=value3)
    sample1 = Sample(timestamp - timedelta(seconds=0.5), value=value1)
    sample2 = Sample(timestamp - timedelta(seconds=0.7), value=value2)

    resampler.add_sample(sample3)
    resampler.add_sample(sample1)
    resampler.add_sample(sample2)

    value = resampler.resample()
    assert value is not None
    assert value == sum([value1, value2])
