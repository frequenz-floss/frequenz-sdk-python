# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the example resampler configuration."""

import dataclasses
from datetime import timedelta

from frequenz.sdk import microgrid
from frequenz.sdk.timeseries import ResamplerConfig3


def test_example_resampler_config_values() -> None:
    """Test the configuration has the values its documentation explains."""
    assert microgrid.EXAMPLE_RESAMPLER_CONFIG == ResamplerConfig3(
        timedelta(seconds=1),
        max_data_age_in_periods=3.0,
        resampling_function=microgrid.latest_window_mean,
    )


def test_example_resampler_config_can_be_changed_with_replace() -> None:
    """Test the `dataclasses.replace()` example in the documentation.

    Code examples in attribute docstrings are not checked automatically.
    """
    config = dataclasses.replace(
        microgrid.EXAMPLE_RESAMPLER_CONFIG, resampling_period=timedelta(seconds=5)
    )

    assert config.resampling_period == timedelta(seconds=5)
    assert config.max_data_age_in_periods == 3.0
    assert config.resampling_function is microgrid.latest_window_mean
