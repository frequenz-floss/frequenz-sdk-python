"""
ComponentMetricGroupResampler class that delegates resampling to individual resamplers.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import logging
from datetime import datetime
from typing import Dict, Generator, Tuple

import pytz

from ...data_pipeline import Sample
from .component_metric_resampler import ComponentMetricResampler, ResamplingFunction

logger = logging.Logger(__name__)


class ComponentMetricGroupResampler:
    """Class that delegates resampling to individual component metric resamplers."""

    def __init__(
        self,
        *,
        resampling_period_s: float,
        initial_resampling_function: ResamplingFunction,
        max_data_age_in_periods: float = 3.0,
    ) -> None:
        """Initialize the ComponentMetricGroupResampler.

        Args:
            resampling_period_s: value describing how often resampling should be
                performed, in seconds
            initial_resampling_function: function to be applied to a sequence of
                samples within a resampling period to produce a single output sample
            max_data_age_in_periods: max age that samples shouldn't exceed in order
                to be used in the resampling function
        """
        self._resampling_period_s = resampling_period_s
        self._max_data_age_in_periods: float = max_data_age_in_periods
        self._initial_resampling_function: ResamplingFunction = (
            initial_resampling_function
        )
        self._resamplers: Dict[str, ComponentMetricResampler] = {}

    def add_time_series(self, time_series_id: str) -> None:
        """Create a new resampler for a specific time series.

        If resampler already exists for the provided `time_series_id`, it will be used
            without creating a new one.

        Args:
            time_series_id: time series id
        """
        if time_series_id in self._resamplers:
            return

        self._resamplers[time_series_id] = ComponentMetricResampler(
            resampling_period_s=self._resampling_period_s,
            max_data_age_in_periods=self._max_data_age_in_periods,
            resampling_function=self._initial_resampling_function,
        )

    def remove_timeseries(self, time_series_id: str) -> None:
        """Remove a resampler for a specific time series.

        Args:
            time_series_id: time series id, for which to remove the resampler

        Raises:
            KeyError: if resampler for the provided timer_series_id doesn't exist
        """
        try:
            del self._resamplers[time_series_id]
        except KeyError as err:
            raise KeyError(
                f"No resampler for time series {time_series_id} found!"
            ) from err

    def add_sample(self, time_series_id: str, sample: Sample) -> None:
        """Add a sample for a specific time series.

        Args:
            time_series_id: time series id, which the sample should be added to
            sample: sample to be added

        Raises:
            KeyError: if resampler for the provided timer_series_id doesn't exist
        """
        try:
            self._resamplers[time_series_id].add_sample(sample)
        except KeyError as err:
            raise KeyError(
                f"No resampler for time series {time_series_id} found!"
            ) from err

    def resample(self) -> Generator[Tuple[str, Sample], None, None]:
        """Resample samples for all time series.

        Yields:
            iterator of time series ids and their newly resampled samples
        """
        now = datetime.now(tz=pytz.UTC)
        for time_series_id, resampler in self._resamplers.items():
            yield time_series_id, Sample(timestamp=now, value=resampler.resample())
