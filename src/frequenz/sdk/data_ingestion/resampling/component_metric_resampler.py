"""
ComponentMetricResampler class for resampling individual metrics.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import logging
from collections import deque
from datetime import datetime, timedelta
from typing import Callable, Deque, Optional, Sequence

import pytz

from ...data_pipeline import Sample

logger = logging.Logger(__name__)


ResamplingFunction = Callable[[Sequence[Sample], float], float]


class ComponentMetricResampler:
    """Resampler for a single metric of a specific component, e.g. 123_active_power."""

    def __init__(
        self,
        resampling_period_s: float,
        max_data_age_in_periods: float,
        resampling_function: ResamplingFunction,
    ) -> None:
        """Initialize the ComponentMetricResampler.

        Args:
            resampling_period_s: value describing how often resampling should be
                performed, in seconds
            max_data_age_in_periods: max age that samples shouldn't exceed in order
                to be used in the resampling function
            resampling_function: function to be applied to a sequence of samples within
                a resampling period to produce a single output sample
        """
        self._resampling_period_s = resampling_period_s
        self._max_data_age_in_periods: float = max_data_age_in_periods
        self._buffer: Deque[Sample] = deque()
        self._resampling_function: ResamplingFunction = resampling_function

    def add_sample(self, sample: Sample) -> None:
        """Add a new sample.

        Args:
            sample: sample to be added to the buffer
        """
        self._buffer.append(sample)

    def remove_outdated_samples(self, threshold: datetime) -> None:
        """Remove samples that are older than the provided time threshold.

        It is assumed that items in the buffer are in a sorted order (ascending order
        by timestamp).

        The removal works by traversing the buffer starting from the oldest sample
        (smallest timestamp) and comparing sample's timestamp with the threshold.
        If the sample's threshold is smaller than `threshold`, it means that the
        sample is outdated and it is removed from the buffer. This continues until
        the first sample that is with timestamp greater or equal to `threshold` is
        encountered, then buffer is considered up to date.

        Args:
            threshold: samples whose timestamp is older than the threshold are
                considered outdated and should be remove from the buffer
        """
        while self._buffer:
            sample: Sample = self._buffer[0]
            if sample.timestamp >= threshold:
                return

            self._buffer.popleft()

    def resample(self) -> Optional[float]:
        """Resample samples from the buffer and produce a single sample.

        Returns:
            Samples resampled into a single sample or `None` if the
                `resampling_function` cannot produce a valid Sample.
        """
        # It might be better to provide `now` from the outside so that all
        # individual resamplers use the same `now`
        now = datetime.now(tz=pytz.UTC)
        threshold = now - timedelta(
            seconds=self._max_data_age_in_periods * self._resampling_period_s
        )
        self.remove_outdated_samples(threshold=threshold)
        if len(self._buffer) == 0:
            return None
        return self._resampling_function(self._buffer, self._resampling_period_s)
