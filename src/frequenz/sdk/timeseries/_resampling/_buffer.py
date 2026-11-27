# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""The buffer holding the samples of a timeseries until they are resampled."""

import itertools
import logging
from bisect import bisect, bisect_left
from collections import deque
from datetime import datetime
from typing import assert_never

from ._base_types import SourceProperties
from ._config import WindowSide

_logger = logging.getLogger(__name__)


class _ResamplingBuffer:
    """The buffer holding the samples of a timeseries until they are resampled.

    The buffer can't hold more than `max_len` samples: when it is full, the oldest
    sample is dropped when a new one is added. Otherwise, old samples are only
    removed when asked to, while getting the relevant samples.

    It keeps the number of received and dropped samples, the windows without
    relevant samples, and the time of the first sample, up to date in the source
    properties. It logs when relevant
    samples were dropped or when it holds more than `warn_len` samples, and
    when it has no relevant samples, once per gap in the data.
    """

    def __init__(
        self,
        name: str,
        *,
        max_len: int,
        warn_len: int,
        source_properties: SourceProperties,
    ) -> None:
        """Initialize an instance.

        Args:
            name: The name of the timeseries (for logging purposes).
            max_len: The maximum number of samples the buffer can hold.
            warn_len: The number of samples above which a warning is logged.
            source_properties: The properties of the timeseries to keep up to date.
        """
        self._name = name
        self._samples: deque[tuple[datetime, float]] = deque(maxlen=max_len)
        self._warn_len = warn_len
        self._source_properties = source_properties
        self._removed_samples = 0
        """The number of samples removed because they were too old."""
        self._logged_dropped = False
        """Whether dropping relevant samples was already logged."""
        self._logged_too_big = False
        """Whether the buffer holding more than `warn_len` was already logged."""

    def __len__(self) -> int:
        """Return the number of samples in the buffer.

        Returns:
            The number of samples in the buffer.
        """
        return len(self._samples)

    def add(self, sample: tuple[datetime, float]) -> None:
        """Add a new sample to the buffer.

        Args:
            sample: The sample to add.
        """
        self._samples.append(sample)
        if self._source_properties.sampling_start is None:
            self._source_properties.sampling_start = sample[0]
        self._source_properties.received_samples += 1

    def relevant_samples(
        self,
        start: datetime,
        end: datetime,
        closed: WindowSide,
        *,
        remove_older: bool,
    ) -> list[tuple[datetime, float]]:
        """Return the samples between `start` and `end`.

        Args:
            start: The timestamp of the oldest relevant sample.
            end: The timestamp of the newest relevant sample.
            closed: Which side of the period between `start` and `end` includes
                the samples exactly at its edge.
            remove_older: Whether to remove the samples older than `start` from
                the buffer.

        Returns:
            The samples between `start` and `end`.
        """
        match closed:
            case WindowSide.LEFT:
                bisect_func = bisect_left
            case WindowSide.RIGHT:
                bisect_func = bisect
            case unexpected:
                assert_never(unexpected)

        min_index = bisect_func(self._samples, start, key=lambda s: s[0])
        max_index = bisect_func(self._samples, end, key=lambda s: s[0])

        # Dropped samples are counted here, as counting them when adding samples
        # would slow down adding samples, which must be as fast as possible.
        self._update_dropped_samples(min_index)

        # Using itertools for slicing doesn't look very efficient, but
        # experiments with a custom (ring) buffer that can slice showed that
        # it is not that bad. See:
        # https://github.com/frequenz-floss/frequenz-sdk-python/pull/130
        # So if we need more performance beyond this point, we probably need to
        # resort to some C (or similar) implementation.
        relevant_samples = list(itertools.islice(self._samples, min_index, max_index))
        # Missing samples are logged once per gap in the data, not for each window.
        props = self._source_properties
        if not relevant_samples:
            props.total_windows_without_samples += 1
            props.current_windows_without_samples += 1
            if props.current_windows_without_samples == 1:
                self._log_no_relevant_samples(start, end)
        elif props.current_windows_without_samples:
            _logger.info(
                "Relevant samples found again for timeseries %s, after %s windows "
                "without them",
                self._name,
                props.current_windows_without_samples,
            )
            props.current_windows_without_samples = 0

        if remove_older:
            for _ in range(min_index):
                self._samples.popleft()
            self._removed_samples += min_index
            self._log_too_big()

        return relevant_samples

    def _update_dropped_samples(self, min_index: int) -> None:
        """Update the number of dropped samples, logging once if needed ones may be.

        Args:
            min_index: The index of the oldest relevant sample in the buffer.
        """
        props = self._source_properties
        dropped = props.received_samples - self._removed_samples - len(self._samples)
        new_drops = dropped > props.dropped_samples
        props.dropped_samples = dropped
        # Dropped samples are older than the oldest one in the buffer, so they can
        # only be needed if the oldest one in the buffer is not older than the
        # relevant ones. Their timestamps are not kept, so they may also be older
        # than the relevant ones.
        if self._logged_dropped or not new_drops or min_index > 0:
            return
        self._logged_dropped = True
        _logger.error(
            "The buffer for timeseries %s is full (max_buffer_len=%s), so samples "
            "needed for resampling may have been dropped. Use a bigger "
            "max_buffer_len.",
            self._name,
            self._samples.maxlen,
        )

    def _log_too_big(self) -> None:
        """Log, once, if the buffer holds more than `warn_len` samples."""
        if self._logged_too_big or len(self._samples) <= self._warn_len:
            return
        self._logged_too_big = True
        _logger.warning(
            "The buffer for timeseries %s holds %s samples, more than "
            "warn_buffer_len (%s)",
            self._name,
            len(self._samples),
            self._warn_len,
        )

    def _log_no_relevant_samples(self, start: datetime, end: datetime) -> None:
        """Log that no relevant samples were found.

        Args:
            start: The timestamp of the oldest relevant sample.
            end: The timestamp of the newest relevant sample.
        """
        if not _logger.isEnabledFor(logging.WARNING):
            return

        if self._samples:
            buffer_info = (
                f"{self._samples[0][0]} to {self._samples[-1][0]}, "
                f"{len(self._samples)} samples"
            )
        else:
            buffer_info = "empty"

        _logger.warning(
            "No relevant samples found for timeseries %s between %s and %s "
            "(buffer: %s)",
            self._name,
            start,
            end,
            buffer_info,
        )
