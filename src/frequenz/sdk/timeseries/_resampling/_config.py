# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Resampler configuration."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from .._base_types import UNIX_EPOCH, Sample
from .._quantities import Quantity, QuantityT
from ._base_types import SourceProperties

_logger = logging.getLogger(__name__)


DEFAULT_BUFFER_LEN_INIT = 16
"""Default initial buffer length.

Buffers will be created initially with this length, but they could grow or
shrink depending on the source properties, like sampling rate, to make
sure all the requested past sampling periods can be stored.
"""


DEFAULT_BUFFER_LEN_MAX = 1024
"""Default maximum allowed buffer length.

If a buffer length would get bigger than this, it will be truncated to this
length.
"""


DEFAULT_BUFFER_LEN_WARN = 128
"""Default minimum buffer length that will produce a warning.

If a buffer length would get bigger than this, a warning will be logged.
"""


class ResamplingFunction(Protocol):
    """Combine multiple samples into a new one.

    A resampling function produces a new sample based on a list of pre-existing
    samples. It can do "upsampling" when there data rate of the `input_samples`
    period is smaller than the `resampling_period`, or "downsampling" if it is
    bigger.

    In general, a resampling window is the same as the `resampling_period`, and
    this function might receive input samples from multiple windows in the past to
    enable extrapolation, but no samples from the future (so the timestamp of the
    new sample that is going to be produced will always be bigger than the biggest
    timestamp in the input data).
    """

    def __call__(
        self,
        input_samples: Sequence[Sample[Quantity]],
        resampler_config: ResamplerConfig,
        source_properties: SourceProperties,
        /,
    ) -> float:
        """Call the resampling function.

        Args:
            input_samples: The sequence of pre-existing samples. It must be
                non-empty.
            resampler_config: The configuration of the resampler calling this
                function.
            source_properties: The properties of the source being resampled.

        Returns:
            The value of new sample produced after the resampling.
        """
        ...  # pylint: disable=unnecessary-ellipsis


def average(
    samples: Sequence[Sample[QuantityT]],
    resampler_config: ResamplerConfig,  # pylint: disable=unused-argument
    source_properties: SourceProperties,  # pylint: disable=unused-argument
) -> float:
    """Calculate average of all the provided values.

    Args:
        samples: The samples to apply the average to. It must be non-empty.
        resampler_config: The configuration of the resampler calling this
            function.
        source_properties: The properties of the source being resampled.

    Returns:
        The average of all `samples` values.
    """
    assert len(samples) > 0, "Average cannot be given an empty list of samples"
    values = list(
        sample.value.base_value for sample in samples if sample.value is not None
    )
    return sum(values) / len(values)


@dataclass(frozen=True)
class ResamplerConfig:
    """Resampler configuration."""

    resampling_period: timedelta
    """The resampling period.

    This is the time it passes between resampled data should be calculated.

    It must be a positive time span.
    """

    max_data_age_in_periods: float = 3.0
    """The maximum age a sample can have to be considered *relevant* for resampling.

    Expressed in number of periods, where period is the `resampling_period`
    if we are downsampling (resampling period bigger than the input period) or
    the input sampling period if we are upsampling (input period bigger than
    the resampling period).

    It must be bigger than 1.0.

    Example:
        If `resampling_period` is 3 seconds, the input sampling period is
        1 and `max_data_age_in_periods` is 2, then data older than 3*2
        = 6 seconds will be discarded when creating a new sample and never
        passed to the resampling function.

        If `resampling_period` is 3 seconds, the input sampling period is
        5 and `max_data_age_in_periods` is 2, then data older than 5*2
        = 10 seconds will be discarded when creating a new sample and never
        passed to the resampling function.
    """

    resampling_function: ResamplingFunction = average
    """The resampling function.

    This function will be applied to the sequence of relevant samples at
    a given time. The result of the function is what is sent as the resampled
    value.
    """

    initial_buffer_len: int = DEFAULT_BUFFER_LEN_INIT
    """The initial length of the resampling buffer.

    The buffer could grow or shrink depending on the source properties,
    like sampling rate, to make sure all the requested past sampling periods
    can be stored.

    It must be at least 1 and at most `max_buffer_len`.
    """

    warn_buffer_len: int = DEFAULT_BUFFER_LEN_WARN
    """The minimum length of the resampling buffer that will emit a warning.

    If a buffer grows bigger than this value, it will emit a warning in the
    logs, so buffers don't grow too big inadvertently.

    It must be at least 1 and at most `max_buffer_len`.
    """

    max_buffer_len: int = DEFAULT_BUFFER_LEN_MAX
    """The maximum length of the resampling buffer.

    Buffers won't be allowed to grow beyond this point even if it would be
    needed to keep all the requested past sampling periods. An error will be
    emitted in the logs if the buffer length needs to be truncated to this
    value.

    It must be at bigger than `warn_buffer_len`.
    """

    align_to: datetime | None = UNIX_EPOCH
    """The time to align the resampling period to.

    The resampling period will be aligned to this time, so the first resampled
    sample will be at the first multiple of `resampling_period` starting from
    `align_to`. It must be an aware datetime and can be in the future too.

    If `align_to` is `None`, the resampling period will be aligned to the
    time the resampler is created.
    """

    def __post_init__(self) -> None:
        """Check that config values are valid.

        Raises:
            ValueError: If any value is out of range.
        """
        if self.resampling_period.total_seconds() < 0.0:
            raise ValueError(
                f"resampling_period ({self.resampling_period}) must be positive"
            )
        if self.max_data_age_in_periods < 1.0:
            raise ValueError(
                f"max_data_age_in_periods ({self.max_data_age_in_periods}) should be at least 1.0"
            )
        if self.warn_buffer_len < 1:
            raise ValueError(
                f"warn_buffer_len ({self.warn_buffer_len}) should be at least 1"
            )
        if self.max_buffer_len <= self.warn_buffer_len:
            raise ValueError(
                f"max_buffer_len ({self.max_buffer_len}) should "
                f"be bigger than warn_buffer_len ({self.warn_buffer_len})"
            )

        if self.initial_buffer_len < 1:
            raise ValueError(
                f"initial_buffer_len ({self.initial_buffer_len}) should at least 1"
            )
        if self.initial_buffer_len > self.max_buffer_len:
            raise ValueError(
                f"initial_buffer_len ({self.initial_buffer_len}) is bigger "
                f"than max_buffer_len ({self.max_buffer_len}), use a smaller "
                "initial_buffer_len or a bigger max_buffer_len"
            )
        if self.initial_buffer_len > self.warn_buffer_len:
            _logger.warning(
                "initial_buffer_len (%s) is bigger than warn_buffer_len (%s)",
                self.initial_buffer_len,
                self.warn_buffer_len,
            )
        if self.align_to is not None and self.align_to.tzinfo is None:
            raise ValueError(
                f"align_to ({self.align_to}) should be a timezone aware datetime"
            )
