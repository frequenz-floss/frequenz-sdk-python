# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Resampler configuration."""

from __future__ import annotations

import logging
import statistics
from collections.abc import Sequence
from dataclasses import KW_ONLY, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol, assert_never

from frequenz.core.datetime import UNIX_EPOCH
from frequenz.core.typing import FloatInt
from frequenz.core.warnings import ignoring_deprecations
from typing_extensions import deprecated

from ._base_types import SourceProperties
from ._wall_clock_timer import WallClockTimerConfig

_logger = logging.getLogger(__name__)


DEFAULT_BUFFER_LEN_INIT = 16
"""Default number of samples to receive before estimating the input sampling period.
"""


DEFAULT_BUFFER_LEN_MAX = 1024
"""Default maximum number of samples a buffer can hold.

When a buffer is full, the oldest sample is dropped when a new one is added.
"""


DEFAULT_BUFFER_LEN_WARN = 128
"""Default number of samples above which a buffer logs a warning.
"""


class WindowSide(Enum):
    """Represents a side of a resampling window."""

    LEFT = "left"
    """The left side of the resampling window."""

    RIGHT = "right"
    """The right side of the resampling window."""


@deprecated(
    "frequenz.sdk.timeseries.ResamplingFunction is deprecated. "
    "Use frequenz.sdk.timeseries.ResamplingFunction3 instead, which receives a "
    "resampling context instead of the configuration."
)
class ResamplingFunction(Protocol):
    """Combine multiple samples into a new one.

    Deprecated:
        Deprecated since v1.0.0-rc2217. Use
        [`ResamplingFunction3`][frequenz.sdk.timeseries.ResamplingFunction3] instead.

    A resampling function produces a new sample based on a list of pre-existing
    samples. It can do "upsampling" when the data rate of the `input_samples`
    period is smaller than the `resampling_period`, or "downsampling" if it is
    bigger.

    In general, a resampling window is the same as the `resampling_period`, and
    this function might receive input samples from multiple windows in the past to
    enable extrapolation, but no samples from the future (so the timestamp of the
    new sample that is going to be produced will always be bigger than the biggest
    timestamp in the input data).

    A [`ResamplingFunction3`][frequenz.sdk.timeseries.ResamplingFunction3] receives
    a [`ResamplingContext`][frequenz.sdk.timeseries.ResamplingContext] with the
    configuration and the window being resampled, instead of the configuration.
    """

    def __call__(
        self,
        input_samples: Sequence[tuple[datetime, float]],
        resampler_config: ResamplerConfig,
        source_properties: SourceProperties,
        /,
    ) -> float:
        """Call the resampling function.

        Args:
            input_samples: The sequence of pre-existing samples, where the first item is
                the timestamp of the sample, and the second is the value of the sample.
                The sequence must be non-empty.
            resampler_config: The configuration of the resampler calling this
                function.
            source_properties: The properties of the source being resampled.

        Returns:
            The value of new sample produced after the resampling.
        """
        ...  # pylint: disable=unnecessary-ellipsis


@deprecated(
    "frequenz.sdk.timeseries.ResamplerConfig is deprecated. "
    "Use frequenz.sdk.timeseries.ResamplerConfig3 instead, but note it uses a "
    "wall clock timer and has different defaults, so resampled values can change."
)
@dataclass(frozen=True)
class ResamplerConfig:
    """Resampler configuration.

    Deprecated:
        Deprecated since v1.0.0-rc2217. Use
        [`ResamplerConfig3`][frequenz.sdk.timeseries.ResamplerConfig3] instead.

    With a [`ResamplerConfig3`][frequenz.sdk.timeseries.ResamplerConfig3], the
    resampler uses a [`WallClockTimer`][frequenz.sdk.timeseries.WallClockTimer]
    instead of a monotonic timer, so the alignment is set with
    [`timer_config`][frequenz.sdk.timeseries.ResamplerConfig3.timer_config]
    instead of `align_to`. It also has different defaults, and its resampling
    function receives a
    [`ResamplingContext`][frequenz.sdk.timeseries.ResamplingContext] instead of
    the configuration. To keep the results of this class, pass
    `max_data_age_in_periods=3.0`, `closed=WindowSide.RIGHT` and
    `label=WindowSide.RIGHT`.
    """

    resampling_period: timedelta
    """The resampling period.

    This is the time it passes between resampled data should be calculated.

    It must be a positive time span.
    """

    max_data_age_in_periods: float = 3.0
    """The maximum age a sample can have to be considered *relevant* for resampling.

    Expressed in number of periods, where period is the `resampling_period`
    if we are downsampling (resampling period bigger than the input period) or
    the *input sampling period* if we are upsampling (input period bigger than
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

    resampling_function: ResamplingFunction = lambda samples, _, __: statistics.fmean(
        s[1] for s in samples
    )
    """The resampling function.

    This function will be applied to the sequence of relevant samples at
    a given time. The result of the function is what is sent as the resampled
    value.
    """

    closed: WindowSide = WindowSide.RIGHT
    """Indicates which side of the resampling window is closed.

    If `WindowSide.RIGHT` (default), the resampling window is closed on the
    right side and open on the left, meaning it includes samples with timestamps
    within the range (start, end], where `start` and `end` are the boundaries of
    the window.

    If `WindowSide.LEFT`, the resampling window is closed on the left side and
    open on the right, meaning it includes samples with timestamps within the
    range [start, end), where `start` and `end` are the boundaries of the
    window.
    """

    label: WindowSide = WindowSide.RIGHT
    """Indicates the timestamp label of the resampled data.

    If `WindowSide.RIGHT` (default), the timestamp of the resampled data
    corresponds to the right boundary of the resampling window. If
    `WindowSide.LEFT`, the timestamp corresponds to the left boundary of the
    resampling window.
    """

    initial_buffer_len: int = DEFAULT_BUFFER_LEN_INIT
    """The number of samples to receive before estimating the input sampling period.

    The input sampling period is estimated once, at the first resampling after
    receiving enough samples, and decides how old the relevant samples can be
    when upsampling (see `max_data_age_in_periods`). Until it is known, samples
    are not removed from the resampling buffer, except when it is full.

    The number of received samples must also be at least the number of seconds
    in `max_data_age_in_periods` resampling periods. For example, with a
    resampling period of 60 seconds and a `max_data_age_in_periods` of 3, the
    input sampling period is estimated after receiving at least 180 samples (see
    [frequenz-sdk-python#1497](https://github.com/frequenz-floss/frequenz-sdk-python/issues/1497)).

    It must be at least 1 and at most `max_buffer_len`.
    """

    warn_buffer_len: int = DEFAULT_BUFFER_LEN_WARN
    """The number of samples above which a resampling buffer logs a warning.

    Old samples are removed from the buffer when resampling, so it only holds
    the samples that are still relevant and the ones received since. If it
    holds more samples than this value after removing old ones, a warning is
    logged once, so buffers don't grow too big inadvertently.

    It must be at least 1 and at most `max_buffer_len`.
    """

    max_buffer_len: int = DEFAULT_BUFFER_LEN_MAX
    """The maximum number of samples a resampling buffer can hold.

    When the buffer is full, the oldest sample is dropped when a new one is
    added, even if it would be needed to resample. An error is logged once if
    samples needed to resample may have been dropped.

    It must be bigger than `warn_buffer_len`.
    """

    align_to: datetime | None = UNIX_EPOCH
    """The time to align the resampling period to.

    The resampling period will be aligned to this time, so the first resampled
    sample will be at the first multiple of `resampling_period` starting from
    `align_to`. It must be an aware datetime and can be in the future too.

    If `align_to` is `None`, the resampling period will be aligned to the
    time the resampler is created.
    """

    tick_delay: timedelta = field(default=timedelta(0), kw_only=True)
    """Delay before processing each resampling tick.

    This delays when resampling computation happens, while keeping the
    resampling windows aligned to the original timer tick boundaries. This
    delay is only a time-based buffer, not a strict synchronization mechanism.

    Warning:
        This is an experimental option and may be changed or deprecated in
        the future.

    Example:
        This can be used in cascaded resampling setups to reduce timing races
        where downstream windows are processed before upstream resampled values
        are emitted.

    It must be non-negative and smaller than `resampling_period`.
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
        if self.tick_delay < timedelta(0):
            raise ValueError(f"tick_delay ({self.tick_delay}) should be non-negative")
        if self.tick_delay >= self.resampling_period:
            raise ValueError(
                f"tick_delay ({self.tick_delay}) should be smaller than "
                f"resampling_period ({self.resampling_period})"
            )


@deprecated(
    "frequenz.sdk.timeseries.ResamplingFunction2 is deprecated. "
    "Use frequenz.sdk.timeseries.ResamplingFunction3 instead, which receives a "
    "resampling context instead of the configuration."
)
class ResamplingFunction2(Protocol):
    """Combine multiple samples into a new one.

    Deprecated:
        Deprecated since v1.0.0-rc2217. Use
        [`ResamplingFunction3`][frequenz.sdk.timeseries.ResamplingFunction3] instead.

    A resampling function produces a new sample based on a list of pre-existing
    samples. It can do "upsampling" when the data rate of the `input_samples`
    period is smaller than the `resampling_period`, or "downsampling" if it is
    bigger.

    In general, a resampling window is the same as the `resampling_period`, and
    this function might receive input samples from multiple windows in the past to
    enable extrapolation, but no samples from the future (so the timestamp of the
    new sample that is going to be produced will always be bigger than the biggest
    timestamp in the input data).

    A [`ResamplingFunction3`][frequenz.sdk.timeseries.ResamplingFunction3] receives
    a [`ResamplingContext`][frequenz.sdk.timeseries.ResamplingContext] with the
    configuration and the window being resampled, instead of the configuration.
    """

    def __call__(
        self,
        input_samples: Sequence[tuple[datetime, float]],
        resampler_config: ResamplerConfig | ResamplerConfig2,
        source_properties: SourceProperties,
        /,
    ) -> float:
        """Call the resampling function.

        Args:
            input_samples: The sequence of pre-existing samples, where the first item is
                the timestamp of the sample, and the second is the value of the sample.
                The sequence must be non-empty.
            resampler_config: The configuration of the resampler calling this
                function.
            source_properties: The properties of the source being resampled.

        Returns:
            The value of new sample produced after the resampling.
        """
        ...  # pylint: disable=unnecessary-ellipsis


# Subclassing the deprecated ResamplerConfig warns, but users can't do anything
# about it, as it happens when the SDK is imported.
with ignoring_deprecations():

    @deprecated(
        "frequenz.sdk.timeseries.ResamplerConfig2 is deprecated. "
        "Use frequenz.sdk.timeseries.ResamplerConfig3 instead, but note it has "
        "different defaults and resampling function signature, so resampled "
        "values can change."
    )
    @dataclass(frozen=True)
    class ResamplerConfig2(ResamplerConfig):
        """Resampler configuration.

        Deprecated:
            Deprecated since v1.0.0-rc2217. Use
            [`ResamplerConfig3`][frequenz.sdk.timeseries.ResamplerConfig3] instead.

        A [`ResamplerConfig3`][frequenz.sdk.timeseries.ResamplerConfig3] has
        different defaults, and its resampling function receives a
        [`ResamplingContext`][frequenz.sdk.timeseries.ResamplingContext] instead
        of the configuration. To keep the results of this class, pass
        `max_data_age_in_periods=3.0`, `closed=WindowSide.RIGHT` and
        `label=WindowSide.RIGHT`, together with any `timer_config` and
        `tick_delay`.
        """

        resampling_period: timedelta
        """The resampling period.

        This is the time it passes between resampled data should be calculated.

        It must be a positive time span.
        """

        max_data_age_in_periods: float = 3.0
        """The maximum age a sample can have to be considered *relevant* for resampling.

        Expressed in number of periods, where period is the `resampling_period`
        if we are downsampling (resampling period bigger than the input period) or
        the *input sampling period* if we are upsampling (input period bigger than
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

        resampling_function: ResamplingFunction2 = (
            lambda samples, _, __: statistics.fmean(s[1] for s in samples)
        )
        """The resampling function.

        This function will be applied to the sequence of relevant samples at
        a given time. The result of the function is what is sent as the resampled
        value.
        """

        initial_buffer_len: int = DEFAULT_BUFFER_LEN_INIT
        """The number of samples to receive before estimating the input sampling period.

        The input sampling period is estimated once, at the first resampling after
        receiving enough samples, and decides how old the relevant samples can be
        when upsampling (see `max_data_age_in_periods`). Until it is known, samples
        are not removed from the resampling buffer, except when it is full.

        The number of received samples must also be at least the number of seconds
        in `max_data_age_in_periods` resampling periods. For example, with a
        resampling period of 60 seconds and a `max_data_age_in_periods` of 3, the
        input sampling period is estimated after receiving at least 180 samples (see
    [frequenz-sdk-python#1497](https://github.com/frequenz-floss/frequenz-sdk-python/issues/1497)).

        It must be at least 1 and at most `max_buffer_len`.
        """

        warn_buffer_len: int = DEFAULT_BUFFER_LEN_WARN
        """The number of samples above which a resampling buffer logs a warning.

        Old samples are removed from the buffer when resampling, so it only holds
        the samples that are still relevant and the ones received since. If it
        holds more samples than this value after removing old ones, a warning is
        logged once, so buffers don't grow too big inadvertently.

        It must be at least 1 and at most `max_buffer_len`.
        """

        max_buffer_len: int = DEFAULT_BUFFER_LEN_MAX
        """The maximum number of samples a resampling buffer can hold.

        When the buffer is full, the oldest sample is dropped when a new one is
        added, even if it would be needed to resample. An error is logged once if
        samples needed to resample may have been dropped.

        It must be bigger than `warn_buffer_len`.
        """

        align_to: datetime | None = field(default=None, init=False)
        """Deprecated: Use timer_config.align_to instead."""

        timer_config: WallClockTimerConfig | None = None
        """The custom configuration of the wall clock timer used to keep track of time.

        If not provided or `None`, a configuration will be created by passing the
        [`resampling_period`][frequenz.sdk.timeseries.ResamplerConfig2.resampling_period] to
        the [`from_interval()`][frequenz.sdk.timeseries.WallClockTimerConfig.from_interval]
        method.
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
                    f"max_data_age_in_periods ({self.max_data_age_in_periods}) "
                    "should be at least 1.0"
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
            if self.align_to is not None:
                raise ValueError(
                    f"align_to ({self.align_to}) must be specified via timer_config"
                )
            if self.tick_delay < timedelta(0):
                raise ValueError(
                    f"tick_delay ({self.tick_delay}) should be non-negative"
                )
            if self.tick_delay >= self.resampling_period:
                raise ValueError(
                    f"tick_delay ({self.tick_delay}) should be smaller than "
                    f"resampling_period ({self.resampling_period})"
                )


@dataclass(frozen=True, kw_only=True)
class ResamplingContext:
    """The information a resampling function gets besides the samples to resample.

    It has the resampling window being calculated, the configuration of the
    resampler and the properties of the timeseries being resampled.

    All resampling windows are exactly one
    [`resampling_period`][frequenz.sdk.timeseries.ResamplerConfig3.resampling_period]
    long, so the boundaries of previous windows can be calculated from the current
    one with [`window_bounds()`][frequenz.sdk.timeseries.ResamplingContext.window_bounds],
    and [`window_offset()`][frequenz.sdk.timeseries.ResamplingContext.window_offset]
    tells in which window a sample is.

    Example:
        ```python
        from collections.abc import Sequence
        from datetime import datetime

        from frequenz.sdk.timeseries import ResamplingContext


        def previous_window_size(
            input_samples: Sequence[tuple[datetime, float]],
            context: ResamplingContext,
            /,
        ) -> float:
            return float(
                sum(1 for t, _ in input_samples if context.window_offset(t) == -1)
            )
        ```
    """

    window_start: datetime
    """The start of the resampling window being calculated.

    This is the start of the window, whatever
    [`label`][frequenz.sdk.timeseries.ResamplerConfig3.label] is.
    """

    config: ResamplerConfig3
    """The configuration of the resampler."""

    source_properties: SourceProperties
    """The properties of the timeseries being resampled.

    They are the current properties of the timeseries, not a copy, so they keep
    changing after the resampling function returns. They must not be modified.
    """

    @property
    def window_end(self) -> datetime:
        """The end of the resampling window being calculated."""
        return self.window_start + self.config.resampling_period

    def window_bounds(self, offset: int = 0) -> tuple[datetime, datetime]:
        """Return the start and end of a window, relative to the current one.

        Which bound is included in the window depends on
        [`closed`][frequenz.sdk.timeseries.ResamplerConfig3.closed].

        Args:
            offset: The position of the window relative to the current one: `0`
                is the current window, `-1` the one before it, and so on.

        Returns:
            The start and end of the window.
        """
        start = self.window_start + self.config.resampling_period * offset
        return start, start + self.config.resampling_period

    def window_offset(self, timestamp: datetime) -> int:
        """Return the position of the window that contains a timestamp.

        A timestamp on the boundary between two windows belongs to the window on
        its [`closed`][frequenz.sdk.timeseries.ResamplerConfig3.closed] side.

        Args:
            timestamp: The timestamp to look for.

        Returns:
            The position of the window relative to the current one: `0` is the
                current window, `-1` the one before it, and so on.
        """
        elapsed = timestamp - self.window_start
        period = self.config.resampling_period
        match self.config.closed:
            case WindowSide.LEFT:
                return elapsed // period
            case WindowSide.RIGHT:
                # This is ceil(elapsed / period) - 1
                return -(-elapsed // period) - 1
            case unexpected:
                assert_never(unexpected)


class ResamplingFunction3(Protocol):
    """A function that combines multiple samples into a new one.

    A resampling function produces a new sample based on a list of pre-existing
    samples. It can do "upsampling" when the data rate of the `input_samples`
    period is smaller than the `resampling_period`, or "downsampling" if it is
    bigger.

    The function receives every sample that is not older than
    [`max_data_age_in_periods`][frequenz.sdk.timeseries.ResamplerConfig3.max_data_age_in_periods],
    so it can get samples from previous windows too, for example to extrapolate,
    but never samples from future windows.

    Warning: Limitation
        The resampler currently expects the samples of each timeseries to arrive
        in timestamp order. Samples that arrive out of order can be wrongly left
        out of or included in a window, and the function then receives them out
        of order. See
        [frequenz-sdk-python#47](https://github.com/frequenz-floss/frequenz-sdk-python/issues/47).

    Example:
        ```python
        from collections.abc import Sequence
        from datetime import datetime, timedelta

        from frequenz.sdk.timeseries import ResamplerConfig3, ResamplingContext


        def newest_value(
            input_samples: Sequence[tuple[datetime, float]],
            _context: ResamplingContext,
            /,
        ) -> float:
            return input_samples[-1][1]


        config = ResamplerConfig3(
            timedelta(seconds=1), resampling_function=newest_value
        )
        ```
    """

    def __call__(
        self,
        input_samples: Sequence[tuple[datetime, float]],
        context: ResamplingContext,
        /,
    ) -> FloatInt:
        """Call the resampling function.

        Args:
            input_samples: The samples to resample, as `(timestamp, value)`
                tuples sorted by timestamp. It is never empty.
            context: The resampling window being calculated, the configuration of
                the resampler and the properties of the timeseries being resampled.

        Returns:
            The value of the new sample.
        """
        ...  # pylint: disable=unnecessary-ellipsis


@dataclass(frozen=True)
class ResamplerConfig3:
    """A resampler configuration.

    The resampler produces one value per resampling window, with all the samples
    received for each timeseries that are not older than
    [`max_data_age_in_periods`][frequenz.sdk.timeseries.ResamplerConfig3.max_data_age_in_periods].

    With the default values, when downsampling, each resampled value is the mean of
    the samples in its own window only. When upsampling, it can also include older
    samples (see `max_data_age_in_periods`). Windows are closed on the left and
    values are labelled with the start of their window, so with a 15-minute
    resampling period, a value labelled 10:00 is the mean of the samples in
    `[10:00, 10:15)`. This matches how reporting data and
    [pandas](https://pandas.pydata.org/) label resampled data.

    The resampler always uses a
    [`WallClockTimer`][frequenz.sdk.timeseries.WallClockTimer] to know when a
    window ends. It can be configured with
    [`timer_config`][frequenz.sdk.timeseries.ResamplerConfig3.timer_config].

    Only `resampling_period` can be passed by position.

    Warning: Mixing window conventions
        Timeseries resampled with different
        [`label`][frequenz.sdk.timeseries.ResamplerConfig3.label] values are off by
        one period, and with different
        [`closed`][frequenz.sdk.timeseries.ResamplerConfig3.closed] values, samples
        exactly on a window boundary go to different windows. When they are
        combined, for example in a formula or when the output of a resampler is
        resampled again, values from adjacent windows are silently paired. Use the
        same values for all resamplers whose output is combined.

        When the output of a resampler is resampled again, both resamplers tick at
        the same time, so the second one can process a window before the first one
        sent its last value, whatever the window conventions are.
        [`tick_delay`][frequenz.sdk.timeseries.ResamplerConfig3.tick_delay] can
        reduce this.

    Warning: Limitation
        The resampler currently expects the samples of each timeseries to arrive
        in timestamp order. Samples that arrive out of order can be wrongly left
        out of or included in a window. See
        [frequenz-sdk-python#47](https://github.com/frequenz-floss/frequenz-sdk-python/issues/47).
    """

    resampling_period: timedelta
    """The resampling period.

    This is the time it passes between resampled data should be calculated, and
    the length of each resampling window.

    It must be a positive time span. Periods of a few microseconds also need a
    `timer_config`, as the default one can't be created for them.
    """

    _: KW_ONLY

    max_data_age_in_periods: FloatInt = 1.0
    """The maximum age of the samples passed to the resampling function, in periods.

    Every sample that is not older than this is passed to the resampling function,
    not only the samples in the current window. With the default resampling
    function, a mean, a value bigger than 1.0 makes each resampled value an
    average of several periods, which smooths and delays changes.

    The period is the `resampling_period` if we are downsampling (resampling
    period bigger than the input period) or the *input sampling period* if we are
    upsampling (input period bigger than the resampling period). So when
    upsampling, even with 1.0 the resampling function can receive samples older
    than the current window, and the default mean can include them.

    It must be at least 1.0, and small enough for that many resampling periods to
    fit in a [`timedelta`][datetime.timedelta].

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

    resampling_function: ResamplingFunction3 = lambda samples, _: statistics.fmean(
        s[1] for s in samples
    )
    """The resampling function.

    This function is called with all the samples that are not older than
    `max_data_age_in_periods`, and its result is sent as the resampled value.

    By default, it is the mean of all the samples it receives.
    """

    closed: WindowSide = WindowSide.LEFT
    """Indicates which side of the resampling window is closed.

    If `WindowSide.LEFT` (default), the resampling window is closed on the left
    side and open on the right, meaning it includes samples with timestamps
    within the range `[start, end)`, where `start` and `end` are the boundaries of
    the window.

    If `WindowSide.RIGHT`, the resampling window is closed on the right side and
    open on the left, meaning it includes samples with timestamps within the
    range `(start, end]`, where `start` and `end` are the boundaries of the
    window.
    """

    label: WindowSide = WindowSide.LEFT
    """Indicates the timestamp label of the resampled data.

    If `WindowSide.LEFT` (default), the timestamp of the resampled data
    corresponds to the left boundary (start) of the resampling window. If
    `WindowSide.RIGHT`, the timestamp corresponds to the right boundary (end) of
    the resampling window.

    Resampled values are sent at the end of their window, or later with
    `tick_delay`, so with `WindowSide.LEFT` the timestamp of a value is at least
    one period older than the time it is sent.
    """

    input_period_estimation_samples: int = DEFAULT_BUFFER_LEN_INIT
    """The number of samples to receive before estimating the input sampling period.

    The input sampling period is estimated once, at the first resampling after
    receiving enough samples, and decides how old the relevant samples can be
    when upsampling (see `max_data_age_in_periods`). Until it is known, samples
    are not removed from the resampling buffer, except when it is full.

    The number of received samples must also be at least the number of seconds
    in `max_data_age_in_periods` resampling periods. For example, with a
    resampling period of 60 seconds and a `max_data_age_in_periods` of 3, the
    input sampling period is estimated after receiving at least 180 samples (see
    [frequenz-sdk-python#1497](https://github.com/frequenz-floss/frequenz-sdk-python/issues/1497)).

    It must be at least 1 and at most `max_buffer_len`.
    """

    warn_buffer_len: int = DEFAULT_BUFFER_LEN_WARN
    """The number of samples above which a resampling buffer logs a warning.

    Old samples are removed from the buffer when resampling, so it only holds
    the samples that are still relevant and the ones received since. If it
    holds more samples than this value after removing old ones, a warning is
    logged once, so buffers don't grow too big inadvertently.

    It must be at least 1 and at most `max_buffer_len`.
    """

    max_buffer_len: int = DEFAULT_BUFFER_LEN_MAX
    """The maximum number of samples a resampling buffer can hold.

    When the buffer is full, the oldest sample is dropped when a new one is
    added, even if it would be needed to resample. An error is logged once if
    samples needed to resample may have been dropped.

    It must be bigger than `warn_buffer_len`.
    """

    timer_config: WallClockTimerConfig | None = None
    """The custom configuration of the wall clock timer used to keep track of time.

    If not provided or `None`, a configuration will be created by passing the
    [`resampling_period`][frequenz.sdk.timeseries.ResamplerConfig3.resampling_period]
    to the [`from_interval()`][frequenz.sdk.timeseries.WallClockTimerConfig.from_interval]
    method.
    """

    tick_delay: timedelta = timedelta(0)
    """Delay before processing each resampling tick.

    This delays when resampling computation happens, while keeping the
    resampling windows aligned to the original timer tick boundaries. This
    delay is only a time-based buffer, not a strict synchronization mechanism.

    Warning:
        This is an experimental option and may be changed or deprecated in
        the future.

    Example:
        This can be used in cascaded resampling setups to reduce timing races
        where downstream windows are processed before upstream resampled values
        are emitted.

    It must be non-negative and smaller than `resampling_period`.
    """

    def __post_init__(self) -> None:
        """Check that config values are valid.

        Raises:
            ValueError: If any value is out of range.
        """
        if self.resampling_period <= timedelta(0):
            raise ValueError(
                f"resampling_period ({self.resampling_period}) must be positive"
            )
        if self.timer_config is None:
            # The resampler creates the default timer configuration like this.
            try:
                _ = WallClockTimerConfig.from_interval(self.resampling_period)
            except ValueError as exc:
                raise ValueError(
                    f"resampling_period ({self.resampling_period}) is too small for "
                    "the default timer configuration, pass a timer_config"
                ) from exc
        try:
            # This also fails for values that are not finite.
            _ = self.resampling_period * self.max_data_age_in_periods
        except (OverflowError, ValueError) as exc:
            raise ValueError(
                f"max_data_age_in_periods ({self.max_data_age_in_periods}) must be a "
                "finite number, small enough for that many resampling periods to "
                "fit in a timedelta"
            ) from exc
        if self.max_data_age_in_periods < 1.0:
            raise ValueError(
                f"max_data_age_in_periods ({self.max_data_age_in_periods}) "
                "must be at least 1.0"
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
        if self.input_period_estimation_samples < 1:
            raise ValueError(
                "input_period_estimation_samples "
                f"({self.input_period_estimation_samples}) should be at least 1"
            )
        if self.input_period_estimation_samples > self.max_buffer_len:
            raise ValueError(
                "input_period_estimation_samples "
                f"({self.input_period_estimation_samples}) is bigger than "
                f"max_buffer_len ({self.max_buffer_len}), use a smaller "
                "input_period_estimation_samples or a bigger max_buffer_len"
            )
        if self.input_period_estimation_samples > self.warn_buffer_len:
            _logger.warning(
                "input_period_estimation_samples (%s) is bigger than "
                "warn_buffer_len (%s)",
                self.input_period_estimation_samples,
                self.warn_buffer_len,
            )
        if self.tick_delay < timedelta(0):
            raise ValueError(f"tick_delay ({self.tick_delay}) should be non-negative")
        if self.tick_delay >= self.resampling_period:
            raise ValueError(
                f"tick_delay ({self.tick_delay}) should be smaller than "
                f"resampling_period ({self.resampling_period})"
            )
