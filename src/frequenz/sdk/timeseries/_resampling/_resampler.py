# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries resampler."""

from __future__ import annotations

import asyncio
import itertools
import logging
import math
from bisect import bisect
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import assert_never

from frequenz.channels.timer import Timer, TriggerAllMissed, _to_microseconds
from frequenz.quantities import Quantity

from ..._internal._asyncio import cancel_and_await
from .._base_types import Sample
from ._base_types import Sink, Source, SourceProperties
from ._config import ResamplerConfig, ResamplerConfig2
from ._exceptions import ResamplingError, SourceStoppedError
from ._wall_clock_timer import TickInfo, WallClockTimer

_logger = logging.getLogger(__name__)


class Resampler:
    """A timeseries resampler.

    In general timeseries [`Source`][frequenz.sdk.timeseries.Source]s don't
    necessarily come at periodic intervals. You can use this class to normalize
    timeseries to produce `Sample`s at regular periodic intervals.

    This class uses
    a [`ResamplingFunction`][frequenz.sdk.timeseries._resampling.ResamplingFunction]
    to produce a new sample from samples received in the past. If there are no
    samples coming to a resampled timeseries for a while, eventually the
    `Resampler` will produce `Sample`s with `None` as value, meaning there is
    no way to produce meaningful samples with the available data.
    """

    def __init__(self, config: ResamplerConfig) -> None:
        """Initialize an instance.

        Args:
            config: The configuration for the resampler. If a `ResamplerConfig2` is
                provided, the resampler will use a
                [`WallClockTimer`][frequenz.sdk.timeseries.WallClockTimer] instead of a
                [`Timer`][frequenz.channels.timer.Timer].
        """
        self._config = config
        """The configuration for this resampler."""

        self._resamplers: dict[Source, _StreamingHelper] = {}
        """A mapping between sources and the streaming helper handling that source."""

        self._timer: Timer | WallClockTimer
        """The timer used to trigger the resampling windows."""

        if isinstance(config, ResamplerConfig2):
            self._timer = WallClockTimer(config.resampling_period, config.timer_config)
            return

        window_end, start_delay_time = self._calculate_window_end()
        self._window_end: datetime = window_end
        """The time in which the current window ends.

        This is used to make sure every resampling window is generated at
        precise times. We can't rely on the timer timestamp because timers will
        never fire at the exact requested time, so if we don't use a precise
        time for the end of the window, the resampling windows we produce will
        have different sizes.

        The window end will also be aligned to the `config.align_to` time, so
        the window end is deterministic.
        """

        self._timer = Timer(config.resampling_period, TriggerAllMissed())
        """The timer used to trigger the resampling windows."""

        # Hack to align the timer, this should be implemented in the Timer class
        self._timer._next_tick_time = _to_microseconds(
            timedelta(seconds=asyncio.get_running_loop().time())
            + config.resampling_period
            + start_delay_time
        )  # pylint: disable=protected-access

    @property
    def config(self) -> ResamplerConfig:
        """Get the resampler configuration.

        Returns:
            The resampler configuration.
        """
        return self._config

    def get_source_properties(self, source: Source) -> SourceProperties:
        """Get the properties of a timeseries source.

        Args:
            source: The source from which to get the properties.

        Returns:
            The timeseries source properties.
        """
        return self._resamplers[source].source_properties

    async def stop(self) -> None:
        """Cancel all receiving tasks."""
        await asyncio.gather(*[helper.stop() for helper in self._resamplers.values()])

    def add_timeseries(self, name: str, source: Source, sink: Sink) -> bool:
        """Start resampling a new timeseries.

        Args:
            name: The name of the timeseries (for logging purposes).
            source: The source of the timeseries to resample.
            sink: The sink to use to send the resampled data.

        Returns:
            `True` if the timeseries was added, `False` if the timeseries was
            not added because there already a timeseries using the provided
            receiver.
        """
        if source in self._resamplers:
            return False

        resampler = _StreamingHelper(
            _ResamplingHelper(name, self._config), source, sink
        )
        self._resamplers[source] = resampler
        return True

    def remove_timeseries(self, source: Source) -> bool:
        """Stop resampling the timeseries produced by `source`.

        Args:
            source: The source of the timeseries to stop resampling.

        Returns:
            `True` if the timeseries was removed, `False` if nothing was
                removed (because the a timeseries with that `source` wasn't
                being resampled).
        """
        try:
            del self._resamplers[source]
        except KeyError:
            return False
        return True

    async def resample(self, *, one_shot: bool = False) -> None:
        """Start resampling all known timeseries.

        This method will run forever unless there is an error while receiving
        from a source or sending to a sink (or `one_shot` is used).

        Args:
            one_shot: Wether the resampling should run only for one resampling
                period.

        Raises:
            ResamplingError: If some timeseries source or sink encounters any
                errors while receiving or sending samples. In this case the
                timer still runs and the timeseries will keep receiving data.
                The user should remove (and re-add if desired) the faulty
                timeseries from the resampler before calling this method
                again).
        """
        # We use a tolerance of 10% of the resampling period
        tolerance = timedelta(
            seconds=self._config.resampling_period.total_seconds() / 10.0
        )

        async for tick_info in self._timer:
            next_tick_time: datetime
            match tick_info:
                case TickInfo():  # WallClockTimer
                    next_tick_time = tick_info.expected_tick_time

                case timedelta() as drift:  # Timer (monotonic)
                    next_tick_time = self._window_end

                    if drift > tolerance:
                        _logger.warning(
                            "The resampling task woke up too late. Resampling should have "
                            "started at %s, but it started at %s (tolerance: %s, "
                            "difference: %s; resampling period: %s)",
                            self._window_end,
                            datetime.now(tz=timezone.utc),
                            tolerance,
                            tick_info,
                            self._config.resampling_period,
                        )

                    self._window_end += self._config.resampling_period

                case unexpected:
                    assert_never(unexpected)

            # We need to make a copy here because we need to match the results to the
            # current resamplers, and since we await here, new resamplers could be added
            # or removed from the dict while we awaiting the resampling, which would
            # cause the results to be out of sync.
            resampler_sources = list(self._resamplers)
            results = await asyncio.gather(
                *[r.resample(next_tick_time) for r in self._resamplers.values()],
                return_exceptions=True,
            )

            exceptions = {
                source: result
                for source, result in zip(resampler_sources, results)
                # CancelledError inherits from BaseException, but we don't want
                # to catch *all* BaseExceptions here.
                if isinstance(result, (Exception, asyncio.CancelledError))
            }
            if exceptions:
                raise ResamplingError(exceptions)
            if one_shot:
                break

    def _calculate_window_end(self) -> tuple[datetime, timedelta]:
        """Calculate the end of the current resampling window.

        The calculated resampling window end is a multiple of
        `self._config.resampling_period` starting at `self._config.align_to`.

        if `self._config.align_to` is `None`, the current time is used.

        If the current time is not aligned to `self._config.resampling_period`, then
        the end of the current resampling window will be more than one period away, to
        make sure to have some time to collect samples if the misalignment is too big.

        Returns:
            A tuple with the end of the current resampling window aligned to
                `self._config.align_to` as the first item and the time we need to
                delay the timer start to make sure it is also aligned.
        """
        now = datetime.now(timezone.utc)
        period = self._config.resampling_period
        align_to = self._config.align_to

        if align_to is None:
            return (now + period, timedelta(0))

        elapsed = (now - align_to) % period

        # If we are already in sync, we don't need to add an extra period
        if not elapsed:
            return (now + period, timedelta(0))

        return (
            # We add an extra period when it is not aligned to make sure we collected
            # enough samples before the first resampling, otherwise the initial window
            # to collect samples could be too small.
            now + period * 2 - elapsed,
            period - elapsed if elapsed else timedelta(0),
        )


class _ResamplingHelper:
    """Keeps track of *relevant* samples to pass them to the resampling function.

    Samples are stored in an internal ring buffer. All collected samples that
    are newer than `max(resampling_period, input_period)
    * max_data_age_in_periods` are considered *relevant* and are passed
    to the provided `resampling_function` when calling the `resample()` method.
    All older samples are discarded.
    """

    def __init__(self, name: str, config: ResamplerConfig) -> None:
        """Initialize an instance.

        Args:
            name: The name of this resampler helper (for logging purposes).
            config: The configuration for this resampler helper.
        """
        self._name = name
        self._config = config
        self._buffer: deque[tuple[datetime, float]] = deque(
            maxlen=config.initial_buffer_len
        )
        self._source_properties: SourceProperties = SourceProperties()

    @property
    def source_properties(self) -> SourceProperties:
        """Return the properties of the source.

        Returns:
            The properties of the source.
        """
        return self._source_properties

    def add_sample(self, sample: tuple[datetime, float]) -> None:
        """Add a new sample to the internal buffer.

        Args:
            sample: The sample to be added to the buffer.
        """
        self._buffer.append(sample)
        if self._source_properties.sampling_start is None:
            self._source_properties.sampling_start = sample[0]
        self._source_properties.received_samples += 1

    def _update_source_sample_period(self, now: datetime) -> bool:
        """Update the source sample period.

        Args:
            now: The datetime in which this update happens.

        Returns:
            Whether the source sample period was changed (was really updated).
        """
        assert (
            self._buffer.maxlen is not None and self._buffer.maxlen > 0
        ), "We need a maxlen of at least 1 to update the sample period"

        config = self._config
        props = self._source_properties

        # We only update it if we didn't before and we have enough data
        if (
            props.sampling_period is not None
            or props.sampling_start is None
            or props.received_samples
            < config.resampling_period.total_seconds() * config.max_data_age_in_periods
            or len(self._buffer) < self._buffer.maxlen
            # There might be a race between the first sample being received and
            # this function being called
            or now <= props.sampling_start
        ):
            return False

        samples_time_delta = now - props.sampling_start
        props.sampling_period = timedelta(
            seconds=samples_time_delta.total_seconds() / props.received_samples
        )

        _logger.debug(
            "New input sampling period calculated for %r: %ss",
            self._name,
            props.sampling_period,
        )
        return True

    def _update_buffer_len(self) -> bool:
        """Update the length of the buffer based on the source properties.

        Returns:
            Whether the buffer length was changed (was really updated).
        """
        # To make type checking happy
        assert self._buffer.maxlen is not None
        assert self._source_properties.sampling_period is not None

        input_sampling_period = self._source_properties.sampling_period

        config = self._config

        new_buffer_len = math.ceil(
            # If we are upsampling, one sample could be enough for
            # back-filling, but we store max_data_age_in_periods for input
            # periods, so resampling functions can do more complex
            # inter/extrapolation if they need to.
            (input_sampling_period.total_seconds() * config.max_data_age_in_periods)
            if input_sampling_period > config.resampling_period
            # If we are downsampling, we want a buffer that can hold
            # max_data_age_in_periods * resampling_period of data, and we one
            # sample every input_sampling_period.
            else (
                config.resampling_period.total_seconds()
                / input_sampling_period.total_seconds()
                * config.max_data_age_in_periods
            )
        )

        new_buffer_len = max(1, new_buffer_len)
        if new_buffer_len > config.max_buffer_len:
            _logger.error(
                "The new buffer length (%s) for timeseries %s is too big, using %s instead",
                new_buffer_len,
                self._name,
                config.max_buffer_len,
            )
            new_buffer_len = config.max_buffer_len
        elif new_buffer_len > config.warn_buffer_len:
            _logger.warning(
                "The new buffer length (%s) for timeseries %s bigger than %s",
                new_buffer_len,
                self._name,
                config.warn_buffer_len,
            )

        if new_buffer_len == self._buffer.maxlen:
            return False

        _logger.debug(
            "New buffer length calculated for %r: %s",
            self._name,
            new_buffer_len,
        )

        self._buffer = deque(self._buffer, maxlen=new_buffer_len)

        return True

    def resample(self, timestamp: datetime) -> Sample[Quantity]:
        """Generate a new sample based on all the current *relevant* samples.

        Args:
            timestamp: The timestamp to be used to calculate the new sample.

        Returns:
            A new sample generated by calling the resampling function with all
                the current *relevant* samples in the internal buffer, if any.
                If there are no *relevant* samples, then the new sample will
                have `None` as `value`.
        """
        if self._update_source_sample_period(timestamp):
            self._update_buffer_len()

        conf = self._config
        props = self._source_properties

        # To see which samples are relevant we need to consider if we are down
        # or upsampling.
        period = (
            max(
                conf.resampling_period,
                props.sampling_period,
            )
            if props.sampling_period is not None
            else conf.resampling_period
        )
        minimum_relevant_timestamp = timestamp - period * conf.max_data_age_in_periods

        min_index = bisect(
            self._buffer,
            minimum_relevant_timestamp,
            key=lambda s: s[0],
        )
        max_index = bisect(self._buffer, timestamp, key=lambda s: s[0])
        # Using itertools for slicing doesn't look very efficient, but
        # experiments with a custom (ring) buffer that can slice showed that
        # it is not that bad. See:
        # https://github.com/frequenz-floss/frequenz-sdk-python/pull/130
        # So if we need more performance beyond this point, we probably need to
        # resort to some C (or similar) implementation.
        relevant_samples = list(itertools.islice(self._buffer, min_index, max_index))
        if not relevant_samples:
            self._log_no_relevant_samples(minimum_relevant_timestamp, timestamp)

        value = (
            conf.resampling_function(relevant_samples, conf, props)
            if relevant_samples
            else None
        )
        return Sample(timestamp, None if value is None else Quantity(value))

    def _log_no_relevant_samples(
        self, minimum_relevant_timestamp: datetime, timestamp: datetime
    ) -> None:
        """Log that no relevant samples were found.

        Args:
            minimum_relevant_timestamp: Minimum timestamp that was requested
            timestamp: Timestamp that was requested
        """
        if not _logger.isEnabledFor(logging.WARNING):
            return

        if self._buffer:
            buffer_info = (
                f"{self._buffer[0][0]} - "
                f"{self._buffer[-1][0]} ({len(self._buffer)} samples)"
            )
        else:
            buffer_info = "Empty"

        _logger.warning(
            "No relevant samples found for: %s\n  Requested: %s - %s\n     Buffer: %s",
            self._name,
            minimum_relevant_timestamp,
            timestamp,
            buffer_info,
        )


class _StreamingHelper:
    """Resample data coming from a source, sending the results to a sink."""

    def __init__(
        self,
        helper: _ResamplingHelper,
        source: Source,
        sink: Sink,
    ) -> None:
        """Initialize an instance.

        Args:
            helper: The helper instance to use to resample incoming data.
            source: The source to use to get the samples to be resampled.
            sink: The sink to use to send the resampled data.
        """
        self._helper: _ResamplingHelper = helper
        self._source: Source = source
        self._sink: Sink = sink
        self._receiving_task: asyncio.Task[None] = asyncio.create_task(
            self._receive_samples()
        )

    @property
    def source_properties(self) -> SourceProperties:
        """Get the source properties.

        Returns:
            The source properties.
        """
        return self._helper.source_properties

    async def stop(self) -> None:
        """Cancel the receiving task."""
        await cancel_and_await(self._receiving_task)

    async def _receive_samples(self) -> None:
        """Pass received samples to the helper.

        This method keeps running until the source stops (or fails with an
        error).
        """
        async for sample in self._source:
            if sample.value is not None and not sample.value.isnan():
                self._helper.add_sample((sample.timestamp, sample.value.base_value))

    # We need the noqa because pydoclint can't figure out that `recv_exception` is an
    # `Exception` instance.
    async def resample(self, timestamp: datetime) -> None:  # noqa: DOC503
        """Calculate a new sample for the passed `timestamp` and send it.

        The helper is used to calculate the new sample and the sender is used
        to send it.

        Args:
            timestamp: The timestamp to be used to calculate the new sample.

        Raises:
            SourceStoppedError: If the source stopped sending samples.
            Exception: if there was any error while receiving from the source
                or sending to the sink.

                If the error was in the source, then this helper will stop
                working, as the internal task to receive samples will stop due
                to the exception. Any subsequent call to `resample()` will keep
                raising the same exception.

                If the error is in the sink, the receiving part will continue
                working while this helper is alive.
        """
        if self._receiving_task.done():
            if recv_exception := self._receiving_task.exception():
                raise recv_exception
            raise SourceStoppedError(self._source)

        await self._sink(self._helper.resample(timestamp))
