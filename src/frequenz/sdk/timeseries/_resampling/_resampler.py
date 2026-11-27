# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries resampler."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import assert_never

from frequenz.channels.timer import MonotonicTimer, TriggerAllMissed, _to_microseconds
from frequenz.core.typing import FloatInt
from frequenz.quantities import Quantity

from ..._internal._asyncio import cancel_and_await
from .._base_types import Sample
from ._base_types import Sink, Source, SourceProperties
from ._buffer import _ResamplingBuffer
from ._config import (
    ResamplerConfig,
    ResamplerConfig2,
    ResamplerConfig3,
    ResamplingContext,
    WindowSide,
)
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

    Warning: Limitation
        The resampler currently expects the samples of each timeseries to arrive
        in timestamp order. Samples that arrive out of order can be wrongly left
        out of or included in a window. See
        [frequenz-sdk-python#47](https://github.com/frequenz-floss/frequenz-sdk-python/issues/47).
    """

    def __init__(
        self, config: ResamplerConfig3 | ResamplerConfig2 | ResamplerConfig
    ) -> None:
        """Initialize an instance.

        Args:
            config: The configuration for the resampler. If a `ResamplerConfig3` or
                a `ResamplerConfig2` is provided, the resampler will use a
                [`WallClockTimer`][frequenz.sdk.timeseries.WallClockTimer] instead of a
                [`MonotonicTimer`][frequenz.channels.timer.MonotonicTimer].
        """
        self._config = config
        """The configuration for this resampler."""

        self._resamplers: dict[Source, _StreamingHelper] = {}
        """A mapping between sources and the streaming helper handling that source."""

        self._timer: MonotonicTimer | WallClockTimer
        """The timer used to trigger the resampling windows."""

        # ResamplerConfig2 is a subclass of ResamplerConfig, so it has to be
        # matched first.
        match config:
            case ResamplerConfig3() | ResamplerConfig2():
                self._timer = WallClockTimer(
                    config.resampling_period, config.timer_config
                )
                return
            case ResamplerConfig():
                pass
            case unexpected:
                assert_never(unexpected)

        window_end, start_delay_time = self._calculate_window_end(config)
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

        self._timer = MonotonicTimer(config.resampling_period, TriggerAllMissed())
        """The timer used to trigger the resampling windows."""

        # Align the timer; MonotonicTimer should align its own ticks.
        self._timer._next_tick_time = _to_microseconds(
            timedelta(seconds=asyncio.get_running_loop().time())
            + config.resampling_period
            + start_delay_time
        )  # pylint: disable=protected-access

    @property
    def config(self) -> ResamplerConfig3 | ResamplerConfig2 | ResamplerConfig:
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

                case timedelta() as drift:  # MonotonicTimer
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

            # Delay processing to let upstream cascaded resamplers emit their
            # boundary samples first; window boundaries still use next_tick_time.
            if self._config.tick_delay:
                await asyncio.sleep(self._config.tick_delay.total_seconds())

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

    def _calculate_window_end(
        self, config: ResamplerConfig
    ) -> tuple[datetime, timedelta]:
        """Calculate the end of the current resampling window.

        The calculated resampling window end is a multiple of
        `config.resampling_period` starting at `config.align_to`.

        if `config.align_to` is `None`, the current time is used.

        If the current time is not aligned to `config.resampling_period`, then
        the end of the current resampling window will be more than one period away, to
        make sure to have some time to collect samples if the misalignment is too big.

        Args:
            config: The configuration for the resampler.

        Returns:
            A tuple with the end of the current resampling window aligned to
                `config.align_to` as the first item and the time we need to
                delay the timer start to make sure it is also aligned.
        """
        now = datetime.now(timezone.utc)
        period = config.resampling_period
        align_to = config.align_to

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

    Samples are stored in an internal buffer. All collected samples that
    are newer than `max(resampling_period, input_period)
    * max_data_age_in_periods` are considered *relevant* and are passed
    to the provided `resampling_function` when calling the `resample()` method.

    Once the input period is known, older samples are removed from the buffer
    when resampling, as they won't be relevant for any later window either.
    Before that, the look-back can still grow when the input period is
    estimated, so samples are kept.
    """

    def __init__(
        self, name: str, config: ResamplerConfig3 | ResamplerConfig2 | ResamplerConfig
    ) -> None:
        """Initialize an instance.

        Args:
            name: The name of this resampler helper (for logging purposes).
            config: The configuration for this resampler helper.
        """
        self._name = name
        self._config = config
        self._source_properties: SourceProperties = SourceProperties()
        self._buffer = _ResamplingBuffer(
            name,
            max_len=config.max_buffer_len,
            warn_len=config.warn_buffer_len,
            source_properties=self._source_properties,
        )
        # The buffer's method is used directly, as adding samples must be as fast
        # as possible.
        self.add_sample: Callable[[tuple[datetime, float]], None] = self._buffer.add
        """Add a new sample to the buffer."""
        self._estimation_samples: int
        """The number of samples to receive before estimating the input period."""
        match config:
            case ResamplerConfig3():
                self._estimation_samples = config.input_period_estimation_samples
            case ResamplerConfig():
                self._estimation_samples = config.initial_buffer_len
            case unexpected:
                assert_never(unexpected)

    @property
    def source_properties(self) -> SourceProperties:
        """Return the properties of the source.

        Returns:
            The properties of the source.
        """
        return self._source_properties

    def _update_source_sample_period(self, now: datetime) -> bool:
        """Update the source sample period.

        Args:
            now: The datetime in which this update happens.

        Returns:
            Whether the source sample period was changed (was really updated).
        """
        config = self._config
        props = self._source_properties

        # We only update it if we didn't before and we have enough data
        if (
            props.sampling_period is not None
            or props.sampling_start is None
            or props.received_samples
            < config.resampling_period.total_seconds() * config.max_data_age_in_periods
            or props.received_samples < self._estimation_samples
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

    def resample(self, timestamp: datetime) -> Sample[Quantity]:
        """Generate a new sample based on all the current *relevant* samples.

        Args:
            timestamp: The reference timestamp for the resampling process. This
                timestamp indicates the end of the resampling period.

        Returns:
            A new sample generated by calling the resampling function with all
                the current *relevant* samples in the internal buffer, if any.
                If there are no *relevant* samples, then the new sample will
                have `None` as `value`.
        """
        self._update_source_sample_period(timestamp)

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
        try:
            minimum_relevant_timestamp = (
                timestamp - period * conf.max_data_age_in_periods
            )
        except OverflowError:
            # The maximum data age goes back further than a datetime can, so all
            # samples are relevant.
            minimum_relevant_timestamp = datetime.min.replace(tzinfo=timezone.utc)

        # Samples older than the relevant ones won't be relevant for later windows
        # either, unless the input period is still unknown, as the look-back can
        # grow when it is estimated.
        relevant_samples = self._buffer.relevant_samples(
            minimum_relevant_timestamp,
            timestamp,
            conf.closed,
            remove_older=props.sampling_period is not None,
        )

        value = (
            self._call_resampling_function(relevant_samples, timestamp)
            if relevant_samples
            else None
        )

        match conf.label:
            case WindowSide.LEFT:
                timestamp -= conf.resampling_period
            case WindowSide.RIGHT:
                pass
            case unexpected:
                assert_never(unexpected)

        return Sample(timestamp, None if value is None else Quantity(value))

    def _call_resampling_function(
        self,
        relevant_samples: list[tuple[datetime, float]],
        timestamp: datetime,
    ) -> FloatInt:
        """Call the resampling function of the configuration.

        Args:
            relevant_samples: The samples to pass to the resampling function.
            timestamp: The end of the resampling window.

        Returns:
            The value returned by the resampling function.
        """
        conf = self._config
        match conf:
            case ResamplerConfig3():
                context = ResamplingContext(
                    window_start=timestamp - conf.resampling_period,
                    config=conf,
                    source_properties=self._source_properties,
                )
                return conf.resampling_function(relevant_samples, context)
            case ResamplerConfig():
                return conf.resampling_function(
                    relevant_samples, conf, self._source_properties
                )
            case unexpected:
                assert_never(unexpected)


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
        """Calculate a new sample using buffered samples up to the given `timestamp` and send it.

        The helper is used to calculate the new sample and the sender is used
        to send it.

        Args:
            timestamp: The timestamp up to which all buffered samples are
                considered for calculating the new sample.

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
