# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Event-driven resampler for cascaded resampling stages."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from frequenz.quantities import Quantity

from .._base_types import Sample
from ._base_types import Sink, Source
from ._config import ResamplerConfig
from ._resampler import Resampler, _ResamplingHelper, _StreamingHelper

_logger = logging.getLogger(__name__)


class EventResampler(Resampler):
    """Event-driven resampler for cascaded resampling stages.

    Unlike the standard Timer-based Resampler which uses fixed wall-clock
    intervals, EventResampler is triggered by incoming data. Windows are
    emitted when a sample arrives that falls outside the current window,
    not on a fixed timer schedule.

    Problem Solved:
    When cascading Timer-based resamplers (e.g., 1s → 10s) with
    align_to=UNIX_EPOCH, samples can be lost at window boundaries due to
    timing synchronization issues. EventResampler eliminates this by
    opening/closing windows based on actual data arrival.

    Important: This resampler is optimized for continuous data streams
    where samples arrive at regular or semi-regular intervals. It is not
    suitable for handling raw, irregular data directly from sources.

    Best Used:
    Stage 1: Timer-based Resampler (handles raw, irregular data)
    Stage 2+: Event-based Resampler (handles continuous data from Stage 1)

    Example:
        config = ResamplerConfig(
            resampling_period=timedelta(seconds=10),
            resampling_function=...,
        )
        resampler = EventResampler(config)
        resampler.add_timeseries("my_source", source, sink)
        await resampler.resample()

    Note: If a long gap occurs without incoming samples (no data for multiple periods),
    the corresponding windows will be emitted all at once when data resumes. This is
    acceptable for cascaded resampling since the input typically comes from another
    Resampler with guaranteed continuous output.
    """

    # pylint: disable=super-init-not-called
    def __init__(self, config: ResamplerConfig) -> None:
        """Initialize EventResampler.

        This does not call super().__init__() to avoid starting any timers

        Args:
            config: Resampler configuration
        """
        self._config = config
        """The configuration for this resampler."""

        self._resamplers: dict[Source, _StreamingHelper] = {}
        """A mapping between sources and the streaming helper handling that source."""

        window_end, _ = self._calculate_window_end()
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

        self._window_lock = asyncio.Lock()
        """Lock protecting access to `_window_end` during window state transitions."""

        self._sample_queue: asyncio.Queue[Sample[Quantity]] = asyncio.Queue()
        """Queue for samples awaiting processing. Filled by `_StreamingHelper` callbacks,
        consumed by the event loop in `resample()`.
        """

    # OVERRIDDEN: Register callback to receive samples asynchronously for
    # event-driven window management.
    def add_timeseries(self, name: str, source: Source, sink: Sink) -> bool:
        """Start resampling a new timeseries.

        Registers the timeseries and sets up a sample callback to enqueue
        incoming samples for event-driven processing.

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

        # Register the callback to receive samples from the streaming helper.
        resampler.register_sample_callback(self._enqueue_sample)

        self._resamplers[source] = resampler
        return True

    async def _enqueue_sample(self, sample: Sample[Quantity]) -> None:
        """Add a sample to the processing queue.

        Args:
            sample: The sample to enqueue.
        """
        await self._sample_queue.put(sample)

    # OVERRIDDEN: no warm-up period needed for event-driven sample accumulation.
    def _calculate_window_end(self) -> tuple[datetime, timedelta]:
        """Calculate the end of the first resampling window.

        Calculates the next multiple of resampling_period after the current time,
        respecting align_to configuration.

        Returns:
            A tuple (window_end, delay_time) where:
            - window_end: datetime when the first window should end
            - delay_time: always timedelta(0) for EventResampler
        """
        now = datetime.now(timezone.utc)
        period = self._config.resampling_period
        align_to = self._config.align_to

        if align_to is None:
            return (now + period, timedelta(0))

        elapsed = (now - align_to) % period

        return (
            (now + (period - elapsed), timedelta(0))
            if elapsed > timedelta(0)
            else (now, timedelta(0))
        )

    async def resample(self, *, one_shot: bool = False) -> None:
        """Start event-driven resampling.

        Processes incoming samples from the queue continuously. Windows are
        emitted when a sample arrives with a timestamp >= current window_end.
        This is in contrast to Timer-based resampling which emits windows at
        fixed intervals regardless of data arrival.

        Args:
            one_shot: If True, waits for the first window to be emitted, then exits.

        Raises:
            asyncio.CancelledError: If the task is cancelled.
        """
        try:
            while True:
                sample = await self._sample_queue.get()
                emmitted = await self._process_sample(sample)

                if one_shot and emmitted:
                    return

        except asyncio.CancelledError:
            _logger.info("EventResampler task cancelled")
            raise

    async def _process_sample(
        self,
        sample: Sample[Quantity],
    ) -> bool:
        """Process an incoming sample and manage window state.

        This method checks if the incoming sample falls outside the current
        window and emits completed windows as needed. Returns True if any
        windows were emitted.

        Args:
            sample: Incoming sample to process

        Returns:
            True if at least one window was emitted, False otherwise.
        """
        async with self._window_lock:
            emmitted = False
            while sample.timestamp >= self._window_end:
                _logger.debug(
                    "EventResampler: Sample at %s >= window end %s, closing window",
                    sample.timestamp,
                    self._window_end,
                )
                await self._emit_window(self._window_end)
                self._window_end += self._config.resampling_period
                emmitted = True
            return emmitted
