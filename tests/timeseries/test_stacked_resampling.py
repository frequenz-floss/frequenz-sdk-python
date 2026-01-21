# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Minimal demonstration of the stacked resampler timing issue.

ISSUE SUMMARY
=============
When stacking resamplers (resampling already-resampled data), the sample at each
window boundary is consistently dropped. This happens because both resamplers' timers
fire at the same moment, but the higher-level resampler processes BEFORE the lower-level
one has emitted its boundary sample.

Example from the original issue description:
- Microgrid is initialized with 1s resampling period
- These 1s values are then resampled into 15-minute values by user code
- The 15-minute window ends at 12:00, but the data point for 11:59:59-12:00:00
  is only emitted AFTER 12:00:00, so the 15-minute resampler cannot include it

IMPACT
======
- For a 15-minute window losing 1 second: ~0.1% data loss (arguably negligible)
- For shorter higher-level periods: more significant impact
- For resampling functions other than mean (e.g., sum, max): the error compounds

ROOT CAUSE
==========
When both resamplers fire at timestamp T:
1. Both timers wake up at time T
2. asyncio schedules both callbacks
3. The higher-level resampler runs first (or concurrently)
4. It looks for samples with timestamp <= T
5. The lower-level resampler hasn't emitted its T sample yet
6. Result: The T sample is missing from the higher-level window

WORKAROUND
==========
If the lower-level resampler runs and emits BEFORE the higher-level one processes,
and there's a small delay (`await asyncio.sleep(0)`) for the channel to deliver
the message, then the sample IS included. See `test_stacked_resampler_with_reversed_order`.

POTENTIAL FIXES
===============
1. Add a small configurable delay to the higher-level resampler
2. Have the resampler emit samples slightly BEFORE the window boundary
3. Use an "open interval" approach where samples at exactly window_end go to next window
4. Coordinate between stacked resamplers (dependency injection / explicit chaining)

TEST STRUCTURE
==============
- `test_stacked_resampler_boundary_sample_loss`: Shows the bug with explicit ordering
- `test_stacked_resampler_concurrent_timers`: Shows the bug with asyncio.gather (realistic)
- `test_stacked_resampler_with_reversed_order`: Shows that correct ordering + delay fixes it
"""

import asyncio
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Broadcast
from frequenz.quantities import Quantity

from frequenz.sdk.timeseries import (
    Resampler,
    ResamplerConfig,
    ResamplerConfig2,
    ResamplerStack,
    Sample,
)


@pytest.fixture(autouse=True)
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


@pytest.fixture
def fake_time() -> Iterator[time_machine.Coordinates]:
    """Replace real time with a time machine that doesn't automatically tick."""
    with time_machine.travel(0, tick=False) as traveller:
        yield traveller


async def _advance_time(fake_time: time_machine.Coordinates, seconds: float) -> None:
    """Advance both wall clock and event loop time."""
    await asyncio.sleep(seconds)
    fake_time.shift(seconds)


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_stacked_resampler_boundary_sample_loss(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
) -> None:
    """Demonstrate that the boundary sample is lost when stacking resamplers.

    Setup:
    - First resampler: 2-second windows
    - Second resampler: 4-second windows (should aggregate 2 samples from first)
    - Raw data: samples at t=1,2,3,4,5,6,7,8 with value = timestamp

    Timeline showing the bug:
    - t=2: First resampler emits sample (ts=2)
    - t=4: Second resampler's window 0-4 closes. It should see samples ts=2,4
           BUT first resampler hasn't emitted ts=4 yet!
           So second resampler only gets ts=2.
    - t=4: First resampler emits sample (ts=4) - too late!
    - t=6: First resampler emits sample (ts=6)
    - t=8: Same problem repeats...
    """
    first_level_outputs: list[Sample[Quantity]] = []
    second_level_input_samples: list[list[tuple[datetime, float]]] = []

    def tracking_resampling_function(
        samples: Sequence[tuple[datetime, float]],
        config: ResamplerConfig,
        props: object,
    ) -> float:
        """Track samples passed to resampling and return mean."""
        second_level_input_samples.append(list(samples))
        if not samples:
            return float("nan")
        return sum(v for _, v in samples) / len(samples)

    first_level_config = config_class(
        resampling_period=timedelta(seconds=2),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
    )
    first_resampler = Resampler(first_level_config)

    second_level_config = config_class(
        resampling_period=timedelta(seconds=4),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
        resampling_function=tracking_resampling_function,
    )
    second_resampler = Resampler(second_level_config)

    raw_chan = Broadcast[Sample[Quantity]](name="raw")
    intermediate_chan = Broadcast[Sample[Quantity]](name="intermediate")

    raw_sender = raw_chan.new_sender()
    intermediate_sender = intermediate_chan.new_sender()

    async def track_and_forward(sample: Sample[Quantity]) -> None:
        first_level_outputs.append(sample)
        await intermediate_sender.send(sample)

    first_resampler.add_timeseries(
        "first",
        raw_chan.new_receiver(),
        track_and_forward,
    )

    final_outputs: list[Sample[Quantity]] = []

    async def collect_final(sample: Sample[Quantity]) -> None:
        final_outputs.append(sample)

    second_resampler.add_timeseries(
        "second",
        intermediate_chan.new_receiver(),
        collect_final,
    )

    timestamp = datetime.now(timezone.utc)

    # Send raw samples
    for i in range(1, 9):
        sample_time = timestamp + timedelta(seconds=i)
        sample = Sample(sample_time, value=Quantity(float(i)))
        await raw_sender.send(sample)

    # t=2: First resampler fires
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=4: BOTH fire - second runs first (simulating the problem)
    await _advance_time(fake_time, 2.0)
    await second_resampler.resample(one_shot=True)
    await first_resampler.resample(one_shot=True)

    # t=6: First resampler fires
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=8: BOTH fire
    await _advance_time(fake_time, 2.0)
    await second_resampler.resample(one_shot=True)
    await first_resampler.resample(one_shot=True)

    await raw_chan.aclose()
    await intermediate_chan.aclose()
    await first_resampler.stop()
    await second_resampler.stop()

    # Verify the bug: each window should have 2 samples but only has 1
    assert len(second_level_input_samples) == 2
    for i, batch in enumerate(second_level_input_samples):
        assert len(batch) == 1, (
            f"Window {i + 1}: Expected 1 sample (bug), got {len(batch)}. "
            "If this fails with 2 samples, the bug might be fixed!"
        )


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_stacked_resampler_concurrent_timers(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
) -> None:
    """Show the bug with realistic concurrent timer execution via asyncio.gather."""
    second_level_input_samples: list[list[tuple[datetime, float]]] = []

    def tracking_resampling_function(
        samples: Sequence[tuple[datetime, float]],
        config: ResamplerConfig,
        props: object,
    ) -> float:
        second_level_input_samples.append(list(samples))
        if not samples:
            return float("nan")
        return sum(v for _, v in samples) / len(samples)

    first_level_config = config_class(
        resampling_period=timedelta(seconds=2),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
    )
    first_resampler = Resampler(first_level_config)

    second_level_config = config_class(
        resampling_period=timedelta(seconds=4),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
        resampling_function=tracking_resampling_function,
    )
    second_resampler = Resampler(second_level_config)

    raw_chan = Broadcast[Sample[Quantity]](name="raw")
    intermediate_chan = Broadcast[Sample[Quantity]](name="intermediate")

    raw_sender = raw_chan.new_sender()
    intermediate_sender = intermediate_chan.new_sender()

    async def forward_fix(sample: Sample[Quantity]) -> None:
        await intermediate_sender.send(sample)

    async def noop_sink_fix(sample: Sample[Quantity]) -> None:
        pass

    first_resampler.add_timeseries("first", raw_chan.new_receiver(), forward_fix)
    second_resampler.add_timeseries(
        "second",
        intermediate_chan.new_receiver(),
        noop_sink_fix,
    )

    timestamp = datetime.now(timezone.utc)
    for i in range(1, 9):
        await raw_sender.send(
            Sample(timestamp + timedelta(seconds=i), Quantity(float(i)))
        )

    # t=2
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=4: Both fire concurrently
    await _advance_time(fake_time, 2.0)
    await asyncio.gather(
        second_resampler.resample(one_shot=True),
        first_resampler.resample(one_shot=True),
    )

    # t=6
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=8: Both fire concurrently
    await _advance_time(fake_time, 2.0)
    await asyncio.gather(
        second_resampler.resample(one_shot=True),
        first_resampler.resample(one_shot=True),
    )

    await raw_chan.aclose()
    await intermediate_chan.aclose()
    await first_resampler.stop()
    await second_resampler.stop()

    # Verify: each window has 1 sample instead of expected 2
    total_samples = sum(len(batch) for batch in second_level_input_samples)
    assert total_samples == 2, f"Expected 2 total samples (bug), got {total_samples}"


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_stacked_resampler_with_sleep0_fix(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that adding `await asyncio.sleep(0)` in resample() fixes the issue.

    This test patches the Resampler.resample() method to add a yield point
    at the start of each tick, giving pending channel deliveries a chance
    to complete before the resampling function runs.

    NOTE: A single sleep(0) is NOT enough when using asyncio.gather because:
    - gather starts both coroutines
    - sleep(0) yields to other ready tasks
    - but the first resampler is also yielding/waiting, not ready

    We need multiple yields to ensure the first resampler completes.
    """
    second_level_input_samples: list[list[tuple[datetime, float]]] = []

    def tracking_resampling_function(
        samples: Sequence[tuple[datetime, float]],
        config: ResamplerConfig,
        props: object,
    ) -> float:
        second_level_input_samples.append(list(samples))
        if not samples:
            return float("nan")
        return sum(v for _, v in samples) / len(samples)

    first_level_config = config_class(
        resampling_period=timedelta(seconds=2),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
    )
    first_resampler = Resampler(first_level_config)

    second_level_config = config_class(
        resampling_period=timedelta(seconds=4),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
        resampling_function=tracking_resampling_function,
    )
    second_resampler = Resampler(second_level_config)

    raw_chan = Broadcast[Sample[Quantity]](name="raw")
    intermediate_chan = Broadcast[Sample[Quantity]](name="intermediate")

    raw_sender = raw_chan.new_sender()
    intermediate_sender = intermediate_chan.new_sender()

    async def forward_concurrent(sample: Sample[Quantity]) -> None:
        await intermediate_sender.send(sample)

    async def noop_sink_concurrent(sample: Sample[Quantity]) -> None:
        pass

    first_resampler.add_timeseries("first", raw_chan.new_receiver(), forward_concurrent)
    second_resampler.add_timeseries(
        "second",
        intermediate_chan.new_receiver(),
        noop_sink_concurrent,
    )

    timestamp = datetime.now(timezone.utc)
    for i in range(1, 9):
        await raw_sender.send(
            Sample(timestamp + timedelta(seconds=i), Quantity(float(i)))
        )

    # Store original resample method
    original_resample = Resampler.resample

    async def patched_resample_concurrent(
        self: Resampler, *, one_shot: bool = False
    ) -> None:
        """Patched resample that yields multiple times before processing."""
        # Multiple yields to let the first resampler complete and channel deliver
        for _ in range(5):  # Enough iterations to let everything settle
            await asyncio.sleep(0)
        # Call original
        await original_resample(self, one_shot=one_shot)

    # Apply the patch to second_resampler only
    second_resampler.resample = patched_resample_concurrent.__get__(
        second_resampler, Resampler
    )  # type: ignore[method-assign]

    # t=2
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=4: Both fire concurrently - but second_resampler now yields first
    await _advance_time(fake_time, 2.0)
    await asyncio.gather(
        second_resampler.resample(one_shot=True),
        first_resampler.resample(one_shot=True),
    )

    # t=6
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=8: Both fire concurrently
    await _advance_time(fake_time, 2.0)
    await asyncio.gather(
        second_resampler.resample(one_shot=True),
        first_resampler.resample(one_shot=True),
    )

    await raw_chan.aclose()
    await intermediate_chan.aclose()
    await first_resampler.stop()
    await second_resampler.stop()

    # With the sleep(0) fix, each window should have 2 samples
    print(f"\n=== RESULTS WITH sleep(0) FIX ===")
    for i, batch in enumerate(second_level_input_samples):
        print(
            f"Window {i + 1}: got {len(batch)} samples: {[t.second for t, v in batch]}"
        )

    # This should now pass with 2 samples per window
    for i, batch in enumerate(second_level_input_samples):
        assert len(batch) == 2, (
            f"Window {i + 1}: Expected 2 samples with sleep(0) fix, got {len(batch)}"
        )


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_stacked_resampler_with_reversed_order(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
) -> None:
    """Show that correct ordering + yield to event loop fixes the issue.

    This test demonstrates that if:
    1. The first-level resampler runs BEFORE the second-level one
    2. AND we yield to the event loop (await asyncio.sleep(0)) to let the
       channel deliver the message

    Then the boundary sample IS included correctly.
    """
    second_level_input_samples: list[list[tuple[datetime, float]]] = []

    def tracking_resampling_function(
        samples: Sequence[tuple[datetime, float]],
        config: ResamplerConfig,
        props: object,
    ) -> float:
        second_level_input_samples.append(list(samples))
        if not samples:
            return float("nan")
        return sum(v for _, v in samples) / len(samples)

    first_level_config = config_class(
        resampling_period=timedelta(seconds=2),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
    )
    first_resampler = Resampler(first_level_config)

    second_level_config = config_class(
        resampling_period=timedelta(seconds=4),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
        resampling_function=tracking_resampling_function,
    )
    second_resampler = Resampler(second_level_config)

    raw_chan = Broadcast[Sample[Quantity]](name="raw")
    intermediate_chan = Broadcast[Sample[Quantity]](name="intermediate")

    raw_sender = raw_chan.new_sender()
    intermediate_sender = intermediate_chan.new_sender()

    async def forward(sample: Sample[Quantity]) -> None:
        await intermediate_sender.send(sample)

    async def noop_sink2(sample: Sample[Quantity]) -> None:
        pass

    first_resampler.add_timeseries("first", raw_chan.new_receiver(), forward)
    second_resampler.add_timeseries(
        "second",
        intermediate_chan.new_receiver(),
        noop_sink2,
    )

    timestamp = datetime.now(timezone.utc)
    for i in range(1, 9):
        await raw_sender.send(
            Sample(timestamp + timedelta(seconds=i), Quantity(float(i)))
        )

    # t=2
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=4: First resampler runs first, then yield, then second resampler
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)
    await asyncio.sleep(0)  # Yield to let channel deliver the message
    await second_resampler.resample(one_shot=True)

    # t=6
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=8: Same pattern
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)
    await asyncio.sleep(0)
    await second_resampler.resample(one_shot=True)

    await raw_chan.aclose()
    await intermediate_chan.aclose()
    await first_resampler.stop()
    await second_resampler.stop()

    # With correct ordering, each window has 2 samples as expected
    for i, batch in enumerate(second_level_input_samples):
        assert len(batch) == 2, (
            f"Window {i + 1}: Expected 2 samples with correct ordering, got {len(batch)}"
        )


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampler_stack(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that ResamplerStack handles ordering correctly.

    ResamplerStack is the recommended solution for stacking resamplers.
    It ensures lower-level resamplers emit their samples before higher-level
    ones process, preventing boundary sample loss.
    """
    second_level_input_samples: list[list[tuple[datetime, float]]] = []

    def tracking_resampling_function(
        samples: Sequence[tuple[datetime, float]],
        config: ResamplerConfig,
        props: object,
    ) -> float:
        second_level_input_samples.append(list(samples))
        if not samples:
            return float("nan")
        return sum(v for _, v in samples) / len(samples)

    first_level_config = config_class(
        resampling_period=timedelta(seconds=2),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
    )
    first_resampler = Resampler(first_level_config)

    second_level_config = config_class(
        resampling_period=timedelta(seconds=4),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
        resampling_function=tracking_resampling_function,
    )
    second_resampler = Resampler(second_level_config)

    # Create a stack with correct ordering (lower-level first)
    stack = ResamplerStack([first_resampler, second_resampler])

    raw_chan = Broadcast[Sample[Quantity]](name="raw")
    intermediate_chan = Broadcast[Sample[Quantity]](name="intermediate")

    raw_sender = raw_chan.new_sender()
    intermediate_sender = intermediate_chan.new_sender()

    async def forward(sample: Sample[Quantity]) -> None:
        await intermediate_sender.send(sample)

    async def noop_sink(sample: Sample[Quantity]) -> None:
        pass

    first_resampler.add_timeseries("first", raw_chan.new_receiver(), forward)
    second_resampler.add_timeseries(
        "second",
        intermediate_chan.new_receiver(),
        noop_sink,
    )

    timestamp = datetime.now(timezone.utc)
    for i in range(1, 9):
        await raw_sender.send(
            Sample(timestamp + timedelta(seconds=i), Quantity(float(i)))
        )

    # t=2: Only first resampler should fire
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=4: Both fire - use the stack to handle ordering
    await _advance_time(fake_time, 2.0)
    await stack.resample(one_shot=True)

    # t=6: Only first resampler should fire
    await _advance_time(fake_time, 2.0)
    await first_resampler.resample(one_shot=True)

    # t=8: Both fire - use the stack
    await _advance_time(fake_time, 2.0)
    await stack.resample(one_shot=True)

    await raw_chan.aclose()
    await intermediate_chan.aclose()
    await stack.stop()

    # With ResamplerStack, each window should have 2 samples
    assert len(second_level_input_samples) == 2, (
        f"Expected 2 windows, got {len(second_level_input_samples)}"
    )
    for i, batch in enumerate(second_level_input_samples):
        assert len(batch) == 2, (
            f"Window {i + 1}: Expected 2 samples with ResamplerStack, got {len(batch)}"
        )


@pytest.mark.parametrize("config_class", [ResamplerConfig, ResamplerConfig2])
async def test_resampler_trigger(
    config_class: type[ResamplerConfig],
    fake_time: time_machine.Coordinates,
) -> None:
    """Test the Resampler.trigger() method for external timing control.

    The trigger() method allows external control of when resampling occurs,
    which is the foundation for ResamplerStack's continuous mode coordination.
    """
    second_level_input_samples: list[list[tuple[datetime, float]]] = []

    def tracking_resampling_function_trigger(
        samples: Sequence[tuple[datetime, float]],
        config: ResamplerConfig,
        props: object,
    ) -> float:
        second_level_input_samples.append(list(samples))
        if not samples:
            return float("nan")
        return sum(v for _, v in samples) / len(samples)

    first_level_config = config_class(
        resampling_period=timedelta(seconds=2),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
    )
    first_resampler = Resampler(first_level_config)

    second_level_config = config_class(
        resampling_period=timedelta(seconds=4),
        max_data_age_in_periods=1.0,
        initial_buffer_len=10,
        resampling_function=tracking_resampling_function_trigger,
    )
    second_resampler = Resampler(second_level_config)

    raw_chan = Broadcast[Sample[Quantity]](name="raw")
    intermediate_chan = Broadcast[Sample[Quantity]](name="intermediate")

    raw_sender = raw_chan.new_sender()
    intermediate_sender = intermediate_chan.new_sender()

    async def forward_trigger(sample: Sample[Quantity]) -> None:
        await intermediate_sender.send(sample)

    async def noop_sink_trigger(sample: Sample[Quantity]) -> None:
        pass

    first_resampler.add_timeseries("first", raw_chan.new_receiver(), forward_trigger)
    second_resampler.add_timeseries(
        "second",
        intermediate_chan.new_receiver(),
        noop_sink_trigger,
    )

    timestamp = datetime.now(timezone.utc)
    for i in range(1, 9):
        await raw_sender.send(
            Sample(timestamp + timedelta(seconds=i), Quantity(float(i)))
        )

    # Use trigger() to control timing externally
    # This is what ResamplerStack._run_continuous() does internally

    # t=2: Only first resampler fires
    await first_resampler.trigger(timestamp + timedelta(seconds=2))
    await asyncio.sleep(0)  # Let channel deliver

    # t=4: Both fire - trigger in order (first, then second)
    await first_resampler.trigger(timestamp + timedelta(seconds=4))
    await asyncio.sleep(0)
    await second_resampler.trigger(timestamp + timedelta(seconds=4))

    # t=6: Only first resampler fires
    await first_resampler.trigger(timestamp + timedelta(seconds=6))
    await asyncio.sleep(0)

    # t=8: Both fire
    await first_resampler.trigger(timestamp + timedelta(seconds=8))
    await asyncio.sleep(0)
    await second_resampler.trigger(timestamp + timedelta(seconds=8))

    await raw_chan.aclose()
    await intermediate_chan.aclose()
    await first_resampler.stop()
    await second_resampler.stop()

    # With proper ordering via trigger(), each window should have 2 samples
    assert len(second_level_input_samples) == 2, (
        f"Expected 2 windows, got {len(second_level_input_samples)}"
    )
    for i, batch in enumerate(second_level_input_samples):
        assert len(batch) == 2, f"Window {i + 1}: Expected 2 samples, got {len(batch)}"
