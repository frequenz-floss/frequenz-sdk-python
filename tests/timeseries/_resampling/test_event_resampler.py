# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the `EventResampler` class."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from frequenz.quantities import Quantity

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._resampling._config import ResamplerConfig
from frequenz.sdk.timeseries._resampling._event_resampler import EventResampler
from frequenz.sdk.timeseries._resampling._resampler import Resampler

# pylint: disable=protected-access


@dataclass
class ResamplerTestCase:
    """Data class for holding test case parameters for EventResampler tests."""

    align_to: datetime | None
    """Alignment point for windows. If None, windows are aligned to the first sample time."""

    first_window_end: datetime
    """Expected end time of the first window based on the configuration and start time."""


@pytest.fixture
def now() -> datetime:
    """Fixture providing a fixed current time for testing."""
    return datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)


@pytest.fixture(
    params=[
        ResamplerTestCase(
            align_to=None,
            first_window_end=datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc),
        ),
        ResamplerTestCase(
            align_to=datetime(1970, 1, 1, tzinfo=timezone.utc),
            first_window_end=datetime(2024, 1, 1, 12, 0, 10, tzinfo=timezone.utc),
        ),
    ],
    ids=["no_alignment", "with_alignment"],
)
def resampler_case(request: pytest.FixtureRequest) -> ResamplerTestCase:
    """Fixture for EventResampler test cases."""
    assert isinstance(request.param, ResamplerTestCase)
    return request.param


@pytest.fixture
def resampler_config(resampler_case: ResamplerTestCase) -> ResamplerConfig:
    """Create a basic resampler config for testing."""
    return ResamplerConfig(
        resampling_period=timedelta(seconds=10),
        max_data_age_in_periods=1,
        align_to=resampler_case.align_to,
    )


@pytest.fixture
def first_window_end(resampler_case: ResamplerTestCase) -> datetime:
    """Fixture providing the expected first window end time based on the test case."""
    return resampler_case.first_window_end


@patch("frequenz.sdk.timeseries._resampling._event_resampler.datetime")
@pytest.mark.asyncio
async def test_event_resampler_initialization(
    mock_datetime: AsyncMock,
    resampler_config: ResamplerConfig,
    now: datetime,
    first_window_end: datetime,
) -> None:
    """Event Resampler initializes without errors."""
    mock_datetime.now.return_value = now
    resampler = EventResampler(resampler_config)

    assert resampler.config == resampler_config
    assert len(resampler._resamplers) == 0
    assert resampler._window_end == first_window_end


@pytest.mark.asyncio
async def test_event_resampler_inherits_from_resampler(
    resampler_config: ResamplerConfig,
) -> None:
    """Event Resampler is a Resampler subclass."""
    resampler = EventResampler(resampler_config)
    assert isinstance(resampler, Resampler)
    assert hasattr(resampler, "add_timeseries")
    assert hasattr(resampler, "remove_timeseries")
    assert callable(resampler.add_timeseries)
    assert callable(resampler.remove_timeseries)


@patch("frequenz.sdk.timeseries._resampling._event_resampler.datetime")
@pytest.mark.asyncio
async def test_window_initialization(
    mock_datetime: AsyncMock,
    resampler_config: ResamplerConfig,
    now: datetime,
    first_window_end: datetime,
) -> None:
    """Window initializes correctly on first sample."""
    mock_datetime.now.return_value = now
    resampler = EventResampler(resampler_config)

    assert resampler._window_end == first_window_end

    sample = Sample(now, Quantity(42.0))
    await resampler._process_sample(sample)

    assert resampler._window_end == first_window_end


@patch("frequenz.sdk.timeseries._resampling._event_resampler.datetime")
@pytest.mark.asyncio
async def test_sample_before_first_window_boundary(
    mock_datetime: AsyncMock,
    resampler_config: ResamplerConfig,
    now: datetime,
    first_window_end: datetime,
) -> None:
    """Samples before window boundary don't trigger emit."""
    mock_datetime.now.return_value = now
    resampler = EventResampler(resampler_config)

    with patch.object(
        resampler, "_emit_window", new_callable=AsyncMock
    ) as mock_emit_window:
        # Sample 1
        sample1 = Sample(now + timedelta(seconds=1), Quantity(10.0))
        await resampler._process_sample(sample1)

        assert resampler._window_end == first_window_end
        mock_emit_window.assert_not_called()

        # Process sample still within first window
        sample2 = Sample(now + timedelta(seconds=3), Quantity(20.0))
        await resampler._process_sample(sample2)

        assert resampler._window_end == first_window_end
        mock_emit_window.assert_not_called()


@patch("frequenz.sdk.timeseries._resampling._event_resampler.datetime")
@pytest.mark.asyncio
async def test_sample_at_window_boundary_triggers_emit(
    mock_datetime: AsyncMock,
    resampler_config: ResamplerConfig,
    now: datetime,
    first_window_end: datetime,
) -> None:
    """Sample at window boundary triggers emit and opens new window."""
    mock_datetime.now.return_value = now
    resampler = EventResampler(resampler_config)

    with patch.object(
        resampler, "_emit_window", new_callable=AsyncMock
    ) as mock_emit_window:
        # Sample 1
        sample1 = Sample(now + timedelta(seconds=1), Quantity(10.0))
        await resampler._process_sample(sample1)

        assert resampler._window_end == first_window_end
        mock_emit_window.assert_not_called()

        # Sample 2 at boundary
        sample2 = Sample(now + timedelta(seconds=10), Quantity(20.0))
        await resampler._process_sample(sample2)

        mock_emit_window.assert_called_once_with(first_window_end)
        assert resampler._window_end == first_window_end + timedelta(seconds=10)


@patch("frequenz.sdk.timeseries._resampling._event_resampler.datetime")
@pytest.mark.asyncio
async def test_sample_after_window_boundary(
    mock_datetime: AsyncMock,
    resampler_config: ResamplerConfig,
    now: datetime,
    first_window_end: datetime,
) -> None:
    """Sample after window boundary triggers emit."""
    mock_datetime.now.return_value = now
    resampler = EventResampler(resampler_config)

    with patch.object(
        resampler, "_emit_window", new_callable=AsyncMock
    ) as mock_emit_window:
        # Sample 1
        sample1 = Sample(now + timedelta(seconds=1), Quantity(10.0))
        await resampler._process_sample(sample1)

        assert resampler._window_end == first_window_end
        mock_emit_window.assert_not_called()

        # Sample 2 at boundary
        sample2 = Sample(now + timedelta(seconds=11), Quantity(20.0))
        await resampler._process_sample(sample2)

        mock_emit_window.assert_called_once_with(first_window_end)
        assert resampler._window_end == first_window_end + timedelta(seconds=10)


@patch("frequenz.sdk.timeseries._resampling._event_resampler.datetime")
@pytest.mark.asyncio
async def test_sample_crossing_multiple_windows(
    mock_datetime: AsyncMock,
    resampler_config: ResamplerConfig,
    now: datetime,
    first_window_end: datetime,
) -> None:
    """Sample crossing multiple windows emits each one."""
    mock_datetime.now.return_value = now
    resampler = EventResampler(resampler_config)

    with patch.object(
        resampler, "_emit_window", new_callable=AsyncMock
    ) as mock_emit_window:
        # Sample 1 at 2s
        sample1 = Sample(now + timedelta(seconds=2), Quantity(10.0))
        await resampler._process_sample(sample1)
        mock_emit_window.assert_not_called()
        assert resampler._window_end == first_window_end

        # Sample 2 at 32s
        sample2 = Sample(now + timedelta(seconds=32), Quantity(20.0))
        await resampler._process_sample(sample2)
        assert mock_emit_window.call_count == 3
        assert resampler._window_end == first_window_end + timedelta(seconds=30)


@patch("frequenz.sdk.timeseries._resampling._event_resampler.datetime")
@pytest.mark.asyncio
async def test_window_alignment_maintained(
    mock_datetime: AsyncMock,
    resampler_config: ResamplerConfig,
    now: datetime,
    first_window_end: datetime,
) -> None:
    """Windows remain aligned when using simple addition."""
    mock_datetime.now.return_value = now
    resampler = EventResampler(resampler_config)
    send_sequence = [1, 15, 25, 35]  # Sample times in seconds

    with patch.object(
        resampler, "_emit_window", new_callable=AsyncMock
    ) as mock_emit_window:
        for offset in send_sequence:
            sample = Sample(now + timedelta(seconds=offset), Quantity(float(offset)))
            await resampler._process_sample(sample)

        for i, call_args in enumerate(mock_emit_window.call_args_list):
            window_end = call_args.args[0]  # Extract the first argument from the call
            expected = first_window_end + i * resampler_config.resampling_period
            assert window_end == expected


@patch("frequenz.sdk.timeseries._resampling._event_resampler.datetime")
@pytest.mark.asyncio
async def test_key_benefit_no_data_loss_at_boundaries(
    mock_datetime: AsyncMock,
    resampler_config: ResamplerConfig,
    now: datetime,
    first_window_end: datetime,
) -> None:
    """
    Key benefit: No data loss at window boundaries.

    This test demonstrates the main value of EventResampler compared
    to cascaded TimerResamplers: samples arriving at boundaries are
    never lost.
    """
    mock_datetime.now.return_value = now
    resampler = EventResampler(resampler_config)
    arriving_samples = [1.0, 5.0, 9.5, 10.0, 10.1, 15.0, 20.0, 20.5]

    with patch.object(
        resampler, "_emit_window", new_callable=AsyncMock
    ) as mock_emit_window:
        for i, sample_offset in enumerate(arriving_samples):
            sample = Sample(now + timedelta(seconds=sample_offset), Quantity(i))
            await resampler._process_sample(sample)

        assert mock_emit_window.call_count == 2
        assert mock_emit_window.call_args_list[0].args[0] == first_window_end
        assert mock_emit_window.call_args_list[1].args[
            0
        ] == first_window_end + timedelta(seconds=10)
        assert resampler._window_end == (first_window_end + timedelta(seconds=20))
