# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the resampling buffer."""

import logging
from datetime import datetime, timedelta, timezone

import pytest

from frequenz.sdk.timeseries import SourceProperties, WindowSide
from frequenz.sdk.timeseries._resampling._buffer import _ResamplingBuffer

_LOGGER_NAME = "frequenz.sdk.timeseries._resampling._buffer"
_START = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _at(seconds: float) -> datetime:
    """Return the timestamp `seconds` after the start of the tests."""
    return _START + timedelta(seconds=seconds)


def _make_buffer(
    *, max_len: int = 16, warn_len: int = 8
) -> tuple[_ResamplingBuffer, SourceProperties]:
    """Return a buffer and the source properties it keeps up to date."""
    props = SourceProperties()
    buffer = _ResamplingBuffer(
        "test", max_len=max_len, warn_len=warn_len, source_properties=props
    )
    return buffer, props


def _logs(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    """Return the messages the buffer logged with the given level."""
    return [
        message
        for name, record_level, message in caplog.record_tuples
        if name == _LOGGER_NAME and record_level == level
    ]


def test_add_updates_source_properties() -> None:
    """Test adding samples updates the start time and the received samples."""
    buffer, props = _make_buffer()

    buffer.add((_at(1), 1.0))
    buffer.add((_at(2), 2.0))

    assert len(buffer) == 2
    assert props.sampling_start == _at(1)
    assert props.received_samples == 2


@pytest.mark.parametrize(
    "closed, expected",
    [
        (WindowSide.LEFT, [(_at(1), 1.0), (_at(2), 2.0)]),
        (WindowSide.RIGHT, [(_at(2), 2.0), (_at(3), 3.0)]),
    ],
)
def test_relevant_samples_closed_side(
    closed: WindowSide, expected: list[tuple[datetime, float]]
) -> None:
    """Test the closed side decides whether samples at the edges are relevant."""
    buffer, _ = _make_buffer()
    for seconds in range(5):
        buffer.add((_at(seconds), float(seconds)))

    assert (
        buffer.relevant_samples(_at(1), _at(3), closed, remove_older=False) == expected
    )


@pytest.mark.parametrize("remove_older", [True, False])
def test_relevant_samples_removes_older_samples(remove_older: bool) -> None:
    """Test older samples are only removed when asked, and are not counted as dropped."""
    buffer, props = _make_buffer()
    for seconds in range(5):
        buffer.add((_at(seconds), float(seconds)))

    relevant = buffer.relevant_samples(
        _at(2), _at(4), WindowSide.LEFT, remove_older=remove_older
    )

    assert relevant == [(_at(2), 2.0), (_at(3), 3.0)]
    assert len(buffer) == (3 if remove_older else 5)
    assert props.dropped_samples == 0


def test_dropped_samples_are_counted_when_getting_relevant_samples() -> None:
    """Test samples dropped because the buffer is full are counted later."""
    buffer, props = _make_buffer(max_len=2, warn_len=1)

    for seconds in (0.25, 0.5, 0.75):
        buffer.add((_at(seconds), seconds))
    assert props.dropped_samples == 0

    _ = buffer.relevant_samples(_at(0), _at(1), WindowSide.LEFT, remove_older=False)
    assert props.dropped_samples == 1


def test_dropping_samples_that_may_be_relevant_is_logged_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test dropping samples that may be relevant is logged once.

    The timestamps of dropped samples are not kept, so they may be relevant when
    the oldest sample in the buffer is.
    """
    buffer, props = _make_buffer(max_len=4, warn_len=2)

    for seconds in range(5):
        buffer.add((_at(seconds), float(seconds)))
    relevant = buffer.relevant_samples(
        _at(0), _at(5), WindowSide.LEFT, remove_older=True
    )
    assert relevant == [(_at(s), float(s)) for s in range(1, 5)]

    buffer.add((_at(5), 5.0))
    _ = buffer.relevant_samples(_at(0), _at(6), WindowSide.LEFT, remove_older=True)

    assert props.dropped_samples == 2
    assert _logs(caplog, logging.ERROR) == [
        "The buffer for timeseries test is full (max_buffer_len=4), so samples "
        "needed for resampling may have been dropped. Use a bigger max_buffer_len."
    ]


@pytest.mark.parametrize("closed", [WindowSide.LEFT, WindowSide.RIGHT])
def test_dropping_old_samples_is_not_logged(
    closed: WindowSide, caplog: pytest.LogCaptureFixture
) -> None:
    """Test dropping samples older than a buffered irrelevant one is not an error."""
    buffer, props = _make_buffer(max_len=3, warn_len=1)

    for seconds in (0.25, 0.5):
        buffer.add((_at(seconds), seconds))
    _ = buffer.relevant_samples(_at(0), _at(1), closed, remove_older=True)

    # The buffer is full, so these push out the sample at 0.25, but the one at
    # 0.5 is still in the buffer, and it is not relevant anymore
    for seconds in (1.25, 1.5):
        buffer.add((_at(seconds), seconds))
    relevant = buffer.relevant_samples(_at(1), _at(2), closed, remove_older=True)

    assert relevant == [(_at(1.25), 1.25), (_at(1.5), 1.5)]
    assert props.dropped_samples == 1
    assert not _logs(caplog, logging.ERROR)


def test_bigger_than_warn_len_is_logged_once_after_removing_older_samples(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test a buffer holding more than `warn_len` samples is logged once.

    It is only checked after removing older samples, as before that the buffer
    can hold samples that are not relevant anymore.
    """
    buffer, _ = _make_buffer(max_len=32, warn_len=4)

    for i in range(10):
        buffer.add((_at(i / 10), 1.0))
    _ = buffer.relevant_samples(_at(0), _at(1), WindowSide.LEFT, remove_older=False)
    assert not _logs(caplog, logging.WARNING)

    for window in range(1, 3):
        for i in range(10):
            buffer.add((_at(window + i / 10), 1.0))
        _ = buffer.relevant_samples(
            _at(window), _at(window + 1), WindowSide.LEFT, remove_older=True
        )

    assert _logs(caplog, logging.WARNING) == [
        "The buffer for timeseries test holds 10 samples, more than "
        "warn_buffer_len (4)"
    ]
