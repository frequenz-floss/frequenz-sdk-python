# tests/timeseries/test_resampling.py:93: License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the resampling buffer."""

from __future__ import annotations

from typing import Optional

import pytest

from frequenz.sdk.timeseries._resampling._buffer import Buffer


def test_buffer_init_zero_capacity() -> None:
    """Test a buffer with zero capacity can't be created."""
    with pytest.raises(ValueError):
        _ = Buffer[int](0)


def test_buffer_init_empty() -> None:
    """Test an empty buffer is empty and can be pushed to."""
    buffer = Buffer[int](2)

    assert not buffer
    assert buffer == []  # pylint: disable=use-implicit-booleaness-not-comparison
    assert len(buffer) == 0
    for i in range(-5, 5):
        with pytest.raises(IndexError):
            _ = buffer[i]

    buffer.push(11)
    assert buffer == [11]

    buffer.push(15)
    assert buffer == [11, 15]

    buffer.push(25)
    assert buffer == [15, 25]


def test_buffer_init_full() -> None:
    """Test a full buffer can be created, pushed to, and drops old values."""
    init = (3, 2, 6, 10, 9)
    buffer = Buffer[int](5, init)

    assert buffer == init
    assert len(buffer) == len(init)
    for i in range(5):
        assert buffer[i] == init[i]
    for i in range(-1, -6, -1):
        assert buffer[i] == init[i]
    with pytest.raises(IndexError):
        _ = buffer[-6]
    with pytest.raises(IndexError):
        _ = buffer[5]

    buffer.push(11)
    assert buffer == [2, 6, 10, 9, 11]
    assert buffer[4] == 11
    with pytest.raises(IndexError):
        _ = buffer[-6]
    with pytest.raises(IndexError):
        _ = buffer[5]


def test_buffer_init_partial() -> None:
    """Test a partially initialized buffer.

    Test it can be created, pushed to, grows and eventuall drops old values.
    """
    init = (2, 9)
    buffer = Buffer[int](4, init)

    assert buffer == init
    assert len(buffer) == len(init)
    with pytest.raises(IndexError):
        _ = buffer[-3]
    with pytest.raises(IndexError):
        _ = buffer[2]
    for i in range(2):
        assert buffer[i] == init[i]
    for i in range(-1, -3, -1):
        assert buffer[i] == init[i]

    buffer.push(11)
    assert buffer == [2, 9, 11]

    buffer.push(15)
    assert buffer == [2, 9, 11, 15]

    buffer.push(25)
    assert buffer == [9, 11, 15, 25]


def test_buffer_push() -> None:
    """Test a buffer can be pushed to."""
    buffer = Buffer[int](2)

    with pytest.raises(IndexError):
        _ = buffer[0]
    with pytest.raises(IndexError):
        _ = buffer[1]
    with pytest.raises(IndexError):
        _ = buffer[2]

    buffer.push(10)
    assert len(buffer) == 1
    assert buffer == [10]
    assert buffer[0] == 10
    with pytest.raises(IndexError):
        _ = buffer[2]

    buffer.push(20)
    assert len(buffer) == 2
    assert buffer == [10, 20]
    assert buffer[0] == 10
    assert buffer[1] == 20
    with pytest.raises(IndexError):
        _ = buffer[2]

    buffer.push(5)
    assert len(buffer) == 2
    assert buffer == [20, 5]
    assert buffer[0] == 20
    assert buffer[1] == 5
    with pytest.raises(IndexError):
        _ = buffer[2]


def test_buffer_clear() -> None:
    """Test a buffer can be cleared correctly."""
    buffer = Buffer[int](2, (1, 2))

    buffer.clear()
    assert not buffer
    assert buffer == []  # pylint: disable=use-implicit-booleaness-not-comparison
    assert len(buffer) == 0
    with pytest.raises(IndexError):
        _ = buffer[0]
    with pytest.raises(IndexError):
        _ = buffer[1]
    with pytest.raises(IndexError):
        _ = buffer[2]


@pytest.mark.parametrize("step", [None, 1, 2, 5, -5, -2, -1])
@pytest.mark.parametrize("stop", [0, None, 1, 3, 5, -5, -3, -1])
@pytest.mark.parametrize("start", [0, None, 1, 3, 5, -5, -3, -1])
def test_slice(start: Optional[int], stop: Optional[int], step: Optional[int]) -> None:
    """Test slicing operations."""
    init = (3, 2, 6, 10, 9)
    buffer = Buffer[int](5, init)

    init_slice = init[start:stop:step]
    slice_ = buffer[start:stop:step]

    assert len(slice_) == len(init_slice)
    assert list(slice_) == list(init_slice)
    for i in range(3):
        with pytest.raises(IndexError):
            _ = init_slice[-len(init_slice) - i - 1]
        with pytest.raises(IndexError):
            # For some reason pylint thinks slice_ is unsubscriptable, but the
            # type is properly inferred as Sequence[int], so it looks like
            # a pylint bug
            # pylint: disable=unsubscriptable-object
            _ = slice_[-len(init_slice) - i - 1]
        with pytest.raises(IndexError):
            # pylint: disable=unsubscriptable-object
            _ = slice_[len(init_slice) + i]
    for i in range(len(init_slice)):  # pylint: disable=consider-using-enumerate
        # pylint: disable=unsubscriptable-object
        assert slice_[i] == init_slice[i]
    for i in range(-1, -len(init_slice) - 1, -1):
        # pylint: disable=unsubscriptable-object
        assert slice_[i] == init_slice[i]
