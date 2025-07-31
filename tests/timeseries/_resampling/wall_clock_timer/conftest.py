# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Fixtures for wall clock timer tests."""

import asyncio
from collections.abc import Iterator
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from frequenz.core.datetime import UNIX_EPOCH


# Some of the utils do assertions and we want them to be rewritten by pytest for better
# error messages
pytest.register_assert_rewrite("tests.timeseries._resampling.wall_clock_timer.util")


@pytest.fixture
def datetime_mock() -> Iterator[MagicMock]:
    """Mock the datetime class in the target module and set now to the UNIX epoch."""
    dt_symbol = "frequenz.sdk.timeseries._resampling._wall_clock_timer.datetime"
    dt_mock = MagicMock(name="datetime_mock", wraps=datetime, spec_set=datetime)
    dt_mock.now.return_value = UNIX_EPOCH
    with patch(dt_symbol, new=dt_mock):
        yield dt_mock


@pytest.fixture
def asyncio_sleep_mock() -> Iterator[AsyncMock]:
    """Mock asyncio.sleep in the target module for all tests."""
    asyncio_symbol = "frequenz.sdk.timeseries._resampling._wall_clock_timer.asyncio"
    mock = AsyncMock(
        name="asyncio_sleep_mock", wraps=asyncio.sleep, spec_set=asyncio.sleep
    )
    asyncio_mock = AsyncMock(name="asyncio_mock", wraps=asyncio, spec_set=asyncio)
    asyncio_mock.sleep = mock
    with patch(asyncio_symbol, new=asyncio_mock):
        yield mock
