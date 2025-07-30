# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Fixtures for wall clock timer tests."""

from collections.abc import Iterator
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from frequenz.core.datetime import UNIX_EPOCH


@pytest.fixture
def datetime_mock() -> Iterator[MagicMock]:
    """Mock the datetime class in the target module and set now to the UNIX epoch."""
    dt_symbol = "frequenz.sdk.timeseries._resampling._wall_clock_timer.datetime"
    dt_mock = MagicMock(name="datetime_mock", wraps=datetime, spec_set=datetime)
    dt_mock.now.return_value = UNIX_EPOCH
    with patch(dt_symbol, new=dt_mock):
        yield dt_mock
