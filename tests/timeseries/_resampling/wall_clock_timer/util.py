# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Utility functions for testing the wall clock timer."""

from datetime import datetime, timedelta, timezone
from typing import assert_never, overload


@overload
def to_seconds(value: datetime | timedelta | float) -> float: ...


@overload
def to_seconds(value: None) -> None: ...


def to_seconds(value: datetime | timedelta | float | None) -> float | None:
    """Convert a datetime, timedelta, or float to seconds."""
    match value:
        case datetime():
            return value.timestamp()
        case timedelta():
            return value.total_seconds()
        case float() | int() | None:
            return value
        case unexpected:
            assert_never(unexpected)


# This is needed to work with the datetime_mock fixture in conftest.py
def wall_now() -> datetime:
    """Get the current wall clock time from the mocked datetime in the target module."""
    # Disable isort formatting because it wants to put the ignore in the wrong line
    # We now also have to ignore the maximum line length (E501)
    # isort: off
    # pylint: disable-next=import-outside-toplevel
    from frequenz.sdk.timeseries._resampling._wall_clock_timer import (  # type: ignore[attr-defined] # noqa: E501
        datetime as mock_datetime,
    )

    # isort: on

    return mock_datetime.now(timezone.utc)
