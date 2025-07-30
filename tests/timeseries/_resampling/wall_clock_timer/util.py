# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Utility functions for testing the wall clock timer."""

from datetime import datetime, timedelta, timezone
from typing import assert_never, overload

# This is not great, we are depending on an internal pytest API, but it is
# the most convenient way to provide a custom approx() comparison for datetime
# and timedelta.
# Other alternatives proven to be even more complex and hacky.
# It also looks like we are not the only ones doing this, see:
# https://github.com/pytest-dev/pytest/issues/8395
from _pytest.python_api import ApproxBase


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


# Pylint complains about abstract-method because _yield_comparisons is not implemented
# but it is used only in the default __eq__ method, which we are re-defining, so we can
# ignore it.
class approx_time(ApproxBase):  # pylint: disable=invalid-name, abstract-method
    """Perform approximate comparisons for datetime or timedelta objects.

    Inherits from `ApproxBase` to provide a rich comparison output in pytest.
    """

    expected: datetime | timedelta
    abs: timedelta

    def __init__(
        self,
        expected: datetime | timedelta,
        *,
        abs: timedelta = timedelta(milliseconds=1),  # pylint: disable=redefined-builtin
    ) -> None:
        """Initialize this instance."""
        if abs < timedelta():
            raise ValueError(
                f"absolute tolerance must be a non-negative timedelta, not {abs}"
            )
        super().__init__(expected, abs=abs)

    def __repr__(self) -> str:
        """Return a string representation of this instance."""
        return f"{self.expected} ± {self.abs}"

    def __eq__(self, actual: object) -> bool:
        """Compare this instance with another object."""
        # We need to split the cases for datetime and timedelta for type checking
        # reasons.
        diff: timedelta
        match (self.expected, actual):
            case (datetime(), datetime()):
                diff = self.expected - actual
            case (timedelta(), timedelta()):
                diff = self.expected - actual
            case _:
                return NotImplemented

        return abs(diff) <= self.abs
