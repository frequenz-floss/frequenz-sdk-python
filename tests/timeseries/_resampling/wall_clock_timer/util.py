# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Utility functions for testing the wall clock timer."""

import re
from datetime import datetime, timedelta, timezone
from typing import assert_never, overload

import pytest

# This is not great, we are depending on an internal pytest API, but it is
# the most convenient way to provide a custom approx() comparison for datetime
# and timedelta.
# Other alternatives proven to be even more complex and hacky.
# It also looks like we are not the only ones doing this, see:
# https://github.com/pytest-dev/pytest/issues/8395
from _pytest.python_api import ApproxBase
from typing_extensions import override

from frequenz.sdk.timeseries._resampling._wall_clock_timer import ClocksInfo, TickInfo


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


# We need to rewrite most of the attributes in these classes to use approximate
# comparisons. This means if a new field is added, we need to update the
# approx_tick_info function below to handle the new fields. To catch this, we do
# a sanity check here, so if new fields are added we get an early warning instead of
# getting tests errors because of some rounding error in a newly added field.
assert set(ClocksInfo.__dataclass_fields__.keys()) == {
    "monotonic_requested_sleep",
    "monotonic_time",
    "wall_clock_time",
    "monotonic_elapsed",
    "wall_clock_elapsed",
    "wall_clock_factor",
}, "ClocksInfo fields were added or removed, please update the approx_tick_info function."
assert set(TickInfo.__dataclass_fields__.keys()) == {
    "expected_tick_time",
    "sleep_infos",
}, "TickInfo fields were added or removed, please update the approx_tick_info function."


def approx_tick_info(
    expected: TickInfo,
    *,
    abs: timedelta = timedelta(milliseconds=1),  # pylint: disable=redefined-builtin
) -> TickInfo:
    """Create a copy of a `TickInfo` object with approximate comparisons.

    Fields are replaced by approximate comparison objects (`approx_time` or
    `pytest.approx`).

    This version bypasses `__post_init__` to avoid validation `TypeError`s.

    Args:
        expected: The expected `TickInfo` object to compare against.
        abs: The absolute tolerance as a `timedelta` for all time-based
            comparisons. Defaults to 1ms.

    Returns:
        A new `TickInfo` instance ready for approximate comparison.
    """
    abs_s = abs.total_seconds()
    approx_sleeps = []
    for s_info in expected.sleep_infos:
        # HACK: Create a blank instance to bypass __init__ and __post_init__.
        # This prevents the TypeError from the validation logic.
        approx_s = object.__new__(ClocksInfo)

        # Use object.__setattr__ to assign fields to the frozen instance.
        object.__setattr__(
            approx_s,
            "monotonic_requested_sleep",
            approx_time(s_info.monotonic_requested_sleep, abs=abs),
        )
        # Use the standard pytest.approx for float values
        object.__setattr__(
            approx_s, "monotonic_time", pytest.approx(s_info.monotonic_time, abs=abs_s)
        )
        object.__setattr__(
            approx_s, "wall_clock_time", approx_time(s_info.wall_clock_time, abs=abs)
        )
        object.__setattr__(
            approx_s,
            "monotonic_elapsed",
            approx_time(s_info.monotonic_elapsed, abs=abs),
        )
        object.__setattr__(
            approx_s,
            "wall_clock_elapsed",
            approx_time(s_info.wall_clock_elapsed, abs=abs),
        )
        if s_info.wall_clock_factor is not None:
            object.__setattr__(
                approx_s,
                "wall_clock_factor",
                pytest.approx(s_info.wall_clock_factor, abs=abs_s),
            )
        approx_sleeps.append(approx_s)

    # Do the same for the top-level frozen TickInfo object
    approx_tick_info = object.__new__(TickInfo)
    object.__setattr__(
        approx_tick_info,
        "expected_tick_time",
        approx_time(expected.expected_tick_time, abs=abs),
    )
    object.__setattr__(approx_tick_info, "sleep_infos", approx_sleeps)

    return approx_tick_info


class matches_re:  # pylint: disable=invalid-name
    """Assert that a given string (or string representation) matches a regex pattern."""

    def __init__(self, pattern: str, flags: int = 0) -> None:
        """Initialize with a regex pattern and optional flags."""
        self._regex = re.compile(pattern, flags)

    @override
    def __eq__(self, other: object) -> bool:
        """Check if the string representation of `other` matches the regex pattern."""
        return bool(self._regex.match(str(other)))

    @override
    def __repr__(self) -> str:
        """Return a string representation of this instance."""
        return self._regex.pattern
