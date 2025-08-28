# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Fixtures for wall clock timer tests."""

import asyncio
from collections.abc import Callable, Iterator, Sequence
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from frequenz.core.datetime import UNIX_EPOCH

from frequenz.sdk.timeseries._resampling._wall_clock_timer import ClocksInfo, TickInfo

# Some of the utils do assertions and we want them to be rewritten by pytest for better
# error messages
pytest.register_assert_rewrite("tests.timeseries._resampling.wall_clock_timer.util")

# We need to import this module after registering the assert rewrite
from .util import TimeDriver  # noqa: E402


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


@pytest.fixture
async def time_driver(datetime_mock: MagicMock) -> TimeDriver:
    """Fixture to mock the clocks environment for testing."""
    return TimeDriver(
        datetime_mock=datetime_mock,
    )


def pytest_assertrepr_compare(op: str, left: object, right: object) -> list[str] | None:
    """Provide custom, readable error reports for TickInfo comparisons."""
    # We only care about == comparisons involving our TickInfo objects, returning None
    # makes pytest fall back to its default comparison behavior.
    if op != "==" or not isinstance(left, TickInfo) or not isinstance(right, TickInfo):
        return None

    # Helper function to format values for readability
    def format_val(val: object) -> str:
        # For our time-based types, use str() for readability instead of repr()
        if isinstance(val, (datetime, timedelta)):
            return str(val)
        # For our approx objects and others, the default repr is already good.
        return repr(val)

    errors = _compare_tick_info_objects(left, right, format_val)
    # If the comparison was actually successful (no errors), let pytest handle it
    if not errors:
        return None

    # Format the final error message
    report = ["Comparing TickInfo objects:"]
    report.append("  Differing attributes:")
    report.append(f"  {list(errors.keys())!r}")
    report.append("")

    for field, diff in errors.items():
        report.append(f"  Drill down into differing attribute '{field}':")
        # The diff can be a simple tuple of (left, right) values or a list of
        # strings for nested diffs
        match diff:
            case list():
                report.extend(f"    {line}" for line in diff)
            case (left_val, right_val):
                report.append(f"    - {format_val(left_val)}")
                report.append(f"    + {format_val(right_val)}")
            case _:
                assert False, f"Unexpected diff type: {type(diff)}"

    return report


# We need to compare the fields in TickInfo in a particular way in
# _compare_tick_info_objects. If new fields are added to the dataclass, we'll most
# likely need to add a custom comparison for those fields too. To catch this, we do
# a sanity check here, so if new fields are added we get an early warning instead of
# getting a comparison that misses those fields.
assert set(TickInfo.__dataclass_fields__.keys()) == {
    "expected_tick_time",
    "sleep_infos",
}, "TickInfo fields were added or removed, please update the _compare_tick_info_objects function."


def _compare_tick_info_objects(
    left: TickInfo, right: TickInfo, format_val: Callable[[object], str]
) -> dict[str, object]:
    """Compare two TickInfo objects and return a dictionary of differences."""
    errors: dict[str, object] = {}

    # 1. Compare top-level fields
    if left.expected_tick_time != right.expected_tick_time:
        errors["expected_tick_time"] = (
            left.expected_tick_time,
            right.expected_tick_time,
        )

    # 2. Compare the list of ClocksInfo objects
    sleeps_diff = _compare_sleep_infos_list(
        left.sleep_infos, right.sleep_infos, format_val
    )
    if sleeps_diff:
        errors["sleep_infos"] = sleeps_diff

    return errors


def _compare_sleep_infos_list(
    left: Sequence[ClocksInfo],
    right: Sequence[ClocksInfo],
    format_val: Callable[[object], str],
) -> list[str]:
    """Compare two lists of ClocksInfo objects and return a list of error strings."""
    if len(left) != len(right):
        return [
            f"List lengths differ: {len(left)} != {len(right)}",
            f"                     {left!r}",
            "                     !=",
            f"                     {right!r}",
        ]

    diffs: list[str] = []
    for i, (l_clock, r_clock) in enumerate(zip(left, right)):
        if l_clock != r_clock:
            diffs.append(f"Item at index [{i}] differs:")
            # Get detailed diffs for the fields inside the ClocksInfo object
            for field in l_clock.__dataclass_fields__:
                l_val = getattr(l_clock, field)
                r_val = getattr(r_clock, field)
                if l_val != r_val:
                    diffs.append(f"  Attribute '{field}':")
                    diffs.append(f"    - {format_val(l_val)}")
                    diffs.append(f"    + {format_val(r_val)}")
    return diffs
