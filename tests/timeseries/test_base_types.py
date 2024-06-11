# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for timeseries base types."""


from datetime import datetime

from frequenz.sdk.timeseries._base_types import Bounds, SystemBounds
from frequenz.sdk.timeseries._quantities import Power


def test_bounds_contains() -> None:
    """Test the `__contains__` method of the `Bounds` class.

    This method checks if a value is within the defined bounds.
    """
    bounds = Bounds(lower=Power.from_watts(10), upper=Power.from_watts(100))

    assert Power.from_watts(50) in bounds  # within bounds
    assert Power.from_watts(10) in bounds  # at lower bound
    assert Power.from_watts(100) in bounds  # at upper bound
    assert Power.from_watts(9) not in bounds  # below lower bound
    assert Power.from_watts(101) not in bounds  # above upper bound

    bounds_no_lower = Bounds(lower=None, upper=Power.from_watts(100))
    assert Power.from_watts(50) in bounds_no_lower  # within upper bound
    assert Power.from_watts(100) in bounds_no_lower  # at upper bound
    assert Power.from_watts(101) not in bounds_no_lower  # above upper bound

    bounds_no_upper = Bounds(lower=Power.from_watts(10), upper=None)
    assert Power.from_watts(50) in bounds_no_upper  # within lower bound
    assert Power.from_watts(10) in bounds_no_upper  # at lower bound
    assert Power.from_watts(9) not in bounds_no_upper  # below lower bound


def test_system_bounds_contains() -> None:
    """
    Test the `__contains__` method of the `SystemBounds` class.

    Checks if value is within inclusion bounds and not exclusion bounds.
    """
    inclusion_bounds = Bounds(lower=Power.from_watts(10), upper=Power.from_watts(100))
    exclusion_bounds = Bounds(lower=Power.from_watts(40), upper=Power.from_watts(50))
    system_bounds = SystemBounds(
        timestamp=datetime.now(),
        inclusion_bounds=inclusion_bounds,
        exclusion_bounds=exclusion_bounds,
    )

    assert Power.from_watts(30) in system_bounds  # within inclusion, not in exclusion
    assert (
        Power.from_watts(45) not in system_bounds
    )  # within inclusion, also in exclusion
    assert Power.from_watts(110) not in system_bounds  # outside inclusion bounds

    system_bounds_no_exclusion = SystemBounds(
        timestamp=datetime.now(),
        inclusion_bounds=inclusion_bounds,
        exclusion_bounds=None,
    )
    assert Power.from_watts(30) in system_bounds_no_exclusion  # within inclusion bounds
    assert (
        Power.from_watts(110) not in system_bounds_no_exclusion
    )  # outside inclusion bounds

    system_bounds_no_inclusion = SystemBounds(
        timestamp=datetime.now(),
        inclusion_bounds=None,
        exclusion_bounds=exclusion_bounds,
    )
    assert (
        Power.from_watts(30) not in system_bounds_no_inclusion
    )  # not in inclusion bounds
