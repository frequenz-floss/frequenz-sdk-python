# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for timeseries base types."""


from datetime import datetime

from frequenz.quantities import Power

from frequenz.sdk.timeseries._base_types import Bounds, SystemBounds


def test_bounds_contains() -> None:
    """Tests with complete bounds."""
    bounds = Bounds(lower=Power.from_watts(10), upper=Power.from_watts(100))
    assert Power.from_watts(50) in bounds  # within
    assert Power.from_watts(10) in bounds  # at lower
    assert Power.from_watts(100) in bounds  # at upper
    assert Power.from_watts(9) not in bounds  # below lower
    assert Power.from_watts(101) not in bounds  # above upper


def test_bounds_contains_no_lower() -> None:
    """Tests without lower bound."""
    bounds_no_lower = Bounds(lower=None, upper=Power.from_watts(100))
    assert Power.from_watts(50) in bounds_no_lower  # within upper
    assert Power.from_watts(100) in bounds_no_lower  # at upper
    assert Power.from_watts(101) not in bounds_no_lower  # above upper


def test_bounds_contains_no_upper() -> None:
    """Tests without upper bound."""
    bounds_no_upper = Bounds(lower=Power.from_watts(10), upper=None)
    assert Power.from_watts(50) in bounds_no_upper  # within lower
    assert Power.from_watts(10) in bounds_no_upper  # at lower
    assert Power.from_watts(9) not in bounds_no_upper  # below lower


def test_bounds_contains_no_bounds() -> None:
    """Tests with no bounds."""
    bounds_no_bounds: Bounds[Power | None] = Bounds(lower=None, upper=None)
    assert Power.from_watts(50) in bounds_no_bounds  # any value within bounds


INCLUSION_BOUND = Bounds(lower=Power.from_watts(10), upper=Power.from_watts(100))
EXCLUSION_BOUND = Bounds(lower=Power.from_watts(40), upper=Power.from_watts(50))


def test_system_bounds_contains() -> None:
    """Tests with complete system bounds."""
    system_bounds = SystemBounds(
        timestamp=datetime.now(),
        inclusion_bounds=INCLUSION_BOUND,
        exclusion_bounds=EXCLUSION_BOUND,
    )

    assert Power.from_watts(30) in system_bounds  # within inclusion, not in exclusion
    assert Power.from_watts(45) not in system_bounds  # within inclusion and exclusion
    assert Power.from_watts(110) not in system_bounds  # outside inclusion


def test_system_bounds_contains_no_exclusion() -> None:
    """Tests with no exclusion bounds."""
    system_bounds_no_exclusion = SystemBounds(
        timestamp=datetime.now(),
        inclusion_bounds=INCLUSION_BOUND,
        exclusion_bounds=None,
    )
    assert Power.from_watts(30) in system_bounds_no_exclusion  # within inclusion
    assert Power.from_watts(110) not in system_bounds_no_exclusion  # outside inclusion


def test_system_bounds_contains_no_inclusion() -> None:
    """Tests with no inclusion bounds."""
    system_bounds_no_inclusion = SystemBounds(
        timestamp=datetime.now(),
        inclusion_bounds=None,
        exclusion_bounds=EXCLUSION_BOUND,
    )
    assert Power.from_watts(30) not in system_bounds_no_inclusion  # outside exclusion
    assert Power.from_watts(45) not in system_bounds_no_inclusion  # within exclusion


def test_system_bounds_contains_no_bounds() -> None:
    """Tests with no bounds."""
    system_bounds_no_bounds = SystemBounds(
        timestamp=datetime.now(),
        inclusion_bounds=None,
        exclusion_bounds=None,
    )
    assert Power.from_watts(30) not in system_bounds_no_bounds  # any value outside
    assert Power.from_watts(110) not in system_bounds_no_bounds  # any value outside
