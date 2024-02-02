# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for methods provided by the PowerManager's reports."""

from frequenz.sdk.actor._power_managing import _Report
from frequenz.sdk.timeseries import Bounds, Power


class BoundsTester:
    """Test the bounds adjustment."""

    def __init__(
        self,
        inclusion_bounds: tuple[float, float] | None,
        exclusion_bounds: tuple[float, float] | None,
    ) -> None:
        """Initialize the tester."""
        self._report = _Report(
            target_power=None,
            _inclusion_bounds=(
                Bounds(  # pylint: disable=protected-access
                    Power.from_watts(inclusion_bounds[0]),
                    Power.from_watts(inclusion_bounds[1]),
                )
                if inclusion_bounds is not None
                else None
            ),
            _exclusion_bounds=(
                Bounds(  # pylint: disable=protected-access
                    Power.from_watts(exclusion_bounds[0]),
                    Power.from_watts(exclusion_bounds[1]),
                )
                if exclusion_bounds is not None
                else None
            ),
            distribution_result=None,
        )

    def case(
        self,
        desired_power: float,
        expected_lower: float | None,
        expected_upper: float | None,
    ) -> None:
        """Test a case."""
        lower, upper = self._report.adjust_to_bounds(Power.from_watts(desired_power))

        tgt_lower = (
            Power.from_watts(expected_lower) if expected_lower is not None else None
        )
        assert lower == tgt_lower
        tgt_upper = (
            Power.from_watts(expected_upper) if expected_upper is not None else None
        )
        assert upper == tgt_upper


def test_adjust_to_bounds() -> None:
    """Test that desired powers are adjusted to bounds correctly."""
    tester = BoundsTester(
        inclusion_bounds=(-200.0, 200.0),
        exclusion_bounds=(-30.0, 30.0),
    )
    tester.case(0.0, 0.0, 0.0)
    tester.case(-210.0, -200.0, None)
    tester.case(220.0, None, 200.0)
    tester.case(-20.0, -30.0, 30.0)
    tester.case(-30.0, -30.0, -30.0)
    tester.case(30.0, 30.0, 30.0)
    tester.case(50.0, 50.0, 50.0)
    tester.case(-200.0, -200.0, -200.0)
    tester.case(200.0, 200.0, 200.0)

    tester = BoundsTester(
        inclusion_bounds=(-200.0, 200.0),
        exclusion_bounds=(-210.0, 0.0),
    )
    tester.case(0.0, 0.0, 0.0)
    tester.case(-210.0, None, 0.0)
    tester.case(220.0, None, 200.0)
    tester.case(-20.0, None, 0.0)
    tester.case(20.0, 20.0, 20.0)

    tester = BoundsTester(
        inclusion_bounds=(-200.0, 200.0),
        exclusion_bounds=(0.0, 210.0),
    )
    tester.case(0.0, 0.0, 0.0)
    tester.case(-210.0, -200.0, None)
    tester.case(220.0, 0.0, None)
    tester.case(-20.0, -20.0, -20.0)
    tester.case(20.0, 0.0, None)

    tester = BoundsTester(
        inclusion_bounds=(-200.0, 200.0),
        exclusion_bounds=None,
    )
    tester.case(0.0, 0.0, 0.0)
    tester.case(-210.0, -200.0, None)
    tester.case(220.0, None, 200.0)
    tester.case(-20.0, -20.0, -20.0)
    tester.case(20.0, 20.0, 20.0)

    tester = BoundsTester(
        inclusion_bounds=(-200.0, 200.0),
        exclusion_bounds=(-210.0, 210.0),
    )
    tester.case(0.0, None, None)
    tester.case(-210.0, None, None)
    tester.case(220.0, None, None)
    tester.case(-20.0, None, None)
    tester.case(20.0, None, None)

    tester = BoundsTester(
        inclusion_bounds=None,
        exclusion_bounds=(-210.0, 210.0),
    )
    tester.case(0.0, None, None)
    tester.case(-210.0, None, None)
    tester.case(220.0, None, None)
    tester.case(-20.0, None, None)
    tester.case(20.0, None, None)
