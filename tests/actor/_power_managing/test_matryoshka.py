# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the Matryoshka power manager algorithm."""

from datetime import datetime, timezone

from frequenz.sdk.actor._power_managing import Proposal
from frequenz.sdk.actor._power_managing._matryoshka import Matryoshka
from frequenz.sdk.timeseries import Power, battery_pool


async def test_matryoshka_algorithm() -> None:
    """Tests for the power managing actor."""
    batteries = frozenset({2, 5})

    algorithm = Matryoshka()
    system_bounds = battery_pool.PowerMetrics(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=battery_pool.Bounds(
            lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
        ),
        exclusion_bounds=battery_pool.Bounds(lower=Power.zero(), upper=Power.zero()),
    )

    call_count = 0

    def case(
        priority: int,
        power: float,
        bounds: tuple[float, float] | None,
        expected: float | None,
    ) -> None:
        nonlocal call_count
        call_count += 1
        tgt_power = algorithm.get_target_power(
            batteries,
            Proposal(
                battery_ids=batteries,
                source_id=f"actor-{priority}",
                preferred_power=Power.from_watts(power),
                bounds=(Power.from_watts(bounds[0]), Power.from_watts(bounds[1]))
                if bounds
                else None,
                priority=priority,
            ),
            system_bounds,
        )
        assert tgt_power == (
            Power.from_watts(expected) if expected is not None else None
        )

    case(priority=2, power=25.0, bounds=(25.0, 50.0), expected=25.0)
    case(priority=1, power=20.0, bounds=(20.0, 50.0), expected=None)
    case(priority=3, power=10.0, bounds=(10.0, 15.0), expected=15.0)
    case(priority=3, power=10.0, bounds=(10.0, 22.0), expected=22.0)
    case(priority=1, power=30.0, bounds=(20.0, 50.0), expected=None)
    case(priority=3, power=10.0, bounds=(10.0, 50.0), expected=30.0)
    case(priority=2, power=40.0, bounds=(40.0, 50.0), expected=40.0)
    case(priority=2, power=0.0, bounds=(-200.0, 200.0), expected=30.0)
    case(priority=4, power=-50.0, bounds=(-200.0, -50.0), expected=-50.0)
    case(priority=3, power=-0.0, bounds=(-200.0, 200.0), expected=None)
    case(priority=1, power=-150.0, bounds=(-200.0, -150.0), expected=-150.0)
    case(priority=4, power=-180.0, bounds=(-200.0, -50.0), expected=None)
    case(priority=4, power=50.0, bounds=(50.0, 200.0), expected=50.0)

    case(priority=4, power=0.0, bounds=(-200.0, 200.0), expected=-150.0)
    case(priority=3, power=0.0, bounds=(-200.0, 200.0), expected=None)
    case(priority=2, power=50.0, bounds=(-100, 100), expected=-100.0)
    case(priority=1, power=100.0, bounds=(100, 200), expected=100.0)
    case(priority=1, power=50.0, bounds=(50, 200), expected=50.0)
    case(priority=1, power=200.0, bounds=(50, 200), expected=100.0)
