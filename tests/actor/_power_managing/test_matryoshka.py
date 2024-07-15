# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the Matryoshka power manager algorithm."""

import asyncio
from datetime import datetime, timedelta, timezone

from frequenz.sdk import timeseries
from frequenz.sdk.actor._power_managing import Proposal
from frequenz.sdk.actor._power_managing._matryoshka import Matryoshka
from frequenz.sdk.timeseries import Power, _base_types


class StatefulTester:
    """A stateful tester for the Matryoshka algorithm."""

    def __init__(
        self,
        batteries: frozenset[int],
        system_bounds: _base_types.SystemBounds,
    ) -> None:
        """Create a new instance of the stateful tester."""
        self._call_count = 0
        self._batteries = batteries
        self._system_bounds = system_bounds
        self.algorithm = Matryoshka(max_proposal_age=timedelta(seconds=60.0))

    def tgt_power(  # pylint: disable=too-many-arguments
        self,
        priority: int,
        power: float | None,
        bounds: tuple[float | None, float | None],
        expected: float | None,
        creation_time: float | None = None,
        must_send: bool = False,
    ) -> None:
        """Test the target power calculation."""
        self._call_count += 1
        tgt_power = self.algorithm.calculate_target_power(
            self._batteries,
            Proposal(
                component_ids=self._batteries,
                source_id=f"actor-{priority}",
                preferred_power=None if power is None else Power.from_watts(power),
                bounds=timeseries.Bounds(
                    None if bounds[0] is None else Power.from_watts(bounds[0]),
                    None if bounds[1] is None else Power.from_watts(bounds[1]),
                ),
                priority=priority,
                creation_time=(
                    creation_time
                    if creation_time is not None
                    else asyncio.get_event_loop().time()
                ),
                set_operating_point=False,
            ),
            self._system_bounds,
            must_send,
        )
        assert tgt_power == (
            Power.from_watts(expected) if expected is not None else None
        )

    def bounds(
        self,
        priority: int,
        expected_power: float | None,
        expected_bounds: tuple[float, float],
    ) -> None:
        """Test the status report."""
        report = self.algorithm.get_status(
            self._batteries, priority, self._system_bounds
        )
        if expected_power is None:
            assert report.target_power is None
        else:
            assert report.target_power is not None
            assert report.target_power.as_watts() == expected_power
        # pylint: disable=protected-access
        assert report._inclusion_bounds is not None
        assert report._inclusion_bounds.lower.as_watts() == expected_bounds[0]
        assert report._inclusion_bounds.upper.as_watts() == expected_bounds[1]
        # pylint: enable=protected-access


async def test_matryoshka_no_excl() -> None:  # pylint: disable=too-many-statements
    """Tests for the power managing actor.

    With just inclusion bounds, and no exclusion bounds.
    """
    batteries = frozenset({2, 5})

    system_bounds = _base_types.SystemBounds(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
        ),
        exclusion_bounds=timeseries.Bounds(lower=Power.zero(), upper=Power.zero()),
    )

    tester = StatefulTester(batteries, system_bounds)

    tester.tgt_power(priority=2, power=25.0, bounds=(25.0, 50.0), expected=25.0)
    tester.bounds(priority=2, expected_power=25.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=25.0, expected_bounds=(25.0, 50.0))

    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=None)
    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=25.0, must_send=True
    )
    tester.bounds(priority=1, expected_power=25.0, expected_bounds=(25.0, 50.0))

    tester.tgt_power(priority=3, power=10.0, bounds=(10.0, 15.0), expected=15.0)
    tester.bounds(priority=3, expected_power=15.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=15.0, expected_bounds=(10.0, 15.0))
    tester.bounds(priority=1, expected_power=15.0, expected_bounds=(10.0, 15.0))

    tester.tgt_power(priority=3, power=10.0, bounds=(10.0, 22.0), expected=22.0)
    tester.bounds(priority=3, expected_power=22.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=22.0, expected_bounds=(10.0, 22.0))
    tester.bounds(priority=1, expected_power=22.0, expected_bounds=(10.0, 22.0))

    tester.tgt_power(priority=1, power=30.0, bounds=(20.0, 50.0), expected=None)
    tester.bounds(priority=1, expected_power=22.0, expected_bounds=(10.0, 22.0))

    tester.tgt_power(priority=3, power=10.0, bounds=(10.0, 50.0), expected=30.0)
    tester.bounds(priority=3, expected_power=30.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=30.0, expected_bounds=(10.0, 50.0))
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(25.0, 50.0))

    tester.tgt_power(priority=2, power=40.0, bounds=(40.0, None), expected=40.0)
    tester.bounds(priority=3, expected_power=40.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=40.0, expected_bounds=(10.0, 50.0))
    tester.bounds(priority=1, expected_power=40.0, expected_bounds=(40.0, 50.0))

    tester.tgt_power(priority=2, power=0.0, bounds=(None, None), expected=30.0)
    tester.bounds(priority=4, expected_power=30.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=3, expected_power=30.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=30.0, expected_bounds=(10.0, 50.0))
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(10.0, 50.0))

    tester.tgt_power(priority=4, power=-50.0, bounds=(None, -50.0), expected=-50.0)
    tester.bounds(priority=4, expected_power=-50.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=3, expected_power=-50.0, expected_bounds=(-200.0, -50.0))
    tester.bounds(priority=2, expected_power=-50.0, expected_bounds=(-200.0, -50.0))
    tester.bounds(priority=1, expected_power=-50.0, expected_bounds=(-200.0, -50.0))

    tester.tgt_power(priority=3, power=-0.0, bounds=(-200.0, 200.0), expected=None)
    tester.bounds(priority=1, expected_power=-50.0, expected_bounds=(-200.0, -50.0))

    tester.tgt_power(priority=1, power=-150.0, bounds=(-200.0, -150.0), expected=-150.0)
    tester.bounds(priority=2, expected_power=-150.0, expected_bounds=(-200.0, -50.0))
    tester.bounds(priority=1, expected_power=-150.0, expected_bounds=(-200.0, -50.0))

    tester.tgt_power(priority=4, power=-180.0, bounds=(-200.0, -50.0), expected=None)
    tester.bounds(priority=1, expected_power=-150.0, expected_bounds=(-200.0, -50.0))

    tester.tgt_power(priority=4, power=50.0, bounds=(50.0, None), expected=50.0)
    tester.bounds(priority=4, expected_power=50.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=3, expected_power=50.0, expected_bounds=(50.0, 200.0))
    tester.bounds(priority=2, expected_power=50.0, expected_bounds=(50.0, 200.0))
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(50.0, 200.0))

    tester.tgt_power(priority=4, power=0.0, bounds=(-200.0, 200.0), expected=-150.0)
    tester.bounds(priority=4, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=3, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=-150.0, expected_bounds=(-200.0, 200.0))

    tester.tgt_power(priority=3, power=0.0, bounds=(-200.0, 200.0), expected=None)
    tester.bounds(priority=3, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=-150.0, expected_bounds=(-200.0, 200.0))

    tester.tgt_power(priority=2, power=50.0, bounds=(-100, 100), expected=-100.0)
    tester.bounds(priority=3, expected_power=-100.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=-100.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=-100.0, expected_bounds=(-100.0, 100.0))

    tester.tgt_power(priority=1, power=100.0, bounds=(100, 200), expected=100.0)
    tester.bounds(priority=1, expected_power=100.0, expected_bounds=(-100.0, 100.0))

    tester.tgt_power(priority=1, power=50.0, bounds=(50, 200), expected=50.0)
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(-100.0, 100.0))

    tester.tgt_power(priority=1, power=200.0, bounds=(50, 200), expected=100.0)
    tester.bounds(priority=1, expected_power=100.0, expected_bounds=(-100.0, 100.0))

    tester.tgt_power(priority=1, power=0.0, bounds=(-200, 200), expected=0.0)
    tester.bounds(priority=1, expected_power=0.0, expected_bounds=(-100.0, 100.0))

    tester.tgt_power(priority=1, power=None, bounds=(-200, 200), expected=50.0)
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(-100.0, 100.0))


async def test_matryoshka_with_excl_1() -> None:
    """Tests for the power managing actor.

    With inclusion bounds, and exclusion bounds -30.0 to 0.0.
    """
    batteries = frozenset({2, 5})

    system_bounds = _base_types.SystemBounds(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
        ),
        exclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-30.0), upper=Power.zero()
        ),
    )

    tester = StatefulTester(batteries, system_bounds)

    tester.tgt_power(priority=2, power=25.0, bounds=(25.0, 50.0), expected=25.0)
    tester.bounds(priority=2, expected_power=25.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=25.0, expected_bounds=(25.0, 50.0))

    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=None)
    tester.bounds(priority=1, expected_power=25.0, expected_bounds=(25.0, 50.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-10.0, 50.0), expected=20.0)
    tester.bounds(priority=1, expected_power=20.0, expected_bounds=(0.0, 50.0))
    tester.bounds(priority=0, expected_power=20.0, expected_bounds=(20.0, 50.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 50.0), expected=0.0)
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(0.0, 50.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 20.0), expected=None)
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(0.0, 20.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, -5.0), expected=None)
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(0.0, 50.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-200.0, -5.0), expected=-30.0)
    tester.bounds(priority=1, expected_power=-30.0, expected_bounds=(-200.0, -30.0))
    tester.bounds(priority=0, expected_power=-30.0, expected_bounds=(-200.0, -30.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-100.0, -5.0), expected=None)
    tester.bounds(priority=0, expected_power=-30.0, expected_bounds=(-100.0, -30.0))

    tester.tgt_power(priority=1, power=-40.0, bounds=(-100.0, -35.0), expected=-40.0)
    tester.bounds(priority=0, expected_power=-40.0, expected_bounds=(-100.0, -35.0))


async def test_matryoshka_with_excl_2() -> None:
    """Tests for the power managing actor.

    With inclusion bounds, and exclusion bounds 0.0 to 30.0.
    """
    batteries = frozenset({2, 5})

    system_bounds = _base_types.SystemBounds(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
        ),
        exclusion_bounds=timeseries.Bounds(
            lower=Power.zero(), upper=Power.from_watts(30.0)
        ),
    )

    tester = StatefulTester(batteries, system_bounds)

    tester.tgt_power(priority=2, power=25.0, bounds=(25.0, 50.0), expected=30.0)
    tester.bounds(priority=2, expected_power=30.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(30.0, 50.0))

    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=None)
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(30.0, 50.0))

    tester.tgt_power(priority=1, power=10.0, bounds=(5.0, 10.0), expected=None)
    tester.bounds(priority=0, expected_power=30.0, expected_bounds=(30, 50.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-10.0, 50.0), expected=0.0)
    tester.bounds(priority=1, expected_power=0.0, expected_bounds=(-10.0, 50.0))
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(-10.0, 50.0))

    tester.tgt_power(priority=0, power=40, bounds=(None, None), expected=40.0)
    tester.tgt_power(priority=0, power=-10, bounds=(None, None), expected=-10.0)
    tester.tgt_power(priority=0, power=10, bounds=(None, None), expected=0.0)
    tester.tgt_power(priority=0, power=20, bounds=(None, None), expected=30.0)
    tester.tgt_power(priority=0, power=None, bounds=(None, None), expected=0.0)

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 50.0), expected=-10.0)
    tester.bounds(priority=0, expected_power=-10.0, expected_bounds=(-10.0, 50.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 20.0), expected=None)
    tester.bounds(priority=0, expected_power=-10.0, expected_bounds=(-10.0, 0.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, -5.0), expected=None)
    tester.bounds(priority=0, expected_power=-10.0, expected_bounds=(-10.0, -5.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-200.0, -5.0), expected=None)
    tester.bounds(priority=1, expected_power=-10.0, expected_bounds=(-200.0, -5.0))
    tester.bounds(priority=0, expected_power=-10.0, expected_bounds=(-10.0, -5.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-100.0, -5.0), expected=None)
    tester.bounds(priority=0, expected_power=-10.0, expected_bounds=(-100.0, -5.0))

    tester.tgt_power(priority=1, power=-40.0, bounds=(-100.0, -35.0), expected=-40.0)
    tester.bounds(priority=0, expected_power=-40.0, expected_bounds=(-100.0, -35.0))


async def test_matryoshka_with_excl_3() -> None:
    """Tests for the power managing actor.

    With inclusion bounds, and exclusion bounds -30.0 to 30.0.
    """
    batteries = frozenset({2, 5})

    system_bounds = _base_types.SystemBounds(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
        ),
        exclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-30.0), upper=Power.from_watts(30.0)
        ),
    )

    tester = StatefulTester(batteries, system_bounds)
    tester.tgt_power(priority=2, power=10.0, bounds=(None, None), expected=30.0)
    tester.tgt_power(priority=2, power=-10.0, bounds=(None, None), expected=-30.0)
    tester.tgt_power(priority=2, power=0.0, bounds=(None, None), expected=0.0)
    tester.tgt_power(priority=3, power=20.0, bounds=(None, None), expected=None)
    tester.tgt_power(priority=1, power=-20.0, bounds=(None, None), expected=-30.0)
    tester.tgt_power(priority=3, power=None, bounds=(None, None), expected=None)
    tester.tgt_power(priority=1, power=None, bounds=(None, None), expected=0.0)

    tester.tgt_power(priority=2, power=25.0, bounds=(25.0, 50.0), expected=30.0)
    tester.bounds(priority=2, expected_power=30.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(30.0, 50.0))

    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=None)
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(30.0, 50.0))

    tester.tgt_power(priority=1, power=10.0, bounds=(5.0, 10.0), expected=None)
    tester.bounds(priority=0, expected_power=30.0, expected_bounds=(30, 50.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-10.0, 50.0), expected=None)
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(30.0, 50.0))
    tester.bounds(priority=0, expected_power=30.0, expected_bounds=(30.0, 50.0))

    tester.tgt_power(priority=1, power=40.0, bounds=(-10.0, 50.0), expected=40.0)
    tester.bounds(priority=0, expected_power=40.0, expected_bounds=(30.0, 50.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 20.0), expected=30.0)
    tester.bounds(priority=0, expected_power=30.0, expected_bounds=(30.0, 50.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-200.0, -5.0), expected=-30.0)
    tester.bounds(priority=1, expected_power=-30.0, expected_bounds=(-200.0, -30.0))
    tester.bounds(priority=0, expected_power=-30.0, expected_bounds=(-200.0, -30.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-100.0, -5.0), expected=None)
    tester.bounds(priority=0, expected_power=-30.0, expected_bounds=(-100.0, -30.0))

    tester.tgt_power(priority=1, power=-40.0, bounds=(-100.0, -35.0), expected=-40.0)
    tester.bounds(priority=0, expected_power=-40.0, expected_bounds=(-100.0, -35.0))


async def test_matryoshka_drop_old_proposals() -> None:
    """Tests for the power managing actor.

    With inclusion bounds, and exclusion bounds -30.0 to 30.0.
    """
    batteries = frozenset({2, 5})

    system_bounds = _base_types.SystemBounds(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
        ),
        exclusion_bounds=timeseries.Bounds(lower=Power.zero(), upper=Power.zero()),
    )

    tester = StatefulTester(batteries, system_bounds)

    now = asyncio.get_event_loop().time()

    tester.tgt_power(priority=3, power=22.0, bounds=(22.0, 30.0), expected=22.0)

    # When a proposal is too old and hasn't been updated, it is dropped.
    tester.tgt_power(
        priority=2,
        power=25.0,
        bounds=(25.0, 50.0),
        creation_time=now - 70.0,
        expected=25.0,
    )

    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=25.0, must_send=True
    )
    tester.algorithm.drop_old_proposals(now)
    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=22.0, must_send=True
    )

    # When overwritten by a newer proposal, that proposal is not dropped.
    tester.tgt_power(
        priority=2,
        power=25.0,
        bounds=(25.0, 50.0),
        creation_time=now - 70.0,
        expected=25.0,
    )
    tester.tgt_power(
        priority=2,
        power=25.0,
        bounds=(25.0, 50.0),
        creation_time=now - 30.0,
        expected=25.0,
        must_send=True,
    )

    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=25.0, must_send=True
    )
    tester.algorithm.drop_old_proposals(now)
    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=25.0, must_send=True
    )
