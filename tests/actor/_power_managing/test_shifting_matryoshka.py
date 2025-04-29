# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the Shifting Matryoshka power manager algorithm."""

# pylint: disable=duplicate-code

import asyncio
import re
from datetime import datetime, timedelta, timezone

import pytest
from frequenz.quantities import Power

from frequenz.sdk import timeseries
from frequenz.sdk.microgrid._power_managing import Proposal
from frequenz.sdk.microgrid._power_managing._shifting_matryoshka import (
    ShiftingMatryoshka,
)
from frequenz.sdk.timeseries import _base_types


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
        self.algorithm = ShiftingMatryoshka(max_proposal_age=timedelta(seconds=60.0))

    def tgt_power(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        priority: int,
        power: float | None,
        bounds: tuple[float | None, float | None],
        expected: float | None,
        creation_time: float | None = None,
        must_send: bool = False,
        batteries: frozenset[int] | None = None,
    ) -> None:
        """Test the target power calculation."""
        self._call_count += 1
        tgt_power = self.algorithm.calculate_target_power(
            self._batteries if batteries is None else batteries,
            Proposal(
                component_ids=self._batteries if batteries is None else batteries,
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
    tester.bounds(priority=1, expected_power=25.0, expected_bounds=(0.0, 25.0))

    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=45.0)
    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=45.0, must_send=True
    )
    tester.bounds(priority=1, expected_power=45.0, expected_bounds=(0.0, 25.0))

    tester.tgt_power(priority=3, power=10.0, bounds=(10.0, 15.0), expected=15.0)
    tester.bounds(priority=3, expected_power=15.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=15.0, expected_bounds=(0.0, 5.0))
    tester.bounds(priority=1, expected_power=15.0, expected_bounds=(0.0, 0.0))

    tester.tgt_power(priority=3, power=10.0, bounds=(10.0, 22.0), expected=22.0)
    tester.bounds(priority=3, expected_power=22.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=22.0, expected_bounds=(0.0, 12.0))
    tester.bounds(priority=1, expected_power=22.0, expected_bounds=(0.0, 0.0))

    tester.tgt_power(priority=1, power=30.0, bounds=(20.0, 50.0), expected=22.0)
    tester.bounds(priority=1, expected_power=22.0, expected_bounds=(0.0, 0.0))

    tester.tgt_power(priority=3, power=10.0, bounds=(10.0, 50.0), expected=50.0)
    tester.bounds(priority=3, expected_power=50.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=50.0, expected_bounds=(0.0, 40.0))
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(0.0, 15.0))

    tester.tgt_power(priority=2, power=40.0, bounds=(40.0, None), expected=50.0)
    tester.bounds(priority=3, expected_power=50.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=50.0, expected_bounds=(0.0, 40.0))
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(0.0, 0.0))

    tester.tgt_power(priority=2, power=0.0, bounds=(-200.0, 200.0), expected=40.0)
    tester.bounds(priority=4, expected_power=40.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=3, expected_power=40.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=40.0, expected_bounds=(0.0, 40.0))
    tester.bounds(priority=1, expected_power=40.0, expected_bounds=(0.0, 40.0))

    tester.tgt_power(priority=4, power=-50.0, bounds=(None, -50.0), expected=-50.0)
    tester.bounds(priority=4, expected_power=-50.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=3, expected_power=-50.0, expected_bounds=(-150.0, 0.0))
    tester.bounds(priority=2, expected_power=-50.0, expected_bounds=(0.0, 0.0))
    tester.bounds(priority=1, expected_power=-50.0, expected_bounds=(0.0, 0.0))

    tester.tgt_power(priority=3, power=0.0, bounds=(-200.0, 200.0), expected=-50.0)
    tester.bounds(priority=1, expected_power=-50.0, expected_bounds=(-150.0, 0.0))

    tester.tgt_power(priority=1, power=-150.0, bounds=(-200.0, -150.0), expected=-200.0)
    tester.bounds(priority=2, expected_power=-200.0, expected_bounds=(-150.0, 0.0))
    tester.bounds(priority=1, expected_power=-200.0, expected_bounds=(-150.0, 0.0))

    tester.tgt_power(priority=4, power=-180.0, bounds=(-200.0, -50.0), expected=-200.0)
    tester.bounds(priority=1, expected_power=-200.0, expected_bounds=(-20.0, 130.0))

    tester.tgt_power(priority=4, power=50.0, bounds=(50.0, None), expected=50.0)
    tester.bounds(priority=4, expected_power=50.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=3, expected_power=50.0, expected_bounds=(0.0, 150.0))
    tester.bounds(priority=2, expected_power=50.0, expected_bounds=(0.0, 150.0))
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(0.0, 150.0))

    tester.tgt_power(priority=4, power=0.0, bounds=(-200.0, 200.0), expected=-150.0)
    tester.bounds(priority=4, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=3, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=-150.0, expected_bounds=(-200.0, 200.0))

    tester.tgt_power(priority=3, power=0.0, bounds=(-200.0, 200.0), expected=-150.0)
    tester.bounds(priority=3, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=-150.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=-150.0, expected_bounds=(-200.0, 200.0))

    tester.tgt_power(priority=2, power=50.0, bounds=(-100, 100), expected=-100.0)
    tester.bounds(priority=3, expected_power=-100.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=2, expected_power=-100.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=-100.0, expected_bounds=(-150.0, 50.0))

    tester.tgt_power(priority=1, power=100.0, bounds=(100, 200), expected=100.0)
    tester.bounds(priority=1, expected_power=100.0, expected_bounds=(-150.0, 50.0))

    tester.tgt_power(priority=1, power=50.0, bounds=(50, 200), expected=100.0)
    tester.bounds(priority=1, expected_power=100.0, expected_bounds=(-150.0, 50.0))

    tester.tgt_power(priority=1, power=10.0, bounds=(10, 200), expected=60.0)
    tester.bounds(priority=1, expected_power=60.0, expected_bounds=(-150.0, 50.0))

    tester.tgt_power(priority=1, power=0.0, bounds=(-200, 200), expected=50.0)
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(-150.0, 50.0))

    tester.tgt_power(priority=1, power=None, bounds=(-200, 200), expected=50.0)
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(-150.0, 50.0))


async def test_matryoshka_simple() -> None:
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
            lower=Power.from_watts(-30.0), upper=Power.from_watts(30.0)
        ),
    )

    tester = StatefulTester(batteries, system_bounds)
    tester.tgt_power(priority=3, power=None, bounds=(-200.0, 200.0), expected=0.0)
    tester.bounds(priority=2, expected_power=0.0, expected_bounds=(-200.0, 200.0))
    tester.tgt_power(priority=1, power=None, bounds=(-200.0, 200.0), expected=0.0)
    tester.bounds(priority=2, expected_power=0.0, expected_bounds=(-200.0, 200.0))
    tester.tgt_power(priority=2, power=25.0, bounds=(25.0, 50.0), expected=30.0)
    # tester.tgt_power(priority=2, power=-10.0, bounds=(-10.0, 50.0), expected=-10.0)
    # tester.bounds(priority=1, expected_power=-10.0, expected_bounds=(0.0, 60.0))
    # tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 20.0), expected=-10.0)
    # tester.bounds(priority=0, expected_power=-10.0, expected_bounds=(0.0, 20.0))


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
    tester.bounds(priority=1, expected_power=25.0, expected_bounds=(0.0, 25.0))

    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=45.0)
    tester.bounds(priority=1, expected_power=45.0, expected_bounds=(0.0, 25.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-10.0, 50.0), expected=10.0)
    tester.bounds(priority=1, expected_power=10.0, expected_bounds=(0.0, 60.0))
    tester.bounds(priority=0, expected_power=10.0, expected_bounds=(0.0, 30.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 50.0), expected=0.0)
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(0.0, 50.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, -5.0), expected=0.0)
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(0.0, 0.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-200.0, -5.0), expected=-30.0)
    tester.bounds(priority=1, expected_power=-30.0, expected_bounds=(-190.0, 5.0))
    tester.bounds(priority=0, expected_power=-30.0, expected_bounds=(0.0, 5.0))

    tester.tgt_power(priority=2, power=None, bounds=(None, None), expected=0.0)
    tester.bounds(priority=2, expected_power=0.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=0.0, expected_bounds=(-200.0, 200.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, -5.0), expected=0.0)
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(0.0, 5.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-100.0, -5.0), expected=-30.0)
    tester.bounds(priority=0, expected_power=-30.0, expected_bounds=(-90.0, 5.0))

    tester.tgt_power(priority=1, power=-40.0, bounds=(-100.0, -35.0), expected=-40.0)
    tester.bounds(priority=0, expected_power=-40.0, expected_bounds=(-60.0, 5.0))


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
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(0.0, 25.0))

    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=45.0)
    tester.bounds(priority=1, expected_power=45.0, expected_bounds=(0.0, 25.0))

    tester.tgt_power(priority=1, power=10.0, bounds=(5.0, 10.0), expected=35.0)
    tester.bounds(priority=0, expected_power=35.0, expected_bounds=(-5.0, 0.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-10.0, 50.0), expected=0.0)
    tester.bounds(priority=1, expected_power=0.0, expected_bounds=(0.0, 60.0))
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(-5.0, 0.0))

    tester.tgt_power(priority=0, power=40, bounds=(None, None), expected=0.0)

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 50.0), expected=30.0)
    tester.bounds(priority=0, expected_power=30.0, expected_bounds=(0.0, 50.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 20.0), expected=0.0)
    tester.bounds(priority=0, expected_power=0.0, expected_bounds=(0.0, 20.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, -5.0), expected=-10.0)
    tester.bounds(priority=0, expected_power=-10.0, expected_bounds=(0.0, 0.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-200.0, -5.0), expected=-15.0)
    tester.bounds(priority=1, expected_power=-15.0, expected_bounds=(-190.0, 5.0))
    tester.bounds(priority=0, expected_power=-15.0, expected_bounds=(0.0, 5.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-100.0, -5.0), expected=-15.0)
    tester.bounds(priority=0, expected_power=-15.0, expected_bounds=(-90.0, 5.0))

    tester.tgt_power(priority=1, power=-40.0, bounds=(-100.0, -35.0), expected=-45.0)
    tester.bounds(priority=0, expected_power=-45.0, expected_bounds=(-60.0, 5.0))


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
    tester.tgt_power(priority=2, power=10.0, bounds=(-200.0, 200.0), expected=30.0)
    tester.tgt_power(priority=2, power=-10.0, bounds=(-200.0, 200.0), expected=-30.0)
    tester.tgt_power(priority=2, power=0.0, bounds=(-200.0, 200.0), expected=0.0)
    tester.tgt_power(priority=3, power=20.0, bounds=(-200.0, 200.0), expected=30.0)
    tester.tgt_power(priority=1, power=-20.0, bounds=(-200.0, 200.0), expected=0.0)
    tester.tgt_power(priority=3, power=None, bounds=(-200.0, 200.0), expected=-30.0)
    tester.tgt_power(priority=1, power=None, bounds=(-200.0, 200.0), expected=0.0)

    tester.tgt_power(priority=2, power=25.0, bounds=(25.0, 50.0), expected=30.0)
    tester.bounds(priority=2, expected_power=30.0, expected_bounds=(-200.0, 200.0))
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(0.0, 25.0))

    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=45.0)
    tester.bounds(priority=1, expected_power=45.0, expected_bounds=(0.0, 25.0))

    tester.tgt_power(priority=1, power=10.0, bounds=(5.0, 10.0), expected=35.0)
    tester.bounds(priority=0, expected_power=35.0, expected_bounds=(-5.0, 0.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-10.0, 50.0), expected=30.0)
    tester.bounds(priority=1, expected_power=30.0, expected_bounds=(0.0, 60.0))
    tester.bounds(priority=0, expected_power=30.0, expected_bounds=(-5.0, 0.0))

    tester.tgt_power(priority=1, power=40.0, bounds=(-10.0, 50.0), expected=30.0)
    tester.bounds(priority=0, expected_power=30.0, expected_bounds=(-40.0, 10.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-10.0, 20.0), expected=30.0)
    tester.bounds(priority=0, expected_power=30.0, expected_bounds=(0.0, 20.0))

    tester.tgt_power(priority=2, power=-10.0, bounds=(-200.0, -5.0), expected=-30.0)
    tester.bounds(priority=1, expected_power=-30.0, expected_bounds=(-190.0, 5.0))
    tester.bounds(priority=0, expected_power=-30.0, expected_bounds=(0.0, 15.0))

    tester.tgt_power(priority=1, power=-10.0, bounds=(-100.0, -5.0), expected=-30.0)
    tester.bounds(priority=0, expected_power=-30.0, expected_bounds=(-90.0, 5.0))

    tester.tgt_power(priority=1, power=-40.0, bounds=(-100.0, -35.0), expected=-50.0)
    tester.bounds(priority=0, expected_power=-50.0, expected_bounds=(-60.0, 5.0))


async def test_matryoshka_drop_old_proposals() -> None:
    """Tests for the power managing actor.

    With inclusion bounds, and exclusion bounds -30.0 to 30.0.
    """
    batteries = frozenset({2, 5})
    overlapping_batteries = frozenset({5, 8})

    system_bounds = _base_types.SystemBounds(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
        ),
        exclusion_bounds=timeseries.Bounds(lower=Power.zero(), upper=Power.zero()),
    )

    tester = StatefulTester(batteries, system_bounds)

    now = asyncio.get_event_loop().time()

    tester.tgt_power(priority=3, power=22.0, bounds=(22.0, 100.0), expected=22.0)

    # When a proposal is too old and hasn't been updated, it is dropped.
    tester.tgt_power(
        priority=2,
        power=25.0,
        bounds=(25.0, 50.0),
        creation_time=now - 70.0,
        expected=47.0,
    )

    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=67.0, must_send=True
    )
    tester.algorithm.drop_old_proposals(now)
    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=42.0, must_send=True
    )

    # When overwritten by a newer proposal, that proposal is not dropped.
    tester.tgt_power(
        priority=2,
        power=25.0,
        bounds=(25.0, 50.0),
        creation_time=now - 70.0,
        expected=67.0,
    )
    tester.tgt_power(
        priority=2,
        power=25.0,
        bounds=(25.0, 50.0),
        creation_time=now - 30.0,
        expected=67.0,
        must_send=True,
    )

    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=67.0, must_send=True
    )
    tester.algorithm.drop_old_proposals(now)
    tester.tgt_power(
        priority=1, power=20.0, bounds=(20.0, 50.0), expected=67.0, must_send=True
    )

    # When all proposals are too old, they are dropped, and the buckets are dropped as
    # well.  After that, sending a request for a different but overlapping bucket will
    # succeed.  And it will fail until then.
    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            "PowerManagingActor: component IDs frozenset({8, 5}) are already "
            + "part of another bucket.  Overlapping buckets are not yet supported."
        ),
    ):
        tester.tgt_power(
            priority=1,
            power=25.0,
            bounds=(25.0, 50.0),
            expected=25.0,
            must_send=True,
            batteries=overlapping_batteries,
        )

    tester.tgt_power(
        priority=1,
        power=25.0,
        bounds=(25.0, 100.0),
        creation_time=now - 70.0,
        expected=72.0,
        must_send=True,
    )
    tester.tgt_power(
        priority=2,
        power=25.0,
        bounds=(25.0, 100.0),
        creation_time=now - 70.0,
        expected=72.0,
        must_send=True,
    )
    tester.tgt_power(
        priority=3,
        power=25.0,
        bounds=(25.0, 100.0),
        creation_time=now - 70.0,
        expected=75.0,
        must_send=True,
    )

    tester.algorithm.drop_old_proposals(now)

    tester.tgt_power(
        priority=1,
        power=25.0,
        bounds=(25.0, 50.0),
        expected=25.0,
        must_send=True,
        batteries=overlapping_batteries,
    )


async def test_matryoshka_none_proposals() -> None:
    """Tests for the power managing actor.

    When a `None` proposal is received, is source id should be dropped from the bucket.
    Then if the bucket becomes empty, it should be dropped as well.
    """
    batteries = frozenset({2, 5})
    overlapping_batteries = frozenset({5, 8})

    system_bounds = _base_types.SystemBounds(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
        ),
        exclusion_bounds=timeseries.Bounds(lower=Power.zero(), upper=Power.zero()),
    )

    def ensure_overlapping_bucket_request_fails() -> None:
        with pytest.raises(
            NotImplementedError,
            match=re.escape(
                "PowerManagingActor: component IDs frozenset({8, 5}) are already "
                + "part of another bucket.  Overlapping buckets are not yet supported."
            ),
        ):
            tester.tgt_power(
                priority=1,
                power=None,
                bounds=(20.0, 50.0),
                expected=None,
                must_send=True,
                batteries=overlapping_batteries,
            )

    tester = StatefulTester(batteries, system_bounds)

    tester.tgt_power(priority=3, power=22.0, bounds=(22.0, 30.0), expected=22.0)
    tester.tgt_power(priority=2, power=25.0, bounds=(25.0, 50.0), expected=30.0)
    tester.tgt_power(priority=1, power=20.0, bounds=(20.0, 50.0), expected=30.0)

    ensure_overlapping_bucket_request_fails()
    tester.tgt_power(priority=1, power=None, bounds=(None, None), expected=30.0)
    ensure_overlapping_bucket_request_fails()
    tester.tgt_power(priority=3, power=None, bounds=(None, None), expected=25.0)
    ensure_overlapping_bucket_request_fails()
    tester.tgt_power(priority=2, power=None, bounds=(None, None), expected=None)

    # Overlapping battery bucket is dropped.
    tester.tgt_power(
        priority=1,
        power=20.0,
        bounds=(20.0, 50.0),
        expected=20.0,
        batteries=overlapping_batteries,
    )


async def test_matryoshka_shifting_limiting() -> None:
    """Tests for the power managing actor.

    With the following scenario:

    | Actor | System Limits     | Specified Limits | Desired | Adjusted | Aggregate |
    | Prio  |                   |                  | Power   | Power    | Power     |
    |-------|-------------------|------------------|---------|----------|-----------|
    | 7     | -100 kW .. 100 kW | None             | 10 kW   | 10 kW    | 10 kW     |
    | 6     | -110 kW .. 90 kW  | -110 kW .. 80 kW | 10 kW   | 10 kW    | 20 kW     |
    | 5     | -120 kW .. 70 kW  | -100 kW .. 80 kW | 80 kW   | 70 kW    | 90 kW     |
    | 4     | -170 kW .. 0 kW   | None             | -120 kW | -120 kW  | -30 kW    |
    | 3     | -50 kW .. 120 kW  | None             | 60 kW   | 60 kW    | 30 kW     |
    | 2     | -110 kW .. 60 kW  | -40 kW .. 30 kW  | 20 kW   | 20 kW    | 50 kW     |
    | 1     | -60 kW .. 10 kW   | -50 kW .. 40 kW  | 25 kW   | 10 kW    | 60 kW     |
    | 0     | -60 kW .. 0 kW    | None             | 12 kW   | 0 kW     | 60 kW     |
    | -1    | -60 kW .. 0 kW    | -40 kW .. -10 kW | -10 kW  | -10 kW   | 50 kW     |
    |-------|-------------------|------------------|---------|----------|-----------|
    |       |                   |                  |         | Power    |           |
    |       |                   |                  |         | Setpoint | 50 kW     |
    """
    batteries = frozenset({2, 5})

    system_bounds = _base_types.SystemBounds(
        timestamp=datetime.now(tz=timezone.utc),
        inclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-100.0), upper=Power.from_watts(100.0)
        ),
        exclusion_bounds=timeseries.Bounds(
            lower=Power.from_watts(-0.0), upper=Power.from_watts(0.0)
        ),
    )

    tester = StatefulTester(batteries, system_bounds)
    tester.tgt_power(priority=7, power=10.0, bounds=(None, None), expected=10.0)
    tester.bounds(priority=7, expected_power=10.0, expected_bounds=(-100.0, 100.0))
    tester.bounds(priority=6, expected_power=10.0, expected_bounds=(-110.0, 90.0))

    tester.tgt_power(priority=6, power=10.0, bounds=(-110.0, 80.0), expected=20.0)
    tester.bounds(priority=5, expected_power=20.0, expected_bounds=(-120.0, 70.0))

    tester.tgt_power(priority=5, power=80.0, bounds=(-100.0, 80.0), expected=90.0)
    tester.bounds(priority=4, expected_power=90.0, expected_bounds=(-170.0, 0.0))

    tester.tgt_power(priority=4, power=-120.0, bounds=(None, None), expected=-30.0)
    tester.bounds(priority=3, expected_power=-30.0, expected_bounds=(-50.0, 120.0))

    tester.tgt_power(priority=3, power=60.0, bounds=(None, None), expected=30.0)
    tester.bounds(priority=2, expected_power=30.0, expected_bounds=(-110.0, 60.0))

    tester.tgt_power(priority=2, power=20.0, bounds=(-40.0, 30.0), expected=50.0)
    tester.bounds(priority=1, expected_power=50.0, expected_bounds=(-60.0, 10.0))

    tester.tgt_power(priority=1, power=25.0, bounds=(-50.0, 40.0), expected=60.0)
    tester.bounds(priority=0, expected_power=60.0, expected_bounds=(-60.0, 0.0))

    tester.tgt_power(priority=0, power=12.0, bounds=(None, None), expected=60.0)
    tester.bounds(priority=-1, expected_power=60.0, expected_bounds=(-60.0, 0.0))

    tester.tgt_power(priority=-1, power=-10.0, bounds=(-40.0, -10.0), expected=50.0)
