# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the steam boiler manager's power distribution."""

from __future__ import annotations

from collections import abc
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

from frequenz.channels import Broadcast, LatestValueCache
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power
from pytest_mock import MockerFixture

from frequenz.sdk.microgrid import connection_manager
from frequenz.sdk.microgrid._old_component_data import SteamBoilerData
from frequenz.sdk.microgrid._power_distributing import ComponentPoolStatus
from frequenz.sdk.microgrid._power_distributing._component_managers._steam_boiler_manager._steam_boiler_manager import (  # noqa: E501
    SteamBoilerManager,
)
from frequenz.sdk.microgrid._power_distributing._component_pool_status_tracker import (
    ComponentPoolStatusTracker,
)
from frequenz.sdk.microgrid._power_distributing.request import Request
from frequenz.sdk.microgrid._power_distributing.result import Result, Success

from ...timeseries.mock_microgrid import MockMicrogrid

# The tests drive the manager through its internal hooks and caches.
# pylint: disable=protected-access

_STEAM_BOILER_MODULE = (
    "frequenz.sdk.microgrid._power_distributing._component_managers"
    "._steam_boiler_manager._steam_boiler_manager"
)


def _steam_boiler_cache(
    mocker: MockerFixture, *, lower_bound_w: float, upper_bound_w: float
) -> LatestValueCache[SteamBoilerData]:
    """Build a steam boiler data cache with fixed inclusion bounds."""
    cache = mocker.MagicMock(spec=LatestValueCache)
    cache.has_value.return_value = True
    data = mocker.MagicMock(spec=SteamBoilerData)
    data.active_power_inclusion_lower_bound = lower_bound_w
    data.active_power_inclusion_upper_bound = upper_bound_w
    cache.get.return_value = data
    return cast(LatestValueCache[SteamBoilerData], cache)


async def _make_manager(
    mocker: MockerFixture,
    *,
    working: abc.Set[ComponentId],
    bounds: dict[ComponentId, tuple[float, float]],
) -> tuple[SteamBoilerManager, AsyncMock]:
    """Build a steam boiler manager with mocked status, caches and API calls."""
    tracker = mocker.MagicMock(spec=ComponentPoolStatusTracker)
    tracker.get_working_components.return_value = working
    tracker.stop = mocker.AsyncMock()
    mocker.patch(
        f"{_STEAM_BOILER_MODULE}.ComponentPoolStatusTracker", return_value=tracker
    )

    status_channel = Broadcast[ComponentPoolStatus](name="steam_boiler_status")
    results_channel = Broadcast[Result](name="steam_boiler_results")
    manager = SteamBoilerManager(
        component_pool_status_sender=status_channel.new_sender(),
        results_sender=results_channel.new_sender(),
        api_power_request_timeout=timedelta(seconds=1),
    )
    manager._component_data_caches = {
        boiler: _steam_boiler_cache(
            mocker,
            lower_bound_w=bounds[boiler][0],
            upper_bound_w=bounds[boiler][1],
        )
        for boiler in working
    }
    set_power = mocker.patch(
        f"{_STEAM_BOILER_MODULE}._set_component_power",
        new=mocker.AsyncMock(return_value=mocker.MagicMock(spec=Success)),
    )
    return manager, set_power


class TestSteamBoilerUnreachablePowerDistribution:
    """Tests for how the manager applies measured unreachable power."""

    async def test_subtracts_unreachable_power_from_target(
        self, mocker: MockerFixture
    ) -> None:
        """Measured unreachable boiler power is subtracted from the target."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(3)
        async with mockgrid:
            boiler_ids = set(mockgrid.steam_boiler_ids)
            working = set(sorted(boiler_ids)[:2])
            manager, set_power = await _make_manager(
                mocker,
                working=working,
                bounds={boiler: (0.0, 10000.0) for boiler in working},
            )
            mocker.patch.object(
                manager, "_subscribe_to_unreachable_power", mocker.AsyncMock()
            )
            mocker.patch.object(
                manager, "_unreachable_power", return_value=Power.from_watts(300.0)
            )

            await manager.distribute_power(
                Request(power=Power.from_watts(1000.0), component_ids=boiler_ids)
            )

        kwargs = set_power.call_args.kwargs
        assert kwargs["target_power"].isclose(Power.from_watts(1000.0))
        allocated = Power.zero()
        for power in kwargs["allocations"].values():
            allocated += power
        # 1000 W requested minus the 300 W already drawn by the unreachable
        # boiler leaves 700 W to split across the two reachable boilers.
        assert allocated.isclose(Power.from_watts(700.0))

    async def test_unreachable_power_exceeding_target(
        self, mocker: MockerFixture
    ) -> None:
        """When unreachable draw exceeds the target, nothing is requested."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(3)
        async with mockgrid:
            boiler_ids = set(mockgrid.steam_boiler_ids)
            working = set(sorted(boiler_ids)[:2])
            manager, set_power = await _make_manager(
                mocker,
                working=working,
                bounds={boiler: (0.0, 10000.0) for boiler in working},
            )
            mocker.patch.object(
                manager, "_subscribe_to_unreachable_power", mocker.AsyncMock()
            )
            mocker.patch.object(
                manager, "_unreachable_power", return_value=Power.from_watts(1200.0)
            )

            await manager.distribute_power(
                Request(power=Power.from_watts(1000.0), component_ids=boiler_ids)
            )

        kwargs = set_power.call_args.kwargs
        # The unreachable boilers already draw more than the target, so the
        # reachable boilers are asked for nothing and the overdraw is reported
        # as remaining power.
        assert all(power == Power.zero() for power in kwargs["allocations"].values())
        assert kwargs["remaining_power"].isclose(Power.from_watts(-200.0))


class TestSteamBoilerMinimumPowerDistribution:
    """Tests for how the manager respects per-boiler minimum power bounds."""

    async def test_shares_below_minimum_are_raised(self, mocker: MockerFixture) -> None:
        """A share below a boiler's minimum is raised to the minimum."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(3)
        async with mockgrid:
            boiler_ids = sorted(mockgrid.steam_boiler_ids)
            fixed_1, fixed_2, flexible = boiler_ids
            manager, set_power = await _make_manager(
                mocker,
                working=set(boiler_ids),
                bounds={
                    fixed_1: (4000.0, 4000.0),
                    fixed_2: (4000.0, 4000.0),
                    flexible: (0.0, 20000.0),
                },
            )
            mocker.patch.object(
                manager, "_subscribe_to_unreachable_power", mocker.AsyncMock()
            )
            mocker.patch.object(manager, "_unreachable_power", return_value=None)

            await manager.distribute_power(
                Request(power=Power.from_watts(9000.0), component_ids=set(boiler_ids))
            )

        allocations = set_power.call_args.kwargs["allocations"]
        # The fixed-power boilers get their 4 kW minimum instead of an illegal
        # 3 kW equal share; the flexible boiler takes the remaining 1 kW.
        assert allocations[fixed_1].isclose(Power.from_watts(4000.0))
        assert allocations[fixed_2].isclose(Power.from_watts(4000.0))
        assert allocations[flexible].isclose(Power.from_watts(1000.0))

    async def test_boiler_kept_off_when_minimum_unaffordable(
        self, mocker: MockerFixture
    ) -> None:
        """A boiler whose minimum exceeds the remaining budget stays off."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(3)
        async with mockgrid:
            boiler_ids = sorted(mockgrid.steam_boiler_ids)
            fixed_1, fixed_2, flexible = boiler_ids
            manager, set_power = await _make_manager(
                mocker,
                working=set(boiler_ids),
                bounds={
                    fixed_1: (4000.0, 4000.0),
                    fixed_2: (4000.0, 4000.0),
                    flexible: (0.0, 20000.0),
                },
            )
            mocker.patch.object(
                manager, "_subscribe_to_unreachable_power", mocker.AsyncMock()
            )
            mocker.patch.object(manager, "_unreachable_power", return_value=None)

            await manager.distribute_power(
                Request(power=Power.from_watts(3000.0), component_ids=set(boiler_ids))
            )

        allocations = set_power.call_args.kwargs["allocations"]
        # 3 kW can't cover either fixed boiler's 4 kW minimum, so both stay
        # off and the flexible boiler takes the full target.
        assert allocations[fixed_1] == Power.zero()
        assert allocations[fixed_2] == Power.zero()
        assert allocations[flexible].isclose(Power.from_watts(3000.0))

    async def test_unaffordable_minimum_strands_excess(
        self, mocker: MockerFixture
    ) -> None:
        """Power freed by a kept-off boiler is not redistributed.

        The single-pass allocation visits boilers in ascending upper-bound
        order, so power that a kept-off high-minimum boiler can't take is
        reported as excess instead of topping up earlier boilers.  This test
        pins that known limitation.
        """
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(2)
        async with mockgrid:
            boiler_ids = sorted(mockgrid.steam_boiler_ids)
            small, high_minimum = boiler_ids
            manager, set_power = await _make_manager(
                mocker,
                working=set(boiler_ids),
                bounds={
                    small: (0.0, 1000.0),
                    high_minimum: (4000.0, 10000.0),
                },
            )
            mocker.patch.object(
                manager, "_subscribe_to_unreachable_power", mocker.AsyncMock()
            )
            mocker.patch.object(manager, "_unreachable_power", return_value=None)

            await manager.distribute_power(
                Request(power=Power.from_watts(4500.0), component_ids=set(boiler_ids))
            )

        kwargs = set_power.call_args.kwargs
        assert kwargs["allocations"][small].isclose(Power.from_watts(1000.0))
        assert kwargs["allocations"][high_minimum] == Power.zero()
        assert kwargs["remaining_power"].isclose(Power.from_watts(3500.0))

    async def test_inconsistent_bounds_keep_boiler_off(
        self, mocker: MockerFixture
    ) -> None:
        """A boiler reporting a minimum above its maximum is kept off."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(1)
        async with mockgrid:
            boiler = mockgrid.steam_boiler_ids[0]
            manager, set_power = await _make_manager(
                mocker,
                working={boiler},
                bounds={boiler: (2000.0, 1000.0)},
            )
            mocker.patch.object(
                manager, "_subscribe_to_unreachable_power", mocker.AsyncMock()
            )
            mocker.patch.object(manager, "_unreachable_power", return_value=None)

            await manager.distribute_power(
                Request(power=Power.from_watts(5000.0), component_ids={boiler})
            )

        # The bump to the minimum must never push the allocation past the
        # reported maximum, so the boiler is kept off instead.
        assert set_power.call_args.kwargs["allocations"][boiler] == Power.zero()


class TestSteamBoilerManagerWiring:
    """Tests for the manager's unreachable-power wiring."""

    async def test_unreachable_power_formula_delegates_to_graph(
        self, mocker: MockerFixture
    ) -> None:
        """The unreachable-power formula comes from the steam boiler formula."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(2)
        async with mockgrid:
            boiler_ids = set(mockgrid.steam_boiler_ids)
            manager, _ = await _make_manager(
                mocker,
                working=boiler_ids,
                bounds={boiler: (0.0, 10000.0) for boiler in boiler_ids},
            )
            graph = connection_manager.get().component_graph
            formula = manager._unreachable_power_formula(boiler_ids)

            # The compiled graph object can't be patched, so pin the
            # delegation by comparing against the graph's own output.
            assert formula == graph.steam_boiler_formula(boiler_ids)
            for boiler in boiler_ids:
                assert f"#{int(boiler)}" in formula

    async def test_stop_tears_down_unreachable_power_subscriptions(
        self, mocker: MockerFixture
    ) -> None:
        """Stopping the manager stops the unreachable-power subscriptions."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(1)
        async with mockgrid:
            boiler = mockgrid.steam_boiler_ids[0]
            manager, _ = await _make_manager(
                mocker,
                working={boiler},
                bounds={boiler: (0.0, 10000.0)},
            )
            stop_subscriptions = mocker.patch.object(
                manager,
                "_stop_all_unreachable_power_subscriptions",
                mocker.AsyncMock(),
            )

            await manager.stop()

        stop_subscriptions.assert_awaited_once()
