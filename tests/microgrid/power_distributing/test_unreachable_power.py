# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the power distributor's unreachable-power handling."""

from __future__ import annotations

from collections import abc
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest
from frequenz.channels import Broadcast, LatestValueCache, Receiver
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Power, Quantity
from pytest_mock import MockerFixture

from frequenz.sdk.microgrid._old_component_data import InverterData
from frequenz.sdk.microgrid._power_distributing import ComponentPoolStatus
from frequenz.sdk.microgrid._power_distributing._component_managers._component_manager import (
    ComponentManager,
    _UnreachablePower,
)
from frequenz.sdk.microgrid._power_distributing._component_managers._pv_inverter_manager._pv_inverter_manager import (  # noqa: E501
    PVManager,
)
from frequenz.sdk.microgrid._power_distributing._component_pool_status_tracker import (
    ComponentPoolStatusTracker,
)
from frequenz.sdk.microgrid._power_distributing.request import Request
from frequenz.sdk.microgrid._power_distributing.result import Result, Success
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.formulas._formula_pool import FormulaPool

from ...timeseries.mock_microgrid import MockMicrogrid

# The tests drive the managers through their internal hooks and caches.
# pylint: disable=protected-access

_PV_MODULE = (
    "frequenz.sdk.microgrid._power_distributing._component_managers"
    "._pv_inverter_manager._pv_inverter_manager"
)


def _formula_for(component_ids: abc.Set[ComponentId]) -> str:
    """Return a deterministic formula string for a set of components."""
    return "power:" + ",".join(str(c) for c in sorted(component_ids))


class _PoolFormula:
    """A pooled-formula stub backed by a real broadcast channel."""

    def __init__(self) -> None:
        self._channel = Broadcast[Sample[Quantity]](name="unreachable-stub")

    def new_receiver(self) -> Receiver[Sample[Quantity]]:
        """Return a receiver for the formula's (empty) output stream."""
        return self._channel.new_receiver()


class _RecordingPool:
    """A `FormulaPool` stub that records its lifecycle calls."""

    def __init__(self) -> None:
        self.created: list[str] = []
        self.stopped: list[str] = []
        self.stop_all_count = 0

    def from_string(self, formula_str: str, metric: Metric) -> _PoolFormula:
        """Record the creation and return a feedable stub formula."""
        # pylint: disable=unused-argument
        self.created.append(formula_str)
        return _PoolFormula()

    async def stop_string_formula(self, formula_str: str, metric: Metric) -> None:
        """Record that a single string formula was stopped."""
        # pylint: disable=unused-argument
        self.stopped.append(formula_str)

    async def stop(self) -> None:
        """Record that the whole pool was stopped."""
        self.stop_all_count += 1


class _MechanismManager(ComponentManager):
    """A minimal manager that exercises the shared unreachable-power mechanism."""

    def component_ids(self) -> abc.Set[ComponentId]:
        """Return no components; unused by these tests."""
        return set()

    async def start(self) -> None:
        """Do nothing; unused by these tests."""

    async def distribute_power(self, request: Request) -> None:
        """Do nothing; unused by these tests."""
        # pylint: disable=unused-argument

    async def stop(self) -> None:
        """Stop the unreachable-power subscriptions and owned pool."""
        await self._stop_all_unreachable_power_subscriptions()

    def _unreachable_power_formula(self, component_ids: abc.Set[ComponentId]) -> str:
        """Return a deterministic formula string for the given components."""
        return _formula_for(component_ids)


class TestUnreachablePowerMechanism:
    """Tests for the shared `ComponentManager` unreachable-power logic."""

    async def test_subscription_restarts_when_unreachable_set_changes(
        self, mocker: MockerFixture
    ) -> None:
        """Subscriptions are restarted on set changes and torn down when reachable."""
        pool = _RecordingPool()
        mocker.patch(
            "frequenz.sdk.microgrid._data_pipeline._new_formula_pool",
            return_value=pool,
        )
        manager = _MechanismManager()
        requested = {ComponentId(1), ComponentId(2), ComponentId(3)}

        # First request: only component 3 is unreachable.
        await manager._subscribe_to_unreachable_power(
            requested, {ComponentId(1), ComponentId(2)}
        )
        assert pool.created == [_formula_for({ComponentId(3)})]
        assert not pool.stopped

        # Re-subscribing with the same unreachable subset is a no-op.
        await manager._subscribe_to_unreachable_power(
            requested, {ComponentId(1), ComponentId(2)}
        )
        assert pool.created == [_formula_for({ComponentId(3)})]

        # The unreachable subset grows to {2, 3}: the old formula is stopped and a
        # new one is created.
        await manager._subscribe_to_unreachable_power(requested, {ComponentId(1)})
        assert pool.stopped == [_formula_for({ComponentId(3)})]
        assert pool.created == [
            _formula_for({ComponentId(3)}),
            _formula_for({ComponentId(2), ComponentId(3)}),
        ]

        # Everything is reachable again: the subscription is torn down.
        await manager._subscribe_to_unreachable_power(requested, requested)
        assert pool.stopped == [
            _formula_for({ComponentId(3)}),
            _formula_for({ComponentId(2), ComponentId(3)}),
        ]
        assert not manager._unreachable_power_subscriptions

        # Manager shutdown stops the owned formula pool.
        await manager.stop()
        assert pool.stop_all_count == 1

    async def test_failed_cache_stop_still_stops_formula(
        self, mocker: MockerFixture
    ) -> None:
        """A failing cache teardown must not leave the formula running."""
        pool = _RecordingPool()
        manager = _MechanismManager()
        manager._unreachable_power_pool = cast(FormulaPool, pool)

        failing_cache = mocker.MagicMock(spec=LatestValueCache)
        failing_cache.stop = mocker.AsyncMock(side_effect=RuntimeError("cache boom"))
        formula = _formula_for({ComponentId(2)})
        subscription = _UnreachablePower(
            component_ids={ComponentId(2)},
            formula=formula,
            cache=cast(LatestValueCache[Sample[Power]], failing_cache),
        )

        with pytest.raises(RuntimeError, match="cache boom"):
            await manager._stop_unreachable_power_subscription(subscription)

        # Despite the cache failure, the formula was stopped via the finally block.
        assert pool.stopped == [formula]


def _pv_cache(
    mocker: MockerFixture, lower_bound_w: float
) -> LatestValueCache[InverterData]:
    """Build a PV inverter data cache with a fixed inclusion lower bound."""
    cache = mocker.MagicMock(spec=LatestValueCache)
    cache.has_value.return_value = True
    data = mocker.MagicMock(spec=InverterData)
    data.active_power_inclusion_lower_bound = lower_bound_w
    cache.get.return_value = data
    return cast(LatestValueCache[InverterData], cache)


async def _make_pv_manager(
    mocker: MockerFixture,
    *,
    working: abc.Set[ComponentId],
    lower_bound_w: float,
) -> tuple[PVManager, AsyncMock]:
    """Build a PV manager with mocked status tracking, caches and API calls."""
    tracker = mocker.MagicMock(spec=ComponentPoolStatusTracker)
    tracker.get_working_components.return_value = working
    tracker.stop = mocker.AsyncMock()
    mocker.patch(f"{_PV_MODULE}.ComponentPoolStatusTracker", return_value=tracker)

    status_channel = Broadcast[ComponentPoolStatus](name="pv_status")
    results_channel = Broadcast[Result](name="pv_results")
    manager = PVManager(
        component_pool_status_sender=status_channel.new_sender(),
        results_sender=results_channel.new_sender(),
        api_power_request_timeout=timedelta(seconds=1),
    )
    manager._component_data_caches = {
        inv: _pv_cache(mocker, lower_bound_w) for inv in working
    }
    set_power = mocker.patch(
        f"{_PV_MODULE}._set_component_power",
        new=mocker.AsyncMock(return_value=mocker.MagicMock(spec=Success)),
    )
    return manager, set_power


class TestPVUnreachablePowerDistribution:
    """Tests for how the PV manager applies measured unreachable power."""

    async def test_subtracts_unreachable_power_from_target(
        self, mocker: MockerFixture
    ) -> None:
        """Measured unreachable PV power is subtracted from the reachable target."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_solar_inverters(3)
        async with mockgrid:
            pv_ids = set(mockgrid.pv_inverter_ids)
            working = set(sorted(pv_ids)[:2])
            manager, set_power = await _make_pv_manager(
                mocker, working=working, lower_bound_w=-500.0
            )
            mocker.patch.object(
                manager, "_subscribe_to_unreachable_power", mocker.AsyncMock()
            )
            mocker.patch.object(
                manager, "_unreachable_power", return_value=Power.from_watts(-300.0)
            )

            await manager.distribute_power(
                Request(power=Power.from_watts(-1000.0), component_ids=pv_ids)
            )

        kwargs = set_power.call_args.kwargs
        assert kwargs["target_power"].isclose(Power.from_watts(-1000.0))
        allocated = Power.zero()
        for power in kwargs["allocations"].values():
            allocated += power
        # 1000 W requested minus the 300 W already produced by the unreachable
        # inverter leaves 700 W to split across the two reachable inverters.
        assert allocated.isclose(Power.from_watts(-700.0))

    async def test_unreachable_power_exceeding_target(
        self, mocker: MockerFixture
    ) -> None:
        """When unreachable production exceeds the target, nothing is requested."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_solar_inverters(3)
        async with mockgrid:
            pv_ids = set(mockgrid.pv_inverter_ids)
            working = set(sorted(pv_ids)[:2])
            manager, set_power = await _make_pv_manager(
                mocker, working=working, lower_bound_w=-500.0
            )
            mocker.patch.object(
                manager, "_subscribe_to_unreachable_power", mocker.AsyncMock()
            )
            mocker.patch.object(
                manager, "_unreachable_power", return_value=Power.from_watts(-1200.0)
            )

            await manager.distribute_power(
                Request(power=Power.from_watts(-1000.0), component_ids=pv_ids)
            )

        kwargs = set_power.call_args.kwargs
        # The unreachable inverters already over-produce, so the reachable inverters
        # are asked for nothing and the surplus is reported as remaining power.
        assert all(power == Power.zero() for power in kwargs["allocations"].values())
        assert kwargs["remaining_power"].isclose(Power.from_watts(200.0))
