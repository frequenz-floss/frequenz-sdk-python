# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Basic tests for the DataPipeline."""

import asyncio
import re
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import async_solipsism
import pytest
import time_machine
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    AcEvCharger,
    BatteryInverter,
    ComponentConnection,
    GridConnectionPoint,
    LiIonBattery,
    SolarInverter,
    SteamBoiler,
)
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid import _data_pipeline
from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.timeseries import ResamplerConfig2
from frequenz.sdk.timeseries.component_pool import ComponentPool

from ..timeseries.mock_microgrid import MockMicrogrid
from ..utils.mock_microgrid_client import MockMicrogridClient


@pytest.fixture(autouse=True)
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


_MICROGRID_ID = MicrogridId(1)


# loop time is advanced but not the system time
async def test_actors_started(
    fake_time: time_machine.Coordinates, mocker: MockerFixture
) -> None:
    """Test that the datasourcing, resampling and power distributing actors are started."""
    datapipeline = _DataPipeline(
        resampler_config=ResamplerConfig2(resampling_period=timedelta(seconds=1))
    )
    await asyncio.sleep(1)

    # pylint: disable=protected-access
    assert datapipeline._data_sourcing_actor is None
    assert datapipeline._resampling_actor is None
    assert datapipeline._battery_power_wrapper._power_distributing_actor is None

    datapipeline.logical_meter()

    assert datapipeline._data_sourcing_actor is not None
    assert datapipeline._data_sourcing_actor.actor is not None
    await asyncio.sleep(1)
    fake_time.shift(timedelta(seconds=1))
    assert datapipeline._data_sourcing_actor.actor.is_running

    assert datapipeline._resampling_actor is not None
    assert datapipeline._resampling_actor.actor is not None
    assert datapipeline._resampling_actor.actor.is_running

    assert datapipeline._battery_power_wrapper._power_distributing_actor is None

    grid_1 = GridConnectionPoint(
        id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
    )
    bat_inverter_4 = BatteryInverter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
    battery_15 = LiIonBattery(id=ComponentId(15), microgrid_id=_MICROGRID_ID)
    pv_inverter_7 = SolarInverter(id=ComponentId(7), microgrid_id=_MICROGRID_ID)
    ev_charger_9 = AcEvCharger(id=ComponentId(9), microgrid_id=_MICROGRID_ID)

    mock_client = MockMicrogridClient(
        components={grid_1, bat_inverter_4, battery_15, pv_inverter_7, ev_charger_9},
        connections={
            ComponentConnection(source=grid_1.id, destination=bat_inverter_4.id),
            ComponentConnection(source=bat_inverter_4.id, destination=battery_15.id),
            ComponentConnection(source=grid_1.id, destination=pv_inverter_7.id),
            ComponentConnection(source=grid_1.id, destination=ev_charger_9.id),
        },
    )
    mock_client.initialize(mocker)

    _data_pipeline._DATA_PIPELINE = datapipeline

    datapipeline.new_battery_pool(priority=5)
    datapipeline.new_pv_pool(priority=3)
    datapipeline.new_ev_charger_pool(priority=4)

    datapipeline.new_battery_pool(priority=1, component_ids={ComponentId(15)})
    datapipeline.new_pv_pool(priority=2, component_ids={ComponentId(7)})
    datapipeline.new_ev_charger_pool(priority=2, component_ids={ComponentId(9)})

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Unable to create BatteryPool. These component IDs are either not "
            + "batteries or are unknown: frozenset({ComponentId(4)})"
        ),
    ):
        datapipeline.new_battery_pool(priority=2, component_ids={ComponentId(4)})

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Unable to create PVPool. These component IDs are either not PV "
            + "inverters or are unknown: frozenset({ComponentId(1)})"
        ),
    ):
        datapipeline.new_pv_pool(priority=4, component_ids={ComponentId(1)})

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Unable to create EVChargerPool. These component IDs are either "
            + "not EV chargers or are unknown: frozenset({ComponentId(4)})"
        ),
    ):
        datapipeline.new_ev_charger_pool(priority=5, component_ids={ComponentId(4)})

    assert datapipeline._battery_power_wrapper._power_distributing_actor is not None
    await asyncio.sleep(1)
    assert datapipeline._battery_power_wrapper._power_distributing_actor.is_running

    await datapipeline._stop()


# pylint: disable-next=too-many-locals
async def test_empty_component_ids_matches_none_in_either_call_order(
    mocker: MockerFixture,
) -> None:
    """`set()` and `None` must give the same pool, no matter which comes first.

    Regression test for
    https://github.com/frequenz-floss/frequenz-sdk-python/issues/1474

    The reference store for a set of component IDs is created once and then
    reused for later calls with the same (normalized) IDs. Before the fix, an
    empty set and `None` normalized to the same cache key, but were resolved
    differently inside the reference store: a `None` pool got all components,
    while a `set()` pool got none. This meant whichever call came first decided
    what BOTH calls got.
    """
    datapipeline = _DataPipeline(
        resampler_config=ResamplerConfig2(resampling_period=timedelta(seconds=1))
    )

    grid_1 = GridConnectionPoint(
        id=ComponentId(1), microgrid_id=_MICROGRID_ID, rated_fuse_current=10_000
    )
    bat_inverter_4 = BatteryInverter(id=ComponentId(4), microgrid_id=_MICROGRID_ID)
    battery_15 = LiIonBattery(id=ComponentId(15), microgrid_id=_MICROGRID_ID)
    pv_inverter_7 = SolarInverter(id=ComponentId(7), microgrid_id=_MICROGRID_ID)
    ev_charger_9 = AcEvCharger(id=ComponentId(9), microgrid_id=_MICROGRID_ID)
    steam_boiler_11 = SteamBoiler(id=ComponentId(11), microgrid_id=_MICROGRID_ID)

    mock_client = MockMicrogridClient(
        components={
            grid_1,
            bat_inverter_4,
            battery_15,
            pv_inverter_7,
            ev_charger_9,
            steam_boiler_11,
        },
        connections={
            ComponentConnection(source=grid_1.id, destination=bat_inverter_4.id),
            ComponentConnection(source=bat_inverter_4.id, destination=battery_15.id),
            ComponentConnection(source=grid_1.id, destination=pv_inverter_7.id),
            ComponentConnection(source=grid_1.id, destination=ev_charger_9.id),
            ComponentConnection(source=grid_1.id, destination=steam_boiler_11.id),
        },
    )
    mock_client.initialize(mocker)

    # pylint: disable=protected-access
    _data_pipeline._DATA_PIPELINE = datapipeline

    all_batteries = frozenset({ComponentId(15)})
    all_pv_inverters = frozenset({ComponentId(7)})
    all_ev_chargers = frozenset({ComponentId(9)})
    all_steam_boilers = frozenset({ComponentId(11)})

    # `set()` first, then `None`: both calls must get all components.
    battery_empty_first = datapipeline.new_battery_pool(
        priority=10, component_ids=set()
    )
    battery_none_second = datapipeline.new_battery_pool(priority=11)
    assert battery_empty_first.component_ids == all_batteries
    assert battery_none_second.component_ids == all_batteries

    pv_empty_first = datapipeline.new_pv_pool(priority=10, component_ids=set())
    pv_none_second = datapipeline.new_pv_pool(priority=11)
    assert pv_empty_first.component_ids == all_pv_inverters
    assert pv_none_second.component_ids == all_pv_inverters

    ev_empty_first = datapipeline.new_ev_charger_pool(priority=10, component_ids=set())
    ev_none_second = datapipeline.new_ev_charger_pool(priority=11)
    assert ev_empty_first.component_ids == all_ev_chargers
    assert ev_none_second.component_ids == all_ev_chargers

    steam_boiler_empty_first = datapipeline.new_steam_boiler_pool(
        priority=10, component_ids=set()
    )
    steam_boiler_none_second = datapipeline.new_steam_boiler_pool(priority=11)
    assert steam_boiler_empty_first.component_ids == all_steam_boilers
    assert steam_boiler_none_second.component_ids == all_steam_boilers

    # `None` first, then `set()`: both calls must still get all components.
    battery_none_first = datapipeline.new_battery_pool(priority=20)
    battery_empty_second = datapipeline.new_battery_pool(
        priority=21, component_ids=set()
    )
    assert battery_none_first.component_ids == all_batteries
    assert battery_empty_second.component_ids == all_batteries

    pv_none_first = datapipeline.new_pv_pool(priority=20)
    pv_empty_second = datapipeline.new_pv_pool(priority=21, component_ids=set())
    assert pv_none_first.component_ids == all_pv_inverters
    assert pv_empty_second.component_ids == all_pv_inverters

    ev_none_first = datapipeline.new_ev_charger_pool(priority=20)
    ev_empty_second = datapipeline.new_ev_charger_pool(priority=21, component_ids=set())
    assert ev_none_first.component_ids == all_ev_chargers
    assert ev_empty_second.component_ids == all_ev_chargers

    steam_boiler_none_first = datapipeline.new_steam_boiler_pool(priority=20)
    steam_boiler_empty_second = datapipeline.new_steam_boiler_pool(
        priority=21, component_ids=set()
    )
    assert steam_boiler_none_first.component_ids == all_steam_boilers
    assert steam_boiler_empty_second.component_ids == all_steam_boilers

    await datapipeline._stop()


_POOL_TYPES = [
    pytest.param(
        microgrid.new_battery_pool,
        MockMicrogrid.add_batteries,
        "battery_ids",
        id="battery",
    ),
    pytest.param(
        microgrid.new_pv_pool,
        MockMicrogrid.add_solar_inverters,
        "pv_inverter_ids",
        id="pv",
    ),
    pytest.param(
        microgrid.new_ev_charger_pool,
        MockMicrogrid.add_ev_chargers,
        "evc_ids",
        id="ev_charger",
    ),
    pytest.param(
        microgrid.new_steam_boiler_pool,
        MockMicrogrid.add_steam_boilers,
        "steam_boiler_ids",
        id="steam_boiler",
    ),
]
"""The pool factory, the mock microgrid method that adds its components, and the
mock microgrid attribute that holds their IDs, for each pool type."""


@pytest.mark.parametrize("new_pool, add_components, ids_attr", _POOL_TYPES)
async def test_pools_for_all_components_share_reference_stores(
    mocker: MockerFixture,
    new_pool: Callable[..., ComponentPool[Any, Any]],
    add_components: Callable[[MockMicrogrid, int], None],
    ids_attr: str,
) -> None:
    """Test that a pool without IDs and a pool given all IDs share a reference store.

    The power manager creates pools for the explicit IDs it gets in proposals and
    bounds subscriptions.  A pool created without IDs, or with an empty set, must
    share its reference store with the pool the power manager creates for the same
    components, instead of tracking the power bounds of those components on its own.
    """
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    add_components(mockgrid, 2)
    async with mockgrid:
        all_pool = new_pool(priority=5)
        explicit_pool = new_pool(
            priority=6, component_ids=set(getattr(mockgrid, ids_attr))
        )
        empty_set_pool = new_pool(priority=7, component_ids=set())
        # pylint: disable=protected-access
        assert explicit_pool._pool_ref_store is all_pool._pool_ref_store
        assert empty_set_pool._pool_ref_store is all_pool._pool_ref_store
