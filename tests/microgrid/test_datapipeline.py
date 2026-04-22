# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Basic tests for the DataPipeline."""

import asyncio
import re
from datetime import timedelta

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
)
from pytest_mock import MockerFixture

from frequenz.sdk.microgrid import _data_pipeline
from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.timeseries import ResamplerConfig2

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
