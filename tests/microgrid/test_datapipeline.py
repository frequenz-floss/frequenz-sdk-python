# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Basic tests for the DataPipeline."""

import asyncio
from datetime import timedelta

import async_solipsism
import pytest
import time_machine
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    BatteryInverter,
    ComponentConnection,
    GridConnectionPoint,
    LiIonBattery,
)
from pytest_mock import MockerFixture

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
    mock_client = MockMicrogridClient(
        components={grid_1, bat_inverter_4, battery_15},
        connections={
            ComponentConnection(source=grid_1.id, destination=bat_inverter_4.id),
            ComponentConnection(source=bat_inverter_4.id, destination=battery_15.id),
        },
    )
    mock_client.initialize(mocker)

    datapipeline.new_battery_pool(priority=5)

    assert datapipeline._battery_power_wrapper._power_distributing_actor is not None
    await asyncio.sleep(1)
    assert datapipeline._battery_power_wrapper._power_distributing_actor.is_running

    await datapipeline._stop()
