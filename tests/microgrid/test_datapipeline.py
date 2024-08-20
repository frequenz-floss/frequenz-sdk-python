# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Basic tests for the DataPipeline."""

import asyncio
from datetime import timedelta

import async_solipsism
import pytest
import time_machine
from frequenz.client.microgrid import (
    Component,
    ComponentCategory,
    Connection,
    InverterType,
)
from pytest_mock import MockerFixture

from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.timeseries._quantities import Power
from frequenz.sdk.timeseries._resampling import ResamplerConfig

from ..utils.mock_microgrid_client import MockMicrogridClient


@pytest.fixture(autouse=True)
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


# loop time is advanced but not the system time
async def test_actors_started(
    fake_time: time_machine.Coordinates, mocker: MockerFixture
) -> None:
    """Test that the datasourcing, resampling and power distributing actors are started."""
    datapipeline = _DataPipeline(
        resampler_config=ResamplerConfig(resampling_period=timedelta(seconds=1)),
        pv_fallback_power=Power.zero(),
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

    mock_client = MockMicrogridClient(
        {
            Component(1, ComponentCategory.GRID),
            Component(4, ComponentCategory.INVERTER, InverterType.BATTERY),
            Component(15, ComponentCategory.BATTERY),
        },
        connections={Connection(1, 4), Connection(4, 15)},
    )
    mock_client.initialize(mocker)

    datapipeline.new_battery_pool(priority=5)

    assert datapipeline._battery_power_wrapper._power_distributing_actor is not None
    await asyncio.sleep(1)
    assert datapipeline._battery_power_wrapper._power_distributing_actor.is_running

    await datapipeline._stop()
