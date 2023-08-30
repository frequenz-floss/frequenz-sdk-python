# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Basic tests for the DataPipeline."""

import asyncio
from datetime import timedelta
from typing import Iterator

import async_solipsism
import pytest
from pytest_mock import MockerFixture

from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory, InverterType
from frequenz.sdk.timeseries._resampling import ResamplerConfig

from ..utils.mock_microgrid_client import MockMicrogridClient


@pytest.fixture
def event_loop() -> Iterator[async_solipsism.EventLoop]:
    """Replace the loop with one that doesn't interact with the outside world."""
    loop = async_solipsism.EventLoop()
    asyncio.set_event_loop(loop)  # Set the loop as default
    yield loop
    loop.close()


async def test_actors_started(mocker: MockerFixture) -> None:
    """Test that the datasourcing, resampling and power distributing actors are started."""
    datapipeline = _DataPipeline(
        resampler_config=ResamplerConfig(resampling_period=timedelta(seconds=1))
    )
    await asyncio.sleep(1)

    # pylint: disable=protected-access
    assert datapipeline._data_sourcing_actor is None
    assert datapipeline._resampling_actor is None
    assert datapipeline._power_distributing_actor is None

    datapipeline.logical_meter()

    assert datapipeline._data_sourcing_actor is not None
    assert datapipeline._data_sourcing_actor.actor is not None
    await asyncio.sleep(1)
    assert datapipeline._data_sourcing_actor.actor.is_running

    assert datapipeline._resampling_actor is not None
    assert datapipeline._resampling_actor.actor is not None
    assert datapipeline._resampling_actor.actor.is_running

    assert datapipeline._power_distributing_actor is None

    mock_client = MockMicrogridClient(
        set(
            [
                Component(1, ComponentCategory.GRID),
                Component(4, ComponentCategory.INVERTER, InverterType.BATTERY),
                Component(15, ComponentCategory.BATTERY),
            ]
        ),
        connections=set([Connection(1, 4), Connection(4, 15)]),
    )
    mock_client.initialize(mocker)

    datapipeline.battery_pool()

    assert datapipeline._power_distributing_actor is not None
    await asyncio.sleep(1)
    assert datapipeline._power_distributing_actor.is_running

    await datapipeline._stop()
