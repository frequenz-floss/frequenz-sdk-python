# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Mock fixture for mocking the microgrid along with streaming."""

from __future__ import annotations

import asyncio
import dataclasses
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncIterator

from frequenz.channels import Sender
from frequenz.client.microgrid import ComponentCategory
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig
from frequenz.sdk.actor.power_distributing import ComponentPoolStatus
from frequenz.sdk.microgrid.component_graph import _MicrogridComponentGraph

from ..timeseries.mock_microgrid import MockMicrogrid
from ..utils.component_data_streamer import MockComponentDataStreamer


@dataclasses.dataclass(frozen=True)
class _Mocks:
    """Mocks for the tests."""

    microgrid: MockMicrogrid
    """A mock microgrid instance."""

    streamer: MockComponentDataStreamer
    """A mock component data streamer."""

    component_status_sender: Sender[ComponentPoolStatus]
    """Sender for sending status of the components being tested."""

    @classmethod
    async def new(
        cls,
        component_category: ComponentCategory,
        mocker: MockerFixture,
        graph: _MicrogridComponentGraph | None = None,
        grid_meter: bool | None = None,
    ) -> _Mocks:
        """Initialize the mocks."""
        mockgrid = MockMicrogrid(graph=graph, grid_meter=grid_meter, mocker=mocker)
        if not graph:
            mockgrid.add_batteries(3)
        await mockgrid.start()

        # pylint: disable=protected-access
        if microgrid._data_pipeline._DATA_PIPELINE is not None:
            microgrid._data_pipeline._DATA_PIPELINE = None
        await microgrid._data_pipeline.initialize(
            ResamplerConfig(resampling_period=timedelta(seconds=0.1))
        )
        streamer = MockComponentDataStreamer(mockgrid.mock_client)

        dp = microgrid._data_pipeline._DATA_PIPELINE
        assert dp is not None

        if component_category == ComponentCategory.BATTERY:
            return cls(
                mockgrid,
                streamer,
                dp._battery_power_wrapper.status_channel.new_sender(),
            )
        if component_category == ComponentCategory.EV_CHARGER:
            return cls(
                mockgrid,
                streamer,
                dp._ev_power_wrapper.status_channel.new_sender(),
            )
        raise ValueError(f"Unsupported component category: {component_category}")

    async def stop(self) -> None:
        """Stop the mocks."""
        # pylint: disable=protected-access
        assert microgrid._data_pipeline._DATA_PIPELINE is not None
        await asyncio.gather(
            *[
                microgrid._data_pipeline._DATA_PIPELINE._stop(),
                self.streamer.stop(),
                self.microgrid.cleanup(),
            ]
        )
        # pylint: enable=protected-access


@asynccontextmanager
async def _mocks(
    mocker: MockerFixture,
    component_category: ComponentCategory,
    *,
    graph: _MicrogridComponentGraph | None = None,
    grid_meter: bool | None = None,
) -> AsyncIterator[_Mocks]:
    """Initialize the mocks."""
    mocks = await _Mocks.new(
        component_category, mocker, graph=graph, grid_meter=grid_meter
    )
    try:
        yield mocks
    finally:
        await mocks.stop()
