# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Test the battery pool control methods."""

import asyncio
import dataclasses
import typing
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from frequenz.channels import Sender
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig
from frequenz.sdk.actor.power_distributing import BatteryStatus
from frequenz.sdk.actor.power_distributing._battery_pool_status import BatteryPoolStatus
from frequenz.sdk.timeseries import Power

from ...utils.component_data_streamer import MockComponentDataStreamer
from ...utils.component_data_wrapper import BatteryDataWrapper, InverterDataWrapper
from ..mock_microgrid import MockMicrogrid


@dataclasses.dataclass(frozen=True)
class Mocks:
    """Mocks for the tests."""

    microgrid: MockMicrogrid
    """A mock microgrid instance."""

    streamer: MockComponentDataStreamer
    """A mock component data streamer."""

    battery_status_sender: Sender[BatteryStatus]
    """Sender for sending status of the batteries."""


# pylint doesn't understand fixtures. It thinks it is redefined name.
# pylint: disable=redefined-outer-name


@pytest.fixture
async def mocks(mocker: MockerFixture) -> typing.AsyncIterator[Mocks]:
    """Fixture for the mocks."""
    mockgrid = MockMicrogrid()
    mockgrid.add_batteries(4)
    await mockgrid.start(mocker)

    # pylint: disable=protected-access
    if microgrid._data_pipeline._DATA_PIPELINE is not None:
        microgrid._data_pipeline._DATA_PIPELINE = None
    await microgrid._data_pipeline.initialize(
        ResamplerConfig(resampling_period=timedelta(seconds=0.1))
    )
    streamer = MockComponentDataStreamer(mockgrid.mock_client)

    assert microgrid._data_pipeline._DATA_PIPELINE is not None

    yield Mocks(
        mockgrid,
        streamer,
        microgrid._data_pipeline._DATA_PIPELINE._battery_status_channel.new_sender(),
    )

    await asyncio.gather(
        *[
            microgrid._data_pipeline._DATA_PIPELINE._stop(),
            streamer.stop(),
        ]
    )


class TestBatteryPoolControl:
    """Test the battery pool control methods."""

    async def _patch_battery_pool_status(
        self, mocks: Mocks, mocker: MockerFixture, battery_ids: list[int] | None = None
    ) -> None:
        """Patch the battery pool status.

        If `battery_ids` is not None, the mock will always return `battery_ids`.
        Otherwise, it will return the requested batteries.
        """
        if battery_ids:
            mock = MagicMock(spec=BatteryPoolStatus)
            mock.get_working_batteries.return_value = battery_ids
            mocker.patch(
                "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
                return_value=mock,
            )
        else:
            mock = MagicMock(spec=BatteryPoolStatus)
            mock.get_working_batteries.side_effect = set
            mocker.patch(
                "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
                return_value=mock,
            )
        await mocks.battery_status_sender.send(
            BatteryStatus(working=set(mocks.microgrid.battery_ids), uncertain=set())
        )

    async def _init_data_for_batteries(self, mocks: Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for battery_id in mocks.microgrid.battery_ids:
            mocks.streamer.start_streaming(
                BatteryDataWrapper(
                    battery_id,
                    now,
                    soc=50.0,
                    soc_lower_bound=10.0,
                    soc_upper_bound=90.0,
                    power_exclusion_lower_bound=0.0,
                    power_exclusion_upper_bound=0.0,
                    power_inclusion_lower_bound=-1000.0,
                    power_inclusion_upper_bound=1000.0,
                    capacity=2000.0,
                ),
                0.05,
            )

    async def _init_data_for_inverters(self, mocks: Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for inv_id in mocks.microgrid.battery_inverter_ids:
            mocks.streamer.start_streaming(
                InverterDataWrapper(
                    inv_id,
                    now,
                    active_power_exclusion_lower_bound=0.0,
                    active_power_exclusion_upper_bound=0.0,
                    active_power_inclusion_lower_bound=-1000.0,
                    active_power_inclusion_upper_bound=1000.0,
                ),
                0.05,
            )

    async def test_case_1(
        self,
        mocks: Mocks,
        mocker: MockerFixture,
    ) -> None:
        """Test case 1.

        - single battery pool with all batteries.
        - all batteries are working.
        """
        set_power = typing.cast(
            AsyncMock, microgrid.connection_manager.get().api_client.set_power
        )

        await self._patch_battery_pool_status(mocks, mocker)
        await self._init_data_for_batteries(mocks)
        await self._init_data_for_inverters(mocks)

        battery_pool = microgrid.battery_pool()

        # This is used just to wait for the processing to be complete.  The results are
        # not used.
        #
        # It will be replaced by a reporting streaming from the PowerManager in a
        # subsequent commit.
        results_rx = battery_pool.power_distribution_results()

        await battery_pool.set_power(Power.from_watts(1000.0))
        await results_rx.receive()

        assert set_power.call_count == 4
        assert set_power.call_args_list == [
            mocker.call(inv_id, 250.0)
            for inv_id in mocks.microgrid.battery_inverter_ids
        ]

    async def test_case_2(self, mocks: Mocks, mocker: MockerFixture) -> None:
        """Test case 2.

        - two battery pools with different batteries.
        - all batteries are working.
        """
        set_power = typing.cast(
            AsyncMock, microgrid.connection_manager.get().api_client.set_power
        )

        await self._patch_battery_pool_status(mocks, mocker)
        await self._init_data_for_batteries(mocks)
        await self._init_data_for_inverters(mocks)

        battery_pool_1 = microgrid.battery_pool(set(mocks.microgrid.battery_ids[:2]))
        battery_pool_2 = microgrid.battery_pool(set(mocks.microgrid.battery_ids[2:]))

        results_rx = battery_pool_1.power_distribution_results()
        await battery_pool_1.set_power(Power.from_watts(1000.0))
        await results_rx.receive()
        assert set_power.call_count == 2
        assert set_power.call_args_list == [
            mocker.call(inv_id, 500.0)
            for inv_id in mocks.microgrid.battery_inverter_ids[:2]
        ]
        set_power.reset_mock()

        await battery_pool_2.set_power(Power.from_watts(1000.0))
        await results_rx.receive()
        assert set_power.call_count == 2
        assert set_power.call_args_list == [
            mocker.call(inv_id, 500.0)
            for inv_id in mocks.microgrid.battery_inverter_ids[2:]
        ]

    async def test_case_3(self, mocks: Mocks, mocker: MockerFixture) -> None:
        """Test case 3.

        - two battery pools with same batteries, but different priorities.
        - all batteries are working.
        """
        set_power = typing.cast(
            AsyncMock, microgrid.connection_manager.get().api_client.set_power
        )

        await self._patch_battery_pool_status(mocks, mocker)
        await self._init_data_for_batteries(mocks)
        await self._init_data_for_inverters(mocks)

        battery_pool_1 = microgrid.battery_pool()
        battery_pool_2 = microgrid.battery_pool()

        results_rx = battery_pool_1.power_distribution_results()
        await battery_pool_1.set_power(
            Power.from_watts(-1000.0),
            _priority=2,
            _bounds=(Power.from_watts(-1000.0), Power.from_watts(0.0)),
        )
        await results_rx.receive()
        assert set_power.call_count == 4
        assert set_power.call_args_list == [
            mocker.call(inv_id, -250.0)
            for inv_id in mocks.microgrid.battery_inverter_ids
        ]
        set_power.reset_mock()

        await battery_pool_2.set_power(
            Power.from_watts(0.0),
            _priority=1,
            _bounds=(Power.from_watts(0.0), Power.from_watts(1000.0)),
        )
        await results_rx.receive()
        assert set_power.call_count == 4
        assert set_power.call_args_list == [
            mocker.call(inv_id, 0.0) for inv_id in mocks.microgrid.battery_inverter_ids
        ]
