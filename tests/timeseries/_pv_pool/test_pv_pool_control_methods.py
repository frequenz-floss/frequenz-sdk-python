# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Test the PV pool control methods."""

import asyncio
import typing
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from frequenz.channels import Receiver
from frequenz.client.microgrid import InverterComponentState
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig, power_distributing
from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.timeseries import Power
from frequenz.sdk.timeseries.pv_pool import PVPoolReport

from ...microgrid.fixtures import _Mocks
from ...utils.component_data_streamer import MockComponentDataStreamer
from ...utils.component_data_wrapper import InverterDataWrapper
from ..mock_microgrid import MockMicrogrid


@pytest.fixture
async def mocks(mocker: MockerFixture) -> typing.AsyncIterator[_Mocks]:
    """Create the mocks."""
    mockgrid = MockMicrogrid(grid_meter=True)
    mockgrid.add_solar_inverters(4)
    await mockgrid.start(mocker)

    # pylint: disable=protected-access
    if microgrid._data_pipeline._DATA_PIPELINE is not None:
        microgrid._data_pipeline._DATA_PIPELINE = None
    await microgrid._data_pipeline.initialize(
        ResamplerConfig(resampling_period=timedelta(seconds=0.1))
    )
    streamer = MockComponentDataStreamer(mockgrid.mock_client)

    dp = typing.cast(_DataPipeline, microgrid._data_pipeline._DATA_PIPELINE)

    yield _Mocks(
        mockgrid,
        streamer,
        dp._pv_power_wrapper.status_channel.new_sender(),
    )


class TestPVPoolControl:
    """Test control methods for the PVPool."""

    async def _init_pv_inverters(self, mocks: _Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(mocks.microgrid.pv_inverter_ids):
            mocks.streamer.start_streaming(
                InverterDataWrapper(
                    comp_id,
                    now,
                    component_state=InverterComponentState.IDLE,
                    active_power=0.0,
                    active_power_inclusion_lower_bound=-10000.0 * (idx + 1),
                    active_power_inclusion_upper_bound=0.0,
                ),
                0.05,
            )

    async def _fail_pv_inverters(self, fail_ids: list[int], mocks: _Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(mocks.microgrid.pv_inverter_ids):
            mocks.streamer.update_stream(
                InverterDataWrapper(
                    comp_id,
                    now,
                    component_state=(
                        InverterComponentState.ERROR
                        if comp_id in fail_ids
                        else InverterComponentState.IDLE
                    ),
                    active_power=0.0,
                    active_power_inclusion_lower_bound=-10000.0 * (idx + 1),
                    active_power_inclusion_upper_bound=0.0,
                ),
            )

    def _assert_report(  # pylint: disable=too-many-arguments
        self,
        report: PVPoolReport,
        *,
        power: float | None,
        lower: float,
        upper: float,
        expected_result_pred: (
            typing.Callable[[power_distributing.Result], bool] | None
        ) = None,
    ) -> None:
        assert report.target_power == (
            Power.from_watts(power) if power is not None else None
        )
        assert report.bounds is not None
        assert report.bounds.lower == Power.from_watts(lower)
        assert report.bounds.upper == Power.from_watts(upper)
        if expected_result_pred is not None:
            assert report.distribution_result is not None
            assert expected_result_pred(report.distribution_result)

    async def _recv_reports_until(
        self,
        bounds_rx: Receiver[PVPoolReport],
        check: typing.Callable[[PVPoolReport], bool],
    ) -> None:
        """Receive reports until the given condition is met."""
        max_reports = 10
        ctr = 0
        while ctr < max_reports:
            ctr += 1
            report = await bounds_rx.receive()
            if check(report):
                break

    async def test_setting_power(  # pylint: disable=too-many-statements
        self,
        mocks: _Mocks,
        mocker: MockerFixture,
    ) -> None:
        """Test setting power."""
        set_power = typing.cast(
            AsyncMock, microgrid.connection_manager.get().api_client.set_power
        )

        await self._init_pv_inverters(mocks)
        pv_pool = microgrid.pv_pool(priority=5)
        bounds_rx = pv_pool.power_status.new_receiver()
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.bounds is not None and x.bounds.lower.as_watts() == -100000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=None, lower=-100000.0, upper=0.0
        )
        await pv_pool.propose_power(Power.from_watts(-80000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -80000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-80000.0, lower=-100000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        # Components are set initial power
        assert set_power.call_count == 4
        inv_ids = mocks.microgrid.pv_inverter_ids
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10000.0),
            mocker.call(inv_ids[1], -20000.0),
            mocker.call(inv_ids[2], -25000.0),
            mocker.call(inv_ids[3], -25000.0),
        ]

        set_power.reset_mock()
        await pv_pool.propose_power(Power.from_watts(-4000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -4000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-4000.0, lower=-100000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        # Components are set initial power
        assert set_power.call_count == 4
        inv_ids = mocks.microgrid.pv_inverter_ids
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -1000.0),
            mocker.call(inv_ids[1], -1000.0),
            mocker.call(inv_ids[2], -1000.0),
            mocker.call(inv_ids[3], -1000.0),
        ]

        # After failing 1 inverter, bounds should go down and power shouldn't be
        # distributed to that inverter.
        await self._fail_pv_inverters([inv_ids[1]], mocks)
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.bounds is not None and x.bounds.lower.as_watts() == -80000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-4000.0, lower=-80000.0, upper=0.0
        )

        set_power.reset_mock()
        await pv_pool.propose_power(Power.from_watts(-70000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -70000.0,
        )

        self._assert_report(
            await bounds_rx.receive(), power=-70000.0, lower=-80000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        # Components are set initial power
        assert set_power.call_count == 3
        inv_ids = mocks.microgrid.pv_inverter_ids
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10000.0),
            mocker.call(inv_ids[2], -30000.0),
            mocker.call(inv_ids[3], -30000.0),
        ]

        # After the failed inverter recovers, bounds should go back up and power
        # should be distributed to all inverters
        await self._fail_pv_inverters([], mocks)
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.bounds is not None and x.bounds.lower.as_watts() == -100000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-70000.0, lower=-100000.0, upper=0.0
        )

        set_power.reset_mock()
        await pv_pool.propose_power(Power.from_watts(-90000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -90000.0,
        )

        self._assert_report(
            await bounds_rx.receive(), power=-90000.0, lower=-100000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        assert set_power.call_count == 4
        inv_ids = mocks.microgrid.pv_inverter_ids
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10000.0),
            mocker.call(inv_ids[1], -20000.0),
            mocker.call(inv_ids[2], -30000.0),
            mocker.call(inv_ids[3], -30000.0),
        ]

        # Setting 0 power should set all inverters to 0
        set_power.reset_mock()
        await pv_pool.propose_power(Power.zero())
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None and x.target_power.as_watts() == 0.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=0.0, lower=-100000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        assert set_power.call_count == 4
        inv_ids = mocks.microgrid.pv_inverter_ids
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], 0.0),
            mocker.call(inv_ids[1], 0.0),
            mocker.call(inv_ids[2], 0.0),
            mocker.call(inv_ids[3], 0.0),
        ]
