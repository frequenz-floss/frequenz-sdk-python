# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Test the PV pool control methods."""

import asyncio
import typing
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Receiver
from frequenz.client.microgrid import (
    ComponentCategory,
    InverterComponentState,
    InverterType,
)
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig
from frequenz.sdk.microgrid import _power_distributing
from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.timeseries import Power
from frequenz.sdk.timeseries.pv_pool import PVPoolReport

from ...microgrid.fixtures import _Mocks
from ...utils.component_data_streamer import MockComponentDataStreamer
from ...utils.component_data_wrapper import InverterDataWrapper, MeterDataWrapper
from ...utils.graph_generator import GraphGenerator
from ..mock_microgrid import MockMicrogrid


@pytest.fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Event loop policy."""
    return async_solipsism.EventLoopPolicy()


@pytest.fixture
async def mocks(mocker: MockerFixture) -> typing.AsyncIterator[_Mocks]:
    """Create the mocks."""
    gen = GraphGenerator()
    pv_inv = [
        gen.component(ComponentCategory.INVERTER, InverterType.SOLAR) for _ in range(4)
    ]
    mockgrid = MockMicrogrid(
        graph=gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (ComponentCategory.METER, [pv_inv[0], pv_inv[1]]),
                    (ComponentCategory.METER, pv_inv[2]),
                    (ComponentCategory.METER, pv_inv[3]),
                ],
            )
        )
    )
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
        await self._start_pv_inverters(mocks.microgrid.pv_inverter_ids, mocks)
        await self._start_meters(mocks.microgrid.meter_ids, mocks)

    async def _start_pv_inverters(self, inv_ids: list[int], mocks: _Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(inv_ids):
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

    async def _start_meters(
        self,
        meter_ids: list[int],
        mocks: _Mocks,
        power: float = 0.0,
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(meter_ids):
            mocks.streamer.start_streaming(
                MeterDataWrapper(
                    comp_id,
                    now,
                    active_power=power,
                ),
                0.05,
            )

    async def _fail_pv_inverters(self, inv_ids: list[int], mocks: _Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(mocks.microgrid.pv_inverter_ids):
            mocks.streamer.update_stream(
                InverterDataWrapper(
                    comp_id,
                    now,
                    component_state=(
                        InverterComponentState.ERROR
                        if comp_id in inv_ids
                        else InverterComponentState.IDLE
                    ),
                    active_power=0.0,
                    active_power_inclusion_lower_bound=-10000.0 * (idx + 1),
                    active_power_inclusion_upper_bound=0.0,
                ),
            )

    async def _stop_components(self, inv_ids: list[int], mocks: _Mocks) -> None:
        for comp_id in inv_ids:
            await mocks.streamer.stop_streaming(comp_id)

    def _assert_report(  # pylint: disable=too-many-arguments
        self,
        report: PVPoolReport,
        *,
        power: float | None,
        lower: float,
        upper: float,
        dist_result: _power_distributing.Result | None = None,
        expected_result_pred: (
            typing.Callable[[_power_distributing.Result], bool] | None
        ) = None,
    ) -> None:
        assert report.target_power == (
            Power.from_watts(power) if power is not None else None
        )
        assert report.bounds is not None
        assert report.bounds.lower == Power.from_watts(lower)
        assert report.bounds.upper == Power.from_watts(upper)
        if expected_result_pred is not None:
            assert dist_result is not None
            assert expected_result_pred(dist_result)

    async def _recv_reports_until(
        self,
        bounds_rx: Receiver[PVPoolReport],
        check: typing.Callable[[PVPoolReport], bool],
        max_reports: int = 100,
    ) -> None:
        """Receive reports until the given condition is met."""
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
        pv_pool = microgrid.new_pv_pool(priority=5)
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

    async def test_fallback_power(  # pylint: disable=too-many-statements
        self,
        mocks: _Mocks,
        mocker: MockerFixture,
    ) -> None:
        """Test fallback power."""
        set_power = typing.cast(
            AsyncMock, microgrid.connection_manager.get().api_client.set_power
        )

        traveller = time_machine.travel(datetime(2012, 12, 12))
        mock_time = traveller.start()

        mocker.patch(
            "frequenz.sdk.microgrid._data_pipeline._DATA_PIPELINE"
            "._pv_power_wrapper._fallback_power",
            Power.from_watts(-10000.0),
        )
        await self._init_pv_inverters(mocks)
        pv_pool = microgrid.new_pv_pool(priority=5)
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

        # Stop inverter 0, which has a sister inverter, so it should use fallback power
        # even if the meter is still working.
        await self._stop_components([mocks.microgrid.pv_inverter_ids[0]], mocks)
        await asyncio.sleep(11.0)
        await pv_pool.propose_power(Power.from_watts(-40000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -40000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-40000.0, lower=-90000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[1], -10000.0),
            mocker.call(inv_ids[2], -10000.0),
            mocker.call(inv_ids[3], -10000.0),
        ]
        set_power.reset_mock()
        await self._start_pv_inverters([mocks.microgrid.pv_inverter_ids[0]], mocks)

        # Stop inverter 2, which has no sister inverter, so it should not use fallback
        # power when its meter is still streaming.
        await self._stop_components([mocks.microgrid.pv_inverter_ids[2]], mocks)
        await asyncio.sleep(11.0)
        mock_time.shift(timedelta(seconds=11.0))
        await pv_pool.propose_power(Power.from_watts(-50000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -50000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-50000.0, lower=-70000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10000.0),
            mocker.call(inv_ids[1], -20000.0),
            mocker.call(inv_ids[3], -20000.0),
        ]
        set_power.reset_mock()

        # Stop meter 2, which has no sister inverter, so it should not use fallback
        # power when its meter is still streaming.
        await self._stop_components([mocks.microgrid.meter_ids[1]], mocks)
        await asyncio.sleep(11.0)
        mock_time.shift(timedelta(seconds=11.0))
        await pv_pool.propose_power(Power.from_watts(-42000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -42000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-42000.0, lower=-70000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10000.0),
            mocker.call(inv_ids[1], -11000.0),
            mocker.call(inv_ids[3], -11000.0),
        ]
        set_power.reset_mock()

        # Start meter 2 measuring -5000W.  Now inv 2's fallback power should be -5000W.
        await self._start_meters([mocks.microgrid.meter_ids[1]], mocks, power=-5000.0)
        await pv_pool.propose_power(Power.from_watts(-50000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -50000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-50000.0, lower=-70000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10000.0),
            mocker.call(inv_ids[1], -17500.0),
            mocker.call(inv_ids[3], -17500.0),
        ]
        set_power.reset_mock()

        # Start inv 2. There should be no fallback power now.
        await self._start_pv_inverters([mocks.microgrid.pv_inverter_ids[2]], mocks)
        await asyncio.sleep(1.0)
        await pv_pool.propose_power(Power.from_watts(-60000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -60000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=-60000.0, lower=-80000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10000.0),
            mocker.call(inv_ids[1], -20000.0),
            mocker.call(inv_ids[2], -10000.0),
            mocker.call(inv_ids[3], -20000.0),
        ]
