# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Test the PV pool control methods."""

import asyncio
import typing
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import async_solipsism
import pytest
from frequenz.channels import Receiver
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import ComponentStateCode
from frequenz.quantities import Power
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid import _power_distributing
from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.timeseries import ResamplerConfig2
from frequenz.sdk.timeseries.pv_pool import PVPoolReport

from ...microgrid.fixtures import _Mocks
from ...utils.component_data_streamer import MockComponentDataStreamer
from ...utils.component_data_wrapper import InverterDataWrapper
from ..mock_microgrid import MockMicrogrid


@pytest.fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Event loop policy."""
    return async_solipsism.EventLoopPolicy()


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
        ResamplerConfig2(resampling_period=timedelta(seconds=0.1))
    )
    streamer = MockComponentDataStreamer(mockgrid.mock_client)

    dp = typing.cast(_DataPipeline, microgrid._data_pipeline._DATA_PIPELINE)

    _mocks = _Mocks(
        mockgrid,
        streamer,
        dp._pv_power_wrapper.status_channel.new_sender(),
    )
    try:
        yield _mocks
    finally:
        await _mocks.stop()


class TestPVPoolControl:
    """Test control methods for the PVPool."""

    async def _init_pv_inverters(self, mocks: _Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(mocks.microgrid.pv_inverter_ids):
            mocks.streamer.start_streaming(
                InverterDataWrapper(
                    comp_id,
                    now,
                    states={ComponentStateCode.READY},
                    active_power=0.0,
                    active_power_inclusion_lower_bound=-10000.0 * (idx + 1),
                    active_power_inclusion_upper_bound=0.0,
                ),
                0.05,
            )

    async def _fail_pv_inverters(
        self, fail_ids: list[ComponentId], mocks: _Mocks
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(mocks.microgrid.pv_inverter_ids):
            mocks.streamer.update_stream(
                InverterDataWrapper(
                    comp_id,
                    now,
                    states=(
                        {ComponentStateCode.ERROR}
                        if comp_id in fail_ids
                        else {ComponentStateCode.READY}
                    ),
                    active_power=0.0,
                    active_power_inclusion_lower_bound=-10000.0 * (idx + 1),
                    active_power_inclusion_upper_bound=0.0,
                ),
            )

    def _assert_report(  # pylint: disable=too-many-arguments
        self,
        report: PVPoolReport | None,
        *,
        power: float | None,
        lower: float,
        upper: float,
        dist_result: _power_distributing.Result | None = None,
        expected_result_pred: (
            typing.Callable[[_power_distributing.Result], bool] | None
        ) = None,
    ) -> None:
        assert report is not None and report.target_power == (
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
    ) -> PVPoolReport | None:
        """Receive reports until the given condition is met."""
        max_reports = 10
        ctr = 0
        latest_report: PVPoolReport | None = None
        while ctr < max_reports:
            ctr += 1
            latest_report = await bounds_rx.receive()
            if check(latest_report):
                break

        return latest_report

    async def test_setting_power(  # pylint: disable=too-many-statements
        self,
        mocks: _Mocks,
        mocker: MockerFixture,
    ) -> None:
        """Test setting power."""
        set_power = typing.cast(
            AsyncMock,
            microgrid.connection_manager.get().api_client.set_component_power_active,
        )

        await self._init_pv_inverters(mocks)
        pv_pool = microgrid.new_pv_pool(priority=5)
        bounds_rx = pv_pool.power_status.new_receiver()
        latest_report = await self._recv_reports_until(
            bounds_rx,
            lambda x: x.bounds is not None and x.bounds.lower.as_watts() == -100000.0,
        )
        dist_results_rx = pv_pool.power_distribution_results.new_receiver()

        self._assert_report(latest_report, power=None, lower=-100000.0, upper=0.0)
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
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(-80000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            ComponentId(8),
            ComponentId(18),
            ComponentId(28),
            ComponentId(38),
        }

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
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(-4000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            ComponentId(8),
            ComponentId(18),
            ComponentId(28),
            ComponentId(38),
        }

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
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(-4000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            ComponentId(8),
            ComponentId(28),
            ComponentId(38),
        }

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
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(-70000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            ComponentId(8),
            ComponentId(28),
            ComponentId(38),
        }

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
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(-70000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            ComponentId(8),
            ComponentId(18),
            ComponentId(28),
            ComponentId(38),
        }

        set_power.reset_mock()
        await pv_pool.propose_power(Power.from_watts(-200000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == -100000.0,
        )

        self._assert_report(
            await bounds_rx.receive(), power=-100000.0, lower=-100000.0, upper=0.0
        )
        await asyncio.sleep(0.0)

        assert set_power.call_count == 4
        inv_ids = mocks.microgrid.pv_inverter_ids
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10000.0),
            mocker.call(inv_ids[1], -20000.0),
            mocker.call(inv_ids[2], -30000.0),
            mocker.call(inv_ids[3], -40000.0),
        ]
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(-100000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            ComponentId(8),
            ComponentId(18),
            ComponentId(28),
            ComponentId(38),
        }

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
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.zero()
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            ComponentId(8),
            ComponentId(18),
            ComponentId(28),
            ComponentId(38),
        }

        # Resetting the power should lead to default (full) power getting set for all
        # inverters.
        set_power.reset_mock()
        await pv_pool.propose_power(None)
        report = await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is None,
        )
        self._assert_report(report, power=None, lower=-100000.0, upper=0.0)
        await asyncio.sleep(0.0)

        assert set_power.call_count == 4
        inv_ids = mocks.microgrid.pv_inverter_ids
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(inv_ids[0], -10_000.0),
            mocker.call(inv_ids[1], -20_000.0),
            mocker.call(inv_ids[2], -30_000.0),
            mocker.call(inv_ids[3], -40_000.0),
        ]
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(-100000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            ComponentId(8),
            ComponentId(18),
            ComponentId(28),
            ComponentId(38),
        }
