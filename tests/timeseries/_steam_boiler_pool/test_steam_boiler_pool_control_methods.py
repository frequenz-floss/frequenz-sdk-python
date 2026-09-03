# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Test the steam boiler pool control methods."""

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
from frequenz.sdk.timeseries.steam_boiler_pool import SteamBoilerPoolReport

from ...microgrid.fixtures import _Mocks
from ...utils.component_data_streamer import MockComponentDataStreamer
from ...utils.component_data_wrapper import SteamBoilerDataWrapper
from ..mock_microgrid import MockMicrogrid


@pytest.fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Event loop policy."""
    return async_solipsism.EventLoopPolicy()


@pytest.fixture
async def mocks(mocker: MockerFixture) -> typing.AsyncIterator[_Mocks]:
    """Create the mocks."""
    mockgrid = MockMicrogrid(grid_meter=True)
    mockgrid.add_steam_boilers(4)
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
        dp._steam_boiler_power_wrapper.status_channel.new_sender(),
    )
    try:
        yield _mocks
    finally:
        await _mocks.stop()


class TestSteamBoilerPoolControl:
    """Test control methods for the SteamBoilerPool."""

    async def _init_steam_boilers(self, mocks: _Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(mocks.microgrid.steam_boiler_ids):
            mocks.streamer.start_streaming(
                SteamBoilerDataWrapper(
                    comp_id,
                    now,
                    states={ComponentStateCode.READY},
                    active_power=0.0,
                    active_power_inclusion_lower_bound=0.0,
                    active_power_inclusion_upper_bound=10000.0 * (idx + 1),
                ),
                0.05,
            )

    async def _fail_steam_boilers(
        self, fail_ids: list[ComponentId], mocks: _Mocks
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        for idx, comp_id in enumerate(mocks.microgrid.steam_boiler_ids):
            mocks.streamer.update_stream(
                SteamBoilerDataWrapper(
                    comp_id,
                    now,
                    states=(
                        {ComponentStateCode.ERROR}
                        if comp_id in fail_ids
                        else {ComponentStateCode.READY}
                    ),
                    active_power=0.0,
                    active_power_inclusion_lower_bound=0.0,
                    active_power_inclusion_upper_bound=10000.0 * (idx + 1),
                ),
            )

    def _assert_report(  # pylint: disable=too-many-arguments
        self,
        report: SteamBoilerPoolReport | None,
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
        bounds_rx: Receiver[SteamBoilerPoolReport],
        check: typing.Callable[[SteamBoilerPoolReport], bool],
    ) -> SteamBoilerPoolReport | None:
        """Receive reports until the given condition is met."""
        max_reports = 10
        ctr = 0
        latest_report: SteamBoilerPoolReport | None = None
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

        await self._init_steam_boilers(mocks)
        steam_boiler_pool = microgrid.new_steam_boiler_pool(priority=5)
        bounds_rx = steam_boiler_pool.power_status.new_receiver()
        latest_report = await self._recv_reports_until(
            bounds_rx,
            lambda x: x.bounds is not None and x.bounds.upper.as_watts() == 100000.0,
        )
        dist_results_rx = steam_boiler_pool.power_distribution_results.new_receiver()

        self._assert_report(latest_report, power=None, lower=0.0, upper=100000.0)
        boiler_ids = mocks.microgrid.steam_boiler_ids

        await steam_boiler_pool.propose_power(Power.from_watts(80000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == 80000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=80000.0, lower=0.0, upper=100000.0
        )
        await asyncio.sleep(0.0)

        # Components are set initial power
        assert set_power.call_count == 4
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(boiler_ids[0], 10000.0),
            mocker.call(boiler_ids[1], 20000.0),
            mocker.call(boiler_ids[2], 25000.0),
            mocker.call(boiler_ids[3], 25000.0),
        ]
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(80000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == set(boiler_ids)

        # Throttling to a lower power should distribute it evenly again.
        set_power.reset_mock()
        await steam_boiler_pool.propose_power(Power.from_watts(4000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == 4000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=4000.0, lower=0.0, upper=100000.0
        )
        await asyncio.sleep(0.0)

        assert set_power.call_count == 4
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(boiler_ids[0], 1000.0),
            mocker.call(boiler_ids[1], 1000.0),
            mocker.call(boiler_ids[2], 1000.0),
            mocker.call(boiler_ids[3], 1000.0),
        ]
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(4000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == set(boiler_ids)

        # After failing 1 boiler, bounds should go down and power shouldn't be
        # distributed to that boiler.
        await self._fail_steam_boilers([boiler_ids[1]], mocks)
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.bounds is not None and x.bounds.upper.as_watts() == 80000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=4000.0, lower=0.0, upper=80000.0
        )
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(4000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            boiler_ids[0],
            boiler_ids[2],
            boiler_ids[3],
        }

        set_power.reset_mock()
        await steam_boiler_pool.propose_power(Power.from_watts(70000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == 70000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=70000.0, lower=0.0, upper=80000.0
        )
        await asyncio.sleep(0.0)

        assert set_power.call_count == 3
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(boiler_ids[0], 10000.0),
            mocker.call(boiler_ids[2], 30000.0),
            mocker.call(boiler_ids[3], 30000.0),
        ]
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(70000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == {
            boiler_ids[0],
            boiler_ids[2],
            boiler_ids[3],
        }

        # After the failed boiler recovers, bounds should go back up and power
        # should be distributed to all boilers.
        await self._fail_steam_boilers([], mocks)
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.bounds is not None and x.bounds.upper.as_watts() == 100000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=70000.0, lower=0.0, upper=100000.0
        )
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(70000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == set(boiler_ids)

        # Proposing more than the available power clamps to the system bounds.
        set_power.reset_mock()
        await steam_boiler_pool.propose_power(Power.from_watts(200000.0))
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None
            and x.target_power.as_watts() == 100000.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=100000.0, lower=0.0, upper=100000.0
        )
        await asyncio.sleep(0.0)

        assert set_power.call_count == 4
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(boiler_ids[0], 10000.0),
            mocker.call(boiler_ids[1], 20000.0),
            mocker.call(boiler_ids[2], 30000.0),
            mocker.call(boiler_ids[3], 40000.0),
        ]
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.from_watts(100000.0)
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == set(boiler_ids)

        # Setting 0 power should set all boilers to 0.
        set_power.reset_mock()
        await steam_boiler_pool.propose_power(Power.zero())
        await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is not None and x.target_power.as_watts() == 0.0,
        )
        self._assert_report(
            await bounds_rx.receive(), power=0.0, lower=0.0, upper=100000.0
        )
        await asyncio.sleep(0.0)

        assert set_power.call_count == 4
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(boiler_ids[0], 0.0),
            mocker.call(boiler_ids[1], 0.0),
            mocker.call(boiler_ids[2], 0.0),
            mocker.call(boiler_ids[3], 0.0),
        ]
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.zero()
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == set(boiler_ids)

        # Resetting the power should lead to default (zero) power getting set for all
        # boilers.
        set_power.reset_mock()
        await steam_boiler_pool.propose_power(None)
        report = await self._recv_reports_until(
            bounds_rx,
            lambda x: x.target_power is None,
        )
        self._assert_report(report, power=None, lower=0.0, upper=100000.0)
        await asyncio.sleep(0.0)

        assert set_power.call_count == 4
        assert sorted(set_power.call_args_list, key=lambda x: x.args[0]) == [
            mocker.call(boiler_ids[0], 0.0),
            mocker.call(boiler_ids[1], 0.0),
            mocker.call(boiler_ids[2], 0.0),
            mocker.call(boiler_ids[3], 0.0),
        ]
        dist_results = await dist_results_rx.receive()
        assert isinstance(
            dist_results, _power_distributing.Success
        ), f"Expected a success, got {dist_results}"
        assert dist_results.succeeded_power == Power.zero()
        assert dist_results.excess_power == Power.zero()
        assert dist_results.succeeded_components == set(boiler_ids)
