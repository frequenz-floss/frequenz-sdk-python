# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Test the EV charger pool control methods."""

import asyncio
import typing
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Receiver
from frequenz.client.microgrid import EVChargerCableState, EVChargerComponentState
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid import _power_distributing
from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.microgrid._power_distributing import ComponentPoolStatus
from frequenz.sdk.microgrid._power_distributing._component_pool_status_tracker import (
    ComponentPoolStatusTracker,
)
from frequenz.sdk.timeseries import Power, ResamplerConfig, Sample3Phase, Voltage
from frequenz.sdk.timeseries.ev_charger_pool import EVChargerPool, EVChargerPoolReport

from ...microgrid.fixtures import _Mocks
from ...utils.component_data_streamer import MockComponentDataStreamer
from ...utils.component_data_wrapper import EvChargerDataWrapper, MeterDataWrapper
from ..mock_microgrid import MockMicrogrid

# pylint: disable=protected-access


@pytest.fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Event loop policy."""
    return async_solipsism.EventLoopPolicy()


@pytest.fixture
async def mocks(mocker: MockerFixture) -> typing.AsyncIterator[_Mocks]:
    """Create the mocks."""
    mockgrid = MockMicrogrid(grid_meter=True)
    mockgrid.add_ev_chargers(4)
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
        dp._ev_power_wrapper.status_channel.new_sender(),
    )


class TestEVChargerPoolControl:
    """Test the EV charger pool control methods."""

    async def _patch_ev_pool_status(
        self,
        mocks: _Mocks,
        mocker: MockerFixture,
        component_ids: list[int] | None = None,
    ) -> None:
        """Patch the EV charger pool status.

        If `component_ids` is not None, the mock will always return `component_ids`.
        Otherwise, it will return the requested components.
        """
        if component_ids:
            mock = MagicMock(spec=ComponentPoolStatusTracker)
            mock.get_working_components.return_value = component_ids
            mocker.patch(
                "frequenz.sdk.microgrid._power_distributing._component_managers"
                "._ev_charger_manager._ev_charger_manager.ComponentPoolStatusTracker",
                return_value=mock,
            )
        else:
            mock = MagicMock(spec=ComponentPoolStatusTracker)
            mock.get_working_components.side_effect = set
            mocker.patch(
                "frequenz.sdk.microgrid._power_distributing._component_managers"
                "._ev_charger_manager._ev_charger_manager.ComponentPoolStatusTracker",
                return_value=mock,
            )
        await mocks.component_status_sender.send(
            ComponentPoolStatus(working=set(mocks.microgrid.evc_ids), uncertain=set())
        )

    async def _patch_power_distributing_actor(
        self,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            "frequenz.sdk.microgrid._data_pipeline._DATA_PIPELINE._ev_power_wrapper"
            "._power_distributing_actor._component_manager._voltage_cache.get",
            return_value=Sample3Phase(
                timestamp=datetime.now(tz=timezone.utc),
                value_p1=Voltage.from_volts(220.0),
                value_p2=Voltage.from_volts(220.0),
                value_p3=Voltage.from_volts(220.0),
            ),
        )

    async def _init_ev_chargers(self, mocks: _Mocks) -> None:
        now = datetime.now(tz=timezone.utc)
        for evc_id in mocks.microgrid.evc_ids:
            mocks.streamer.start_streaming(
                EvChargerDataWrapper(
                    evc_id,
                    now,
                    cable_state=EVChargerCableState.EV_PLUGGED,
                    component_state=EVChargerComponentState.READY,
                    active_power=0.0,
                    active_power_inclusion_lower_bound=0.0,
                    active_power_inclusion_upper_bound=16.0 * 230.0 * 3,
                    voltage_per_phase=(230.0, 230.0, 230.0),
                ),
                0.05,
            )

        for meter_id in mocks.microgrid.meter_ids:
            mocks.streamer.start_streaming(
                MeterDataWrapper(
                    meter_id,
                    now,
                    voltage_per_phase=(230.0, 230.0, 230.0),
                ),
                0.05,
            )

    def _assert_report(  # pylint: disable=too-many-arguments
        self,
        report: EVChargerPoolReport,
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

    async def _get_bounds_receiver(
        self, ev_charger_pool: EVChargerPool
    ) -> Receiver[EVChargerPoolReport]:
        bounds_rx = ev_charger_pool.power_status.new_receiver()

        # Consume initial reports as chargers are initialized
        expected_upper_bounds = 44160.0
        max_reports = 10
        ctr = 0
        while ctr < max_reports:
            ctr += 1
            report = await bounds_rx.receive()
            assert report.bounds is not None
            if report.bounds.upper == Power.from_watts(expected_upper_bounds):
                break

        return bounds_rx

    async def test_setting_power(
        self,
        mocks: _Mocks,
        mocker: MockerFixture,
    ) -> None:
        """Test setting power."""
        traveller = time_machine.travel(datetime(2012, 12, 12))
        mock_time = traveller.start()

        set_power = typing.cast(
            AsyncMock, microgrid.connection_manager.get().api_client.set_power
        )
        await self._init_ev_chargers(mocks)
        ev_charger_pool = microgrid.new_ev_charger_pool(priority=5)
        await self._patch_ev_pool_status(mocks, mocker)
        await self._patch_power_distributing_actor(mocker)

        bounds_rx = await self._get_bounds_receiver(ev_charger_pool)

        # Check that chargers are initialized to Power.zero()
        assert set_power.call_count == 4
        assert all(x.args[1] == 0.0 for x in set_power.call_args_list)

        self._assert_report(
            await bounds_rx.receive(), power=None, lower=0.0, upper=44160.0
        )

        set_power.reset_mock()
        await ev_charger_pool.propose_power(Power.from_watts(40000.0))
        # ignore one report because it is not always immediately updated.
        await bounds_rx.receive()
        self._assert_report(
            await bounds_rx.receive(), power=40000.0, lower=0.0, upper=44160.0
        )
        mock_time.shift(timedelta(seconds=60))
        await asyncio.sleep(0.15)

        # Components are set initial power
        assert set_power.call_count == 4
        assert all(x.args[1] == 6600.0 for x in set_power.call_args_list)

        # All available power is allocated. 3 chargers are set to 11040.0
        # and the last one is set to 6880.0
        set_power.reset_mock()
        mock_time.shift(timedelta(seconds=60))
        await asyncio.sleep(0.15)
        assert set_power.call_count == 4

        evs_11040 = [x.args for x in set_power.call_args_list if x.args[1] == 11040.0]
        assert 3 == len(evs_11040)
        evs_6680 = [x.args for x in set_power.call_args_list if x.args[1] == 6880.0]
        assert 1 == len(evs_6680)

        # Throttle the power
        set_power.reset_mock()
        await ev_charger_pool.propose_power(Power.from_watts(32000.0))
        await bounds_rx.receive()
        await asyncio.sleep(0.02)
        assert set_power.call_count == 1

        stopped_evs = [x.args for x in set_power.call_args_list if x.args[1] == 0.0]
        assert 1 == len(stopped_evs)
        assert stopped_evs[0][0] in [evc[0] for evc in evs_11040]

        traveller.stop()
