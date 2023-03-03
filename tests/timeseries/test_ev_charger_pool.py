# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `EVChargerPool`."""

from __future__ import annotations

import asyncio
from math import isclose
from typing import Optional

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import (
    ComponentMetricId,
    EVChargerCableState,
    EVChargerComponentState,
)
from frequenz.sdk.timeseries.ev_charger_pool import EVChargerPool
from frequenz.sdk.timeseries.ev_charger_pool._state_tracker import (
    EVChargerPoolStates,
    EVChargerState,
    StateTracker,
)
from frequenz.sdk.timeseries.logical_meter import LogicalMeter
from tests.timeseries._formula_engine.utils import (
    get_resampled_stream,
    synchronize_receivers,
)
from tests.timeseries.mock_microgrid import MockMicrogrid


class TestEVChargerPool:
    """Tests for the `EVChargerPool`."""

    async def test_state_updates(self, mocker: MockerFixture) -> None:
        """Test ev charger state updates are visible."""

        mockgrid = MockMicrogrid(grid_side_meter=False, sample_rate_s=0.01)
        mockgrid.add_ev_chargers(5)
        await mockgrid.start(mocker)

        state_tracker = StateTracker(set(mockgrid.evc_ids))
        states = state_tracker.new_receiver()

        async def check_next_state(
            expected: dict[int, EVChargerState],
            latest: Optional[tuple[int, EVChargerState]],
        ) -> EVChargerPoolStates:
            pool_states = await states.receive()
            assert pool_states.latest_change() == latest
            assert pool_states._states == expected  # pylint: disable=protected-access
            return pool_states

        ## check that all chargers are in idle state.
        expected_states = {evc_id: EVChargerState.IDLE for evc_id in mockgrid.evc_ids}
        assert len(expected_states) == 5
        await check_next_state(expected_states, None)

        ## check that EV_PLUGGED state gets set
        await asyncio.sleep(0.02)
        evc_2_id = mockgrid.evc_ids[2]
        mockgrid.evc_cable_states[evc_2_id] = EVChargerCableState.EV_PLUGGED
        mockgrid.evc_component_states[evc_2_id] = EVChargerComponentState.READY
        expected_states[evc_2_id] = EVChargerState.EV_PLUGGED
        await check_next_state(expected_states, (evc_2_id, EVChargerState.EV_PLUGGED))

        ## check that EV_LOCKED state gets set
        await asyncio.sleep(0.03)
        evc_3_id = mockgrid.evc_ids[3]
        mockgrid.evc_cable_states[evc_3_id] = EVChargerCableState.EV_LOCKED
        mockgrid.evc_component_states[evc_3_id] = EVChargerComponentState.READY
        expected_states[evc_3_id] = EVChargerState.EV_LOCKED
        await check_next_state(expected_states, (evc_3_id, EVChargerState.EV_LOCKED))

        ## check that ERROR state gets set
        await asyncio.sleep(0.1)
        evc_1_id = mockgrid.evc_ids[1]
        mockgrid.evc_cable_states[evc_1_id] = EVChargerCableState.EV_LOCKED
        mockgrid.evc_component_states[evc_1_id] = EVChargerComponentState.ERROR
        expected_states[evc_1_id] = EVChargerState.ERROR
        await check_next_state(expected_states, (evc_1_id, EVChargerState.ERROR))

        await state_tracker.stop()
        await mockgrid.cleanup()

    async def test_ev_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the battery power and pv power formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_ev_chargers(5)
        request_chan, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_chan.new_sender(),
            microgrid.get().component_graph,
        )

        ev_pool = EVChargerPool(channel_registry, request_chan.new_sender())

        main_meter_recv = await get_resampled_stream(
            logical_meter,
            channel_registry,
            request_chan.new_sender(),
            mockgrid.main_meter_id,
            ComponentMetricId.ACTIVE_POWER,
        )
        grid_power_recv = await logical_meter.grid_power()
        ev_power_recv = await ev_pool.total_power()

        await synchronize_receivers([grid_power_recv, main_meter_recv, ev_power_recv])

        ev_results = []
        for _ in range(10):
            grid_pow = await grid_power_recv.receive()
            ev_pow = await ev_power_recv.receive()
            main_pow = await main_meter_recv.receive()

            assert grid_pow is not None and grid_pow.value is not None
            assert ev_pow is not None and ev_pow.value is not None
            assert main_pow is not None and main_pow.value is not None
            assert isclose(grid_pow.value, ev_pow.value + main_pow.value)

            ev_results.append(ev_pow.value)

        await mockgrid.cleanup()
        assert len(ev_results) == 10
