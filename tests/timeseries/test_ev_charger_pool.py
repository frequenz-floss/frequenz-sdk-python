# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `EVChargerPool`."""

from __future__ import annotations

import asyncio
from typing import Optional

from frequenz.api.microgrid import ev_charger_pb2
from pytest_mock import MockerFixture

from frequenz.sdk.timeseries.ev_charger_pool import (
    EVChargerPool,
    EVChargerPoolStates,
    EVChargerState,
)
from tests.timeseries.mock_microgrid import MockMicrogrid


class TestEVChargerPool:
    """Tests for the `EVChargerPool`."""

    async def test_state_updates(self, mocker: MockerFixture) -> None:
        """Test ev charger state updates are visible."""

        mockgrid = MockMicrogrid(grid_side_meter=False, sample_rate_s=0.01)
        mockgrid.add_ev_chargers(5)
        await mockgrid.start(mocker)

        pool = EVChargerPool()

        states = pool.states()

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
        mockgrid.evc_states[evc_2_id] = ev_charger_pb2.State(
            component_state=ev_charger_pb2.COMPONENT_STATE_READY,
            cable_state=ev_charger_pb2.CABLE_STATE_EV_PLUGGED,
        )
        expected_states[evc_2_id] = EVChargerState.EV_PLUGGED
        await check_next_state(expected_states, (evc_2_id, EVChargerState.EV_PLUGGED))

        ## check that EV_LOCKED state gets set
        await asyncio.sleep(0.03)
        evc_3_id = mockgrid.evc_ids[3]
        mockgrid.evc_states[evc_3_id] = ev_charger_pb2.State(
            component_state=ev_charger_pb2.COMPONENT_STATE_READY,
            cable_state=ev_charger_pb2.CABLE_STATE_EV_LOCKED,
        )
        expected_states[evc_3_id] = EVChargerState.EV_LOCKED
        await check_next_state(expected_states, (evc_3_id, EVChargerState.EV_LOCKED))

        ## check that ERROR state gets set
        await asyncio.sleep(0.1)
        evc_1_id = mockgrid.evc_ids[1]
        mockgrid.evc_states[evc_1_id] = ev_charger_pb2.State(
            component_state=ev_charger_pb2.COMPONENT_STATE_ERROR,
            cable_state=ev_charger_pb2.CABLE_STATE_EV_LOCKED,
        )
        expected_states[evc_1_id] = EVChargerState.ERROR
        await check_next_state(expected_states, (evc_1_id, EVChargerState.ERROR))

        await pool._stop()  # pylint: disable=protected-access
        await mockgrid.cleanup()
