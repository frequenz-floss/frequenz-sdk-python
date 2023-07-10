# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `EVChargerPool`."""

from __future__ import annotations

import asyncio

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import (
    EVChargerCableState,
    EVChargerComponentState,
)
from frequenz.sdk.timeseries._quantities import Current, Power
from frequenz.sdk.timeseries.ev_charger_pool._state_tracker import (
    EVChargerState,
    StateTracker,
)
from tests.timeseries.mock_microgrid import MockMicrogrid


class TestEVChargerPool:
    """Tests for the `EVChargerPool`."""

    async def test_state_updates(self, mocker: MockerFixture) -> None:
        """Test ev charger state updates are visible."""

        mockgrid = MockMicrogrid(
            grid_side_meter=False, api_client_streaming=True, sample_rate_s=0.01
        )
        mockgrid.add_ev_chargers(5)
        mockgrid.start_mock_client(lambda client: client.initialize(mocker))

        state_tracker = StateTracker(set(mockgrid.evc_ids))

        async def check_states(
            expected: dict[int, EVChargerState],
        ) -> None:
            await asyncio.sleep(0.05)
            for comp_id, exp_state in expected.items():
                assert state_tracker.get(comp_id) == exp_state

        ## check that all chargers are in idle state.
        expected_states = {evc_id: EVChargerState.IDLE for evc_id in mockgrid.evc_ids}
        assert len(expected_states) == 5
        await check_states(expected_states)

        ## check that EV_PLUGGED state gets set
        evc_2_id = mockgrid.evc_ids[2]
        mockgrid.evc_cable_states[evc_2_id] = EVChargerCableState.EV_PLUGGED
        mockgrid.evc_component_states[evc_2_id] = EVChargerComponentState.READY
        expected_states[evc_2_id] = EVChargerState.EV_PLUGGED
        await check_states(expected_states)

        ## check that EV_LOCKED state gets set
        evc_3_id = mockgrid.evc_ids[3]
        mockgrid.evc_cable_states[evc_3_id] = EVChargerCableState.EV_LOCKED
        mockgrid.evc_component_states[evc_3_id] = EVChargerComponentState.READY
        expected_states[evc_3_id] = EVChargerState.EV_LOCKED
        await check_states(expected_states)

        ## check that ERROR state gets set
        evc_1_id = mockgrid.evc_ids[1]
        mockgrid.evc_cable_states[evc_1_id] = EVChargerCableState.EV_LOCKED
        mockgrid.evc_component_states[evc_1_id] = EVChargerComponentState.ERROR
        expected_states[evc_1_id] = EVChargerState.ERROR
        await check_states(expected_states)

        await state_tracker.stop()
        await mockgrid.cleanup()

    async def test_ev_active_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the ev active_power formula."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_ev_chargers(3)
        await mockgrid.start_mock_datapipeline(mocker)

        ev_pool = microgrid.ev_charger_pool()
        active_power_receiver = ev_pool.active_power.new_receiver()
        production_receiver = ev_pool.production_active_power.new_receiver()
        consumption_receiver = ev_pool.consumption_active_power.new_receiver()

        await mockgrid.mock_data.send_evc_active_power([2.0, 4.0, 10.0])
        assert (await active_power_receiver.receive()).value == Power.from_watts(16.0)
        assert (await production_receiver.receive()).value == Power.from_watts(0.0)
        assert (await consumption_receiver.receive()).value == Power.from_watts(16.0)

        await mockgrid.mock_data.send_evc_active_power([2.0, 4.0, -10.0])
        assert (await active_power_receiver.receive()).value == Power.from_watts(-4.0)
        assert (await production_receiver.receive()).value == Power.from_watts(4.0)
        assert (await consumption_receiver.receive()).value == Power.from_watts(0.0)

        await mockgrid.cleanup()

    async def test_ev_component_data(self, mocker: MockerFixture) -> None:
        """Test the component_data method of EVChargerPool."""
        mockgrid = MockMicrogrid(
            grid_side_meter=False,
            api_client_streaming=True,
            sample_rate_s=0.05,
        )
        mockgrid.add_ev_chargers(1)

        # The component_data method is a bit special because it uses both raw data
        # coming from the MicrogridClient to get the component state, and resampler data
        # to get the 3-phase current values.  So we need both `start_mock_client` and
        # `start_mock_datapipeline`.
        mockgrid.start_mock_client(lambda client: client.initialize(mocker))
        await mockgrid.start_mock_datapipeline(mocker)

        evc_id = mockgrid.evc_ids[0]
        ev_pool = microgrid.ev_charger_pool()

        recv = ev_pool.component_data(evc_id)

        await mockgrid.mock_data.send_evc_current([[2, 3, 5]])
        await asyncio.sleep(0.1)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (
            Current.from_amperes(2),
            Current.from_amperes(3),
            Current.from_amperes(5),
        )
        assert status.state == EVChargerState.MISSING

        await mockgrid.mock_data.send_evc_current([[2, 3, None]])
        await asyncio.sleep(0.1)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (
            Current.from_amperes(2),
            Current.from_amperes(3),
            None,
        )
        assert status.state == EVChargerState.IDLE

        await mockgrid.mock_data.send_evc_current([[None, None, None]])
        await asyncio.sleep(0.1)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (
            None,
            None,
            None,
        )
        assert status.state == EVChargerState.MISSING

        await mockgrid.mock_data.send_evc_current([[None, None, None]])
        mockgrid.evc_cable_states[evc_id] = EVChargerCableState.EV_PLUGGED
        await asyncio.sleep(0.1)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (
            None,
            None,
            None,
        )
        assert status.state == EVChargerState.MISSING

        await mockgrid.mock_data.send_evc_current([[4, None, None]])
        await asyncio.sleep(0.1)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (
            Current.from_amperes(4),
            None,
            None,
        )
        assert status.state == EVChargerState.EV_PLUGGED

        await mockgrid.cleanup()
        await ev_pool.stop()
