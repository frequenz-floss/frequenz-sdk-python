# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `EVChargerPool`."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from frequenz.channels import Broadcast, Receiver, Sender
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import (
    ComponentMetricId,
    EVChargerCableState,
    EVChargerComponentState,
)
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.ev_charger_pool._state_tracker import (
    EVChargerState,
    StateTracker,
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

    async def test_ev_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the ev power formula."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_ev_chargers(3)
        await mockgrid.start(mocker)

        channels: dict[int, Broadcast[Sample]] = {
            meter_id: Broadcast(f"#{meter_id}")
            for meter_id in [*mockgrid.meter_ids, *mockgrid.evc_ids]
        }
        senders: list[Sender[Sample]] = [
            channels[component_id].new_sender() for component_id in mockgrid.evc_ids
        ]

        async def send_resampled_data(
            now: datetime,
            meter_data: list[float | None],
        ) -> None:
            """Send resampled data to the channels."""
            for sender, value in zip(senders, meter_data):
                await sender.send(Sample(now, value))

        def mock_resampled_receiver(
            _1: Any, component_id: int, _2: ComponentMetricId
        ) -> Receiver[Sample]:
            return channels[component_id].new_receiver()

        mocker.patch(
            "frequenz.sdk.timeseries._formula_engine._resampled_formula_builder"
            ".ResampledFormulaBuilder._get_resampled_receiver",
            mock_resampled_receiver,
        )

        ev_pool = microgrid.ev_charger_pool()
        power_receiver = ev_pool.power.new_receiver()
        production_receiver = ev_pool.production_power.new_receiver()
        consumption_receiver = ev_pool.consumption_power.new_receiver()

        now = datetime.now(tz=timezone.utc)
        await send_resampled_data(now, [2.0, 4.0, 10.0])
        assert (await power_receiver.receive()).value == 16.0
        assert (await production_receiver.receive()).value == 0.0
        assert (await consumption_receiver.receive()).value == 16.0

        await send_resampled_data(now, [2.0, 4.0, -10.0])
        assert (await power_receiver.receive()).value == -4.0
        assert (await production_receiver.receive()).value == 4.0
        assert (await consumption_receiver.receive()).value == 0.0

        await mockgrid.cleanup()

    async def test_ev_component_data(self, mocker: MockerFixture) -> None:
        """Test the component_data method of EVChargerPool."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_ev_chargers(1)
        await mockgrid.start(mocker)
        evc_id = mockgrid.evc_ids[0]

        ev_pool = microgrid.ev_charger_pool()

        resampled_p1_channel = Broadcast[Sample]("resampled-current-phase-1")
        resampled_p2_channel = Broadcast[Sample]("resampled-current-phase-2")
        resampled_p3_channel = Broadcast[Sample]("resampled-current-phase-3")

        async def send_resampled_current(
            phase_1: float | None, phase_2: float | None, phase_3: float | None
        ) -> None:
            sender_p1 = resampled_p1_channel.new_sender()
            sender_p2 = resampled_p2_channel.new_sender()
            sender_p3 = resampled_p3_channel.new_sender()

            now = datetime.now()
            asyncio.gather(
                sender_p1.send(Sample(now, phase_1)),
                sender_p2.send(Sample(now, phase_2)),
                sender_p3.send(Sample(now, phase_3)),
            )

        async def mock_current_streams(
            _1: Any, _2: int
        ) -> tuple[Receiver[Sample], Receiver[Sample], Receiver[Sample]]:
            return (
                resampled_p1_channel.new_receiver(),
                resampled_p2_channel.new_receiver(),
                resampled_p3_channel.new_receiver(),
            )

        mocker.patch(
            "frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool._get_current_streams",
            mock_current_streams,
        )

        recv = ev_pool.component_data(evc_id)

        await send_resampled_current(2, 3, 5)
        await asyncio.sleep(0.02)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (2, 3, 5)
        assert status.state == EVChargerState.MISSING

        await send_resampled_current(2, 3, None)
        await asyncio.sleep(0.02)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (2, 3, None)
        assert status.state == EVChargerState.IDLE

        await send_resampled_current(None, None, None)
        await asyncio.sleep(0.02)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (None, None, None)
        assert status.state == EVChargerState.MISSING

        await send_resampled_current(None, None, None)
        mockgrid.evc_cable_states[evc_id] = EVChargerCableState.EV_PLUGGED
        await asyncio.sleep(0.02)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (None, None, None)
        assert status.state == EVChargerState.MISSING

        await send_resampled_current(4, None, None)
        await asyncio.sleep(0.02)
        status = await recv.receive()
        assert (
            status.current.value_p1,
            status.current.value_p2,
            status.current.value_p3,
        ) == (4, None, None)
        assert status.state == EVChargerState.EV_PLUGGED

        await mockgrid.cleanup()
