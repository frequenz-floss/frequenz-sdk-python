# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for EVChargerStatusTracker."""

import asyncio
from datetime import datetime, timedelta, timezone

from frequenz.channels import Broadcast
from frequenz.client.microgrid import EVChargerCableState, EVChargerComponentState
from pytest_mock import MockerFixture

from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk.actor.power_distributing._component_status import (
    ComponentStatus,
    ComponentStatusEnum,
    EVChargerStatusTracker,
    SetPowerResult,
)

from ....timeseries.mock_microgrid import MockMicrogrid
from ....utils.component_data_wrapper import EvChargerDataWrapper
from ....utils.receive_timeout import Timeout, receive_timeout

_EV_CHARGER_ID = 6


class TestEVChargerStatusTracker:
    """Tests for EVChargerStatusTracker."""

    async def test_status_changes(self, mocker: MockerFixture) -> None:
        """Test that the status changes as expected."""
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_ev_chargers(3)

        status_channel = Broadcast[ComponentStatus](name="battery_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")
        set_power_result_sender = set_power_result_channel.new_sender()

        async with (
            mock_microgrid,
            EVChargerStatusTracker(
                component_id=_EV_CHARGER_ID,
                max_data_age=timedelta(seconds=0.2),
                max_blocking_duration=timedelta(seconds=1),
                status_sender=status_channel.new_sender(),
                set_power_result_receiver=set_power_result_channel.new_receiver(),
            ),
        ):
            status_receiver = status_channel.new_receiver()
            # The status is initially not working.
            assert (
                await status_receiver.receive()
            ).value == ComponentStatusEnum.NOT_WORKING

            # When an EV is plugged, it is working
            await mock_microgrid.mock_client.send(
                EvChargerDataWrapper(
                    _EV_CHARGER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    component_state=EVChargerComponentState.READY,
                    cable_state=EVChargerCableState.EV_PLUGGED,
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _EV_CHARGER_ID, ComponentStatusEnum.WORKING
            )

            # When an EV is locked, no change in status
            await mock_microgrid.mock_client.send(
                EvChargerDataWrapper(
                    _EV_CHARGER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    component_state=EVChargerComponentState.READY,
                    cable_state=EVChargerCableState.EV_LOCKED,
                )
            )
            assert await receive_timeout(status_receiver) is Timeout

            # When an EV is unplugged, it is not working
            await mock_microgrid.mock_client.send(
                EvChargerDataWrapper(
                    _EV_CHARGER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    component_state=EVChargerComponentState.READY,
                    cable_state=EVChargerCableState.UNPLUGGED,
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _EV_CHARGER_ID, ComponentStatusEnum.NOT_WORKING
            )

            # Get it back to working again
            await mock_microgrid.mock_client.send(
                EvChargerDataWrapper(
                    _EV_CHARGER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    component_state=EVChargerComponentState.READY,
                    cable_state=EVChargerCableState.EV_LOCKED,
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _EV_CHARGER_ID, ComponentStatusEnum.WORKING
            )

            # When there's no new data, it should become not working
            assert await receive_timeout(status_receiver, 0.1) is Timeout
            assert await receive_timeout(status_receiver, 0.2) == ComponentStatus(
                _EV_CHARGER_ID, ComponentStatusEnum.NOT_WORKING
            )

            # Get it back to working again
            await asyncio.sleep(0.1)
            await mock_microgrid.mock_client.send(
                EvChargerDataWrapper(
                    _EV_CHARGER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    component_state=EVChargerComponentState.READY,
                    cable_state=EVChargerCableState.EV_LOCKED,
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _EV_CHARGER_ID, ComponentStatusEnum.WORKING
            )

            async def keep_sending_healthy_message() -> None:
                while True:
                    await mock_microgrid.mock_client.send(
                        EvChargerDataWrapper(
                            _EV_CHARGER_ID,
                            datetime.now(tz=timezone.utc),
                            active_power=0.0,
                            component_state=EVChargerComponentState.READY,
                            cable_state=EVChargerCableState.EV_LOCKED,
                        )
                    )
                    await asyncio.sleep(0.1)

            _keep_sending_healthy_message_task = asyncio.create_task(
                keep_sending_healthy_message()
            )

            # when there's a PowerDistributor failure for the component, status should
            # become uncertain.
            await set_power_result_sender.send(
                SetPowerResult(
                    succeeded=set(),
                    failed={_EV_CHARGER_ID},
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _EV_CHARGER_ID, ComponentStatusEnum.UNCERTAIN
            )

            # After the blocking duration, it should become working again.
            assert await receive_timeout(status_receiver) is Timeout
            assert await receive_timeout(status_receiver, 1.0) == ComponentStatus(
                _EV_CHARGER_ID, ComponentStatusEnum.WORKING
            )
            await cancel_and_await(_keep_sending_healthy_message_task)
