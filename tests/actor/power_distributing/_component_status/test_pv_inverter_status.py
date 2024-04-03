# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for PVInverterStatusTracker."""


import asyncio
from datetime import datetime, timedelta, timezone

# pylint: disable=no-name-in-module
from frequenz.api.microgrid.inverter_pb2 import (
    ComponentState as PbInverterComponentState,
)

# pylint: enable=no-name-in-module
from frequenz.channels import Broadcast
from pytest_mock import MockerFixture

from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk.actor.power_distributing._component_status import (
    ComponentStatus,
    ComponentStatusEnum,
    PVInverterStatusTracker,
    SetPowerResult,
)

from ....timeseries.mock_microgrid import MockMicrogrid
from ....utils.component_data_wrapper import InverterDataWrapper
from ....utils.receive_timeout import Timeout, receive_timeout

_PV_INVERTER_ID = 8


class TestPVInverterStatusTracker:
    """Tests for PVInverterStatusTracker."""

    async def test_status_changes(self, mocker: MockerFixture) -> None:
        """Test that the status changes as expected."""
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_solar_inverters(1)

        status_channel = Broadcast[ComponentStatus](name="pv_inverter_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")
        set_power_result_sender = set_power_result_channel.new_sender()

        async with (
            mock_microgrid,
            PVInverterStatusTracker(
                component_id=_PV_INVERTER_ID,
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

            # When there's healthy inverter data, status should be working.
            await mock_microgrid.mock_client.send(
                InverterDataWrapper(
                    _PV_INVERTER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    _component_state=PbInverterComponentState.COMPONENT_STATE_IDLE,
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _PV_INVERTER_ID, ComponentStatusEnum.WORKING
            )

            # When it is discharging, there should be no change in status
            await mock_microgrid.mock_client.send(
                InverterDataWrapper(
                    _PV_INVERTER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    _component_state=PbInverterComponentState.COMPONENT_STATE_DISCHARGING,
                )
            )
            assert await receive_timeout(status_receiver) is Timeout

            # When there an error message, status should be not working
            await mock_microgrid.mock_client.send(
                InverterDataWrapper(
                    _PV_INVERTER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    _component_state=PbInverterComponentState.COMPONENT_STATE_ERROR,
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _PV_INVERTER_ID, ComponentStatusEnum.NOT_WORKING
            )

            # Get it back to working again
            await mock_microgrid.mock_client.send(
                InverterDataWrapper(
                    _PV_INVERTER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    _component_state=PbInverterComponentState.COMPONENT_STATE_IDLE,
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _PV_INVERTER_ID, ComponentStatusEnum.WORKING
            )

            # When there's no new data, status should be not working
            assert await receive_timeout(status_receiver, 0.1) is Timeout
            assert await receive_timeout(status_receiver, 0.2) == ComponentStatus(
                _PV_INVERTER_ID, ComponentStatusEnum.NOT_WORKING
            )

            # Get it back to working again
            await mock_microgrid.mock_client.send(
                InverterDataWrapper(
                    _PV_INVERTER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    _component_state=PbInverterComponentState.COMPONENT_STATE_IDLE,
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _PV_INVERTER_ID, ComponentStatusEnum.WORKING
            )

            async def keep_sending_healthy_message() -> None:
                """Keep sending healthy messages."""
                while True:
                    await mock_microgrid.mock_client.send(
                        InverterDataWrapper(
                            _PV_INVERTER_ID,
                            datetime.now(tz=timezone.utc),
                            active_power=0.0,
                            _component_state=PbInverterComponentState.COMPONENT_STATE_IDLE,
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
                    failed={_PV_INVERTER_ID},
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _PV_INVERTER_ID, ComponentStatusEnum.UNCERTAIN
            )

            # After the blocking duration, it should become working again.
            assert await receive_timeout(status_receiver) is Timeout
            assert await receive_timeout(status_receiver, 1.0) == ComponentStatus(
                _PV_INVERTER_ID, ComponentStatusEnum.WORKING
            )
            await cancel_and_await(_keep_sending_healthy_message_task)
