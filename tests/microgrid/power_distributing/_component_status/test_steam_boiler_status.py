# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for SteamBoilerStatusTracker."""

import asyncio
from datetime import datetime, timedelta, timezone

from frequenz.channels import Broadcast
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import ComponentStateCode
from pytest_mock import MockerFixture

from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk.microgrid._power_distributing._component_status import (
    ComponentStatus,
    ComponentStatusEnum,
    SetPowerResult,
    SteamBoilerStatusTracker,
)

from ....timeseries.mock_microgrid import MockMicrogrid
from ....utils.component_data_wrapper import SteamBoilerDataWrapper
from ....utils.receive_timeout import Timeout, receive_timeout

_STEAM_BOILER_ID = ComponentId(3)


class TestSteamBoilerStatusTracker:
    """Tests for SteamBoilerStatusTracker."""

    async def test_status_changes(self, mocker: MockerFixture) -> None:
        """Test that the status changes as expected."""
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_steam_boilers(1)

        status_channel = Broadcast[ComponentStatus](name="steam_boiler_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")
        set_power_result_sender = set_power_result_channel.new_sender()

        async with (
            mock_microgrid,
            SteamBoilerStatusTracker(
                component_id=_STEAM_BOILER_ID,
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

            # When there's healthy steam boiler data, status should be working.
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.READY},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.WORKING
            )

            # When it is charging, there should be no change in status
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.CHARGING},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) is Timeout

            # Steam boilers only consume: DISCHARGING is not a healthy state, so
            # the status should be not working.
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.DISCHARGING},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.NOT_WORKING
            )

            # STANDBY is not a healthy state either: the status should stay
            # not working.
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.STANDBY},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) is Timeout

            # Get it back to working again
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.READY},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.WORKING
            )

            # When there an error message, status should be not working
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.ERROR},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.NOT_WORKING
            )

            # Get it back to working again
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.READY},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.WORKING
            )

            # When data with an old timestamp arrives, status should be not working
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc) - timedelta(seconds=1),
                    active_power=0.0,
                    states={ComponentStateCode.READY},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.NOT_WORKING
            )

            # Fresh data should bring it back to working
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.READY},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.WORKING
            )

            # When there's no new data, status should be not working
            assert await receive_timeout(status_receiver, 0.1) is Timeout
            assert await receive_timeout(status_receiver, 0.2) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.NOT_WORKING
            )

            # Get it back to working again
            await mock_microgrid.mock_client.send(
                SteamBoilerDataWrapper(
                    _STEAM_BOILER_ID,
                    datetime.now(tz=timezone.utc),
                    active_power=0.0,
                    states={ComponentStateCode.READY},
                ).to_samples()
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.WORKING
            )

            async def keep_sending_healthy_message() -> None:
                """Keep sending healthy messages."""
                while True:
                    await mock_microgrid.mock_client.send(
                        SteamBoilerDataWrapper(
                            _STEAM_BOILER_ID,
                            datetime.now(tz=timezone.utc),
                            active_power=0.0,
                            states={ComponentStateCode.READY},
                        ).to_samples()
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
                    failed={_STEAM_BOILER_ID},
                )
            )
            assert await receive_timeout(status_receiver) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.UNCERTAIN
            )

            # After the blocking duration, it should become working again.
            assert await receive_timeout(status_receiver) is Timeout
            assert await receive_timeout(status_receiver, 1.0) == ComponentStatus(
                _STEAM_BOILER_ID, ComponentStatusEnum.WORKING
            )
            await cancel_and_await(_keep_sending_healthy_message_task)
