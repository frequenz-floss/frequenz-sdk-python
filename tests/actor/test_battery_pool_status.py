# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH
"""Tests for BatteryPoolStatus."""

import asyncio

from frequenz.channels import Broadcast
from pytest_mock import MockerFixture

from frequenz.sdk.actor.power_distributing._battery_status_tracker import (
    BatteryStatusTracker,
)
from frequenz.sdk.actor.power_distributing._component_pool_status_tracker import (
    ComponentPoolStatusTracker,
)
from frequenz.sdk.actor.power_distributing._component_status import ComponentPoolStatus
from frequenz.sdk.microgrid.component import ComponentCategory
from tests.timeseries.mock_microgrid import MockMicrogrid

from .test_battery_status import battery_data, inverter_data


# pylint: disable=protected-access
class TestBatteryPoolStatus:
    """Tests for BatteryPoolStatus."""

    async def test_batteries_status(self, mocker: MockerFixture) -> None:
        """Basic tests for BatteryPoolStatus.

        BatteryStatusTracker is more tested in its own unit tests.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True)
        mock_microgrid.add_batteries(3)
        await mock_microgrid.start(mocker)

        batteries = {
            battery.component_id
            for battery in mock_microgrid.mock_client.component_graph.components(
                component_categories={ComponentCategory.BATTERY}
            )
        }
        battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
        battery_status_recv = battery_status_channel.new_receiver(maxsize=1)
        batteries_status = ComponentPoolStatusTracker(
            component_ids=batteries,
            component_status_sender=battery_status_channel.new_sender(),
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            component_status_tracker_type=BatteryStatusTracker,
        )
        await asyncio.sleep(0.1)

        expected_working: set[int] = set()
        assert batteries_status.get_working_components(batteries) == expected_working

        batteries_list = list(batteries)

        await mock_microgrid.mock_client.send(
            battery_data(component_id=batteries_list[0])
        )
        await asyncio.sleep(0.1)
        assert batteries_status.get_working_components(batteries) == expected_working

        expected_working.add(batteries_list[0])
        await mock_microgrid.mock_client.send(
            inverter_data(component_id=batteries_list[0] - 1)
        )
        await asyncio.sleep(0.1)
        assert batteries_status.get_working_components(batteries) == expected_working
        msg = await asyncio.wait_for(battery_status_recv.receive(), timeout=0.2)
        assert msg == batteries_status._current_status

        await mock_microgrid.mock_client.send(
            inverter_data(component_id=batteries_list[1] - 1)
        )
        await mock_microgrid.mock_client.send(
            battery_data(component_id=batteries_list[1])
        )

        await mock_microgrid.mock_client.send(
            inverter_data(component_id=batteries_list[2] - 1)
        )
        await mock_microgrid.mock_client.send(
            battery_data(component_id=batteries_list[2])
        )

        expected_working = set(batteries_list)
        await asyncio.sleep(0.1)
        assert batteries_status.get_working_components(batteries) == expected_working
        msg = await asyncio.wait_for(battery_status_recv.receive(), timeout=0.2)
        assert msg == batteries_status._current_status

        await batteries_status.update_status(
            succeeded_components={9}, failed_components={19, 29}
        )
        await asyncio.sleep(0.1)
        assert batteries_status.get_working_components(batteries) == {9}

        await batteries_status.update_status(
            succeeded_components={9, 19}, failed_components=set()
        )
        await asyncio.sleep(0.1)
        assert batteries_status.get_working_components(batteries) == {9, 19}

        await batteries_status.stop()
