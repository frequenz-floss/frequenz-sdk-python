# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests power distributor"""
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Dict, Set, Tuple, TypeVar, Union
from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Bidirectional, Broadcast, Receiver, Sender
from google.protobuf.empty_pb2 import Empty  # pylint: disable=no-name-in-module
from pytest_mock import MockerFixture

from frequenz.sdk.actor.power_distributing import (
    PowerDistributingActor,
    Request,
    Result,
)
from frequenz.sdk.actor.power_distributing.power_distributing import _BrokenComponents
from frequenz.sdk.microgrid._graph import _MicrogridComponentGraph
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    InverterData,
)

from ..conftest import SAFETY_TIMEOUT
from ..power.test_distribution_algorithm import (
    Bound,
    Metric,
    create_battery_msg,
    create_inverter_msg,
)

T = TypeVar("T")  # Declare type variable


@dataclass
class User:
    """User definition."""

    user_id: str
    sender: Sender[Request]
    receiver: Receiver[Result]


class TestPowerDistributingActor(IsolatedAsyncioTestCase):
    # pylint: disable=protected-access
    """Test tool to distribute power"""

    def create_bat_channels(
        self, components: Set[Component]
    ) -> Dict[int, Broadcast[BatteryData]]:
        """Create channels for the given components.

        Args:
            components: set of components.

        Returns:
            Dictionary where the key is battery id and the value is channel for this
                battery.
        """
        bat_components = [
            c for c in components if c.category == ComponentCategory.BATTERY
        ]
        bat_channels: Dict[int, Broadcast[BatteryData]] = {
            c.component_id: Broadcast[BatteryData]("bat" + str(c.component_id))
            for c in bat_components
        }
        return bat_channels

    def create_inv_channels(
        self, components: Set[Component]
    ) -> Dict[int, Broadcast[InverterData]]:
        """Create channels for the given components.

        Args:
            components: set of components.

        Returns:
            Dictionary where the key is inverter id and the value is channel for this
                inverter.
        """

        inv_components = [
            c for c in components if c.category == ComponentCategory.INVERTER
        ]
        inv_channels: Dict[int, Broadcast[InverterData]] = {
            c.component_id: Broadcast[InverterData]("bat" + str(c.component_id))
            for c in inv_components
        }
        return inv_channels

    def return_channel(
        self,
        component_id: int,
        channels: Dict[int, Broadcast[Union[BatteryData, InverterData]]],
    ) -> Receiver[Union[BatteryData, InverterData]]:
        """Return receiver of the broadcast channel for given component_id.

        Args:
            component_id (int): component_id
            channels (Dict[int, Broadcast[Union[BatteryData, InverterData]]]): Broadcast channels

        Returns:
            Receiver[Union[BatteryData, InverterData]]: Receiver from the given
                channels.
        """
        return channels[component_id].new_receiver("component" + str(component_id))

    def mock_api(
        self,
        components: Set[Component],
        bat_channel: Dict[int, Broadcast[BatteryData]],
        inv_channel: Dict[int, Broadcast[InverterData]],
    ) -> MagicMock:
        """Create mock of MicrogridApiClient.

        Args:
            components: set of components.
            bat_channel: battery channels to be returned from
                MicrogridApiClient.battery_data.
            inv_channel: inverter channels to be returned from
                MicrogridApiClient.inverter_data.

        Returns:
            Magic mock instance of MicrogridApiClient.
        """
        api = MagicMock()
        api.components = AsyncMock(return_value=components)
        api.battery_data = AsyncMock(
            side_effect=partial(self.return_channel, channels=bat_channel)
        )

        api.inverter_data = AsyncMock(
            side_effect=partial(self.return_channel, channels=inv_channel)
        )
        api.set_power = AsyncMock(return_value=Empty)
        return api

    def default_components_graph(self) -> Tuple[Set[Component], Set[Connection]]:
        """Create components graph and connections between them.

        Returns:
            Tuple where first element is set of components and second element is
                set of connections.
        """
        components: Set[Component] = {
            Component(1, ComponentCategory.GRID),
            Component(2, ComponentCategory.METER),
            Component(3, ComponentCategory.JUNCTION),
            Component(104, ComponentCategory.METER),
            Component(105, ComponentCategory.INVERTER),
            Component(106, ComponentCategory.BATTERY),
            Component(204, ComponentCategory.METER),
            Component(205, ComponentCategory.INVERTER),
            Component(206, ComponentCategory.BATTERY),
            Component(304, ComponentCategory.METER),
            Component(305, ComponentCategory.INVERTER),
            Component(306, ComponentCategory.BATTERY),
        }
        connections = {
            Connection(1, 2),
            Connection(2, 3),
            Connection(3, 104),
            Connection(104, 105),
            Connection(105, 106),
            Connection(3, 204),
            Connection(204, 205),
            Connection(205, 206),
            Connection(3, 304),
            Connection(304, 305),
            Connection(305, 306),
        }
        return components, connections

    async def test_constructor(self) -> None:
        """Test if gets all necessary data."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        channel = Bidirectional[Request, Result]("user1", "power_distributor")
        distributor = PowerDistributingActor(
            mock_api, component_graph, {"user1": channel.service_handle}
        )

        assert distributor._bat_inv_map == {106: 105, 206: 205, 306: 305}
        assert distributor._inv_bat_map == {105: 106, 205: 206, 305: 306}
        await distributor._stop()  # type: ignore # pylint: disable=no-member

    async def test_power_distributor_one_user(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        for key_id, chan in bat_channels.items():
            sender = chan.new_sender()
            bat = create_battery_msg(
                key_id,
                capacity=Metric(98000),
                soc=Metric(40, Bound(20, 80)),
                power=Bound(-1000, 1000),
            )
            await sender.send(BatteryData.from_proto(bat))

        for key_id, inv_chan in inv_channels.items():
            inv_sender = inv_chan.new_sender()
            inv = create_inverter_msg(
                key_id,
                power=Bound(-500, 500),
            )
            await inv_sender.send(InverterData.from_proto(inv))

        channel = Bidirectional[Request, Result]("user1", "power_distributor")
        with mock.patch("asyncio.sleep", new_callable=AsyncMock):
            distributor = PowerDistributingActor(
                mock_api, component_graph, {"user1": channel.service_handle}
            )

            client_handle = channel.client_handle
            await client_handle.send(
                Request(
                    power=1200, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            done, pending = await asyncio.wait(
                [client_handle.receive()], timeout=SAFETY_TIMEOUT
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert result is not None
        assert result.status == Result.Status.SUCCESS
        assert result.failed_power == 0
        assert result.above_upper_bound == 200

    async def test_power_distributor_two_users(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with two users."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        for key_id, chan in bat_channels.items():
            sender = chan.new_sender()
            bat = create_battery_msg(
                key_id,
                capacity=Metric(98000),
                soc=Metric(40, Bound(20, 80)),
                power=Bound(-1000, 1000),
            )
            await sender.send(BatteryData.from_proto(bat))

        for key_id, inv_chan in inv_channels.items():
            inv_sender = inv_chan.new_sender()
            inv = create_inverter_msg(
                key_id,
                power=Bound(-500, 500),
            )
            await inv_sender.send(InverterData.from_proto(inv))

        channel1 = Bidirectional[Request, Result]("user1", "power_distributor")
        channel2 = Bidirectional[Request, Result]("user2", "power_distributor")
        service_channels = {
            "user1": channel1.service_handle,
            "user2": channel2.service_handle,
        }

        with mock.patch("asyncio.sleep", new_callable=AsyncMock):
            distributor = PowerDistributingActor(
                mock_api, component_graph, service_channels
            )

            user1_handle = channel1.client_handle
            task1 = user1_handle.send(
                Request(
                    power=1200, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            user2_handle = channel2.client_handle
            task2 = user2_handle.send(
                Request(
                    power=1300, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            await asyncio.gather(*[task1, task2])

            done, pending = await asyncio.wait(
                [user1_handle.receive(), user2_handle.receive()], timeout=SAFETY_TIMEOUT
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 2

        success, ignored = 0, 0
        for item in done:
            result = item.result()
            assert result is not None
            if result.status == Result.Status.SUCCESS:
                success += 1
            elif result.status == Result.Status.IGNORED:
                ignored += 1

        assert success == 1
        assert ignored == 1

    async def test_power_distributor_invalid_battery_id(self) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution raises error if any battery id is invalid."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        for key_id, chan in bat_channels.items():
            sender = chan.new_sender()
            bat = create_battery_msg(
                key_id,
                capacity=Metric(98000),
                soc=Metric(40, Bound(20, 80)),
                power=Bound(-1000, 1000),
            )
            await sender.send(BatteryData.from_proto(bat))

        for key_id, inv_chan in inv_channels.items():
            inv_sender = inv_chan.new_sender()
            inv = create_inverter_msg(
                key_id,
                power=Bound(-500, 500),
            )
            await inv_sender.send(InverterData.from_proto(inv))

        channel1 = Bidirectional[Request, Result]("user1", "power_distributor")
        service_channels = {
            "user1": channel1.service_handle,
        }
        with mock.patch("asyncio.sleep", new_callable=AsyncMock):
            distributor = PowerDistributingActor(
                mock_api, component_graph, service_channels
            )

            user1_handle = channel1.client_handle
            await user1_handle.send(
                Request(
                    power=1200, batteries={106, 208}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            done, _ = await asyncio.wait(
                [user1_handle.receive()], timeout=SAFETY_TIMEOUT
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(done) == 1
        result = done.pop().result()
        assert result is not None
        assert result.status == Result.Status.ERROR
        assert result.error_message is not None
        re.search(
            r"^No battery 208, available batteries: [106, 206, 306]",
            result.error_message,
        )

    async def test_power_distributor_overlapping_batteries(self) -> None:
        # pylint: disable=too-many-locals
        """Test if requests with overlapping set of batteries are processed."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        for key_id, chan in bat_channels.items():
            sender = chan.new_sender()
            bat = create_battery_msg(
                key_id,
                capacity=Metric(98000),
                soc=Metric(40, Bound(20, 80)),
                power=Bound(-1000, 1000),
            )
            await sender.send(BatteryData.from_proto(bat))

        for key_id, inv_chan in inv_channels.items():
            inv_sender = inv_chan.new_sender()
            inv = create_inverter_msg(
                key_id,
                power=Bound(-500, 500),
            )
            await inv_sender.send(InverterData.from_proto(inv))

        channel1 = Bidirectional[Request, Result]("user1", "power_distributor")
        channel2 = Bidirectional[Request, Result]("user2", "power_distributor")
        channel3 = Bidirectional[Request, Result]("user3", "power_distributor")
        service_channels = {
            "user1": channel1.service_handle,
            "user2": channel2.service_handle,
            "user3": channel3.service_handle,
        }

        with mock.patch("asyncio.sleep", new_callable=AsyncMock):
            distributor = PowerDistributingActor(
                mock_api, component_graph, service_channels
            )

            user1_handle = channel1.client_handle
            task1 = user1_handle.send(
                Request(
                    power=1200, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            user2_handle = channel2.client_handle
            task2 = user2_handle.send(
                Request(
                    power=1200, batteries={106, 306}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            user3_handle = channel3.client_handle
            task3 = user3_handle.send(
                Request(
                    power=1200, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            await asyncio.gather(*[task1, task2, task3])

            done, _ = await asyncio.wait(
                [
                    user1_handle.receive(),
                    user2_handle.receive(),
                    user3_handle.receive(),
                ],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(done) == 3
        success, ignored = 0, 0
        for item in done:
            result = item.result()
            assert result is not None
            if result.status == Result.Status.SUCCESS:
                success += 1
            elif result.status == Result.Status.IGNORED:
                ignored += 1

        assert success >= 2
        assert ignored <= 1

    async def test_power_distributor_one_user_adjust_power_out_of_bound(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        for key_id, chan in bat_channels.items():
            sender = chan.new_sender()
            bat = create_battery_msg(
                key_id,
                capacity=Metric(98000),
                soc=Metric(40, Bound(20, 80)),
                power=Bound(-1000, 1000),
            )
            await sender.send(BatteryData.from_proto(bat))

        for key_id, inv_chan in inv_channels.items():
            inv_sender = inv_chan.new_sender()
            inv = create_inverter_msg(
                key_id,
                power=Bound(-500, 500),
            )
            await inv_sender.send(InverterData.from_proto(inv))

        channel1 = Bidirectional[Request, Result]("user1", "power_distributor")
        service_channels = {
            "user1": channel1.service_handle,
        }

        with mock.patch("asyncio.sleep", new_callable=AsyncMock):
            distributor = PowerDistributingActor(
                mock_api, component_graph, service_channels
            )

            user1_handle = channel1.client_handle
            await user1_handle.send(
                Request(
                    power=1200,
                    batteries={106, 206},
                    request_timeout_sec=SAFETY_TIMEOUT,
                    adjust_power=False,
                )
            )
            done, pending = await asyncio.wait(
                [user1_handle.receive()], timeout=SAFETY_TIMEOUT
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert result is not None
        assert result.status == Result.Status.OUT_OF_BOUND
        assert result.failed_power == 1200
        assert result.above_upper_bound == 0

    async def test_power_distributor_one_user_adjust_power_success(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        for key_id, chan in bat_channels.items():
            sender = chan.new_sender()
            bat = create_battery_msg(
                key_id,
                capacity=Metric(98000),
                soc=Metric(40, Bound(20, 80)),
                power=Bound(-1000, 1000),
            )
            await sender.send(BatteryData.from_proto(bat))

        for key_id, inv_chan in inv_channels.items():
            inv_sender = inv_chan.new_sender()
            inv = create_inverter_msg(
                key_id,
                power=Bound(-500, 500),
            )
            await inv_sender.send(InverterData.from_proto(inv))

        channel1 = Bidirectional[Request, Result]("user1", "power_distributor")
        service_channels = {
            "user1": channel1.service_handle,
        }

        with mock.patch("asyncio.sleep", new_callable=AsyncMock):
            distributor = PowerDistributingActor(
                mock_api, component_graph, service_channels
            )

            user1_handle = channel1.client_handle
            await user1_handle.send(
                Request(
                    power=1000,
                    batteries={106, 206},
                    request_timeout_sec=SAFETY_TIMEOUT,
                    adjust_power=False,
                )
            )

            done, pending = await asyncio.wait(
                [user1_handle.receive()], timeout=SAFETY_TIMEOUT
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert result is not None
        assert result.status == Result.Status.SUCCESS
        assert result.failed_power == 0
        assert result.above_upper_bound == 0

    async def test_power_distributor_stale_battery_message(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        for key_id, chan in bat_channels.items():
            sender = chan.new_sender()
            if key_id == 106:
                # this battery should has outdated data
                bat = create_battery_msg(
                    key_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=Bound(-1000, 1000),
                    timestamp=datetime.now(timezone.utc) - timedelta(seconds=62),
                )
            else:
                bat = create_battery_msg(
                    key_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=Bound(-1000, 1000),
                )
            await sender.send(BatteryData.from_proto(bat))

        for key_id, inv_chan in inv_channels.items():
            inv_sender = inv_chan.new_sender()
            inv = create_inverter_msg(
                key_id,
                power=Bound(-500, 500),
            )
            await inv_sender.send(InverterData.from_proto(inv))

        channel = Bidirectional[Request, Result]("user1", "power_distributor")

        with mock.patch("asyncio.sleep", new_callable=AsyncMock):
            distributor = PowerDistributingActor(
                mock_api, component_graph, {"user1": channel.service_handle}
            )

            client_handle = channel.client_handle
            await client_handle.send(
                Request(
                    power=1200, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            done, pending = await asyncio.wait(
                [client_handle.receive()], timeout=SAFETY_TIMEOUT
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert result is not None
        assert result.status == Result.Status.SUCCESS
        assert result.failed_power == 0
        assert result.above_upper_bound == 700

    async def test_power_distributor_stale_all_components_message(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        components, connections = self.default_components_graph()
        component_graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        for key_id, chan in bat_channels.items():
            sender = chan.new_sender()
            if key_id == 106:
                # this battery should has outdated data
                bat = create_battery_msg(
                    key_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=Bound(-1000, 1000),
                    timestamp=datetime.now(timezone.utc) - timedelta(seconds=62),
                )
            else:
                bat = create_battery_msg(
                    key_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=Bound(-1000, 1000),
                )
            await sender.send(BatteryData.from_proto(bat))

        for key_id, inv_chan in inv_channels.items():
            inv_sender = inv_chan.new_sender()
            if key_id == 205:
                inv = create_inverter_msg(
                    key_id,
                    power=Bound(-500, 500),
                    timestamp=datetime.now(timezone.utc) - timedelta(seconds=62),
                )
            else:
                inv = create_inverter_msg(
                    key_id,
                    power=Bound(-500, 500),
                )
            await inv_sender.send(InverterData.from_proto(inv))

        channel = Bidirectional[Request, Result]("user1", "power_distributor")

        with mock.patch("asyncio.sleep", new_callable=AsyncMock):
            distributor = PowerDistributingActor(
                mock_api, component_graph, {"user1": channel.service_handle}
            )

            client_handle = channel.client_handle
            await client_handle.send(
                Request(
                    power=1200, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
                )
            )

            done, pending = await asyncio.wait(
                [client_handle.receive()], timeout=SAFETY_TIMEOUT
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert result is not None
        assert result.status == Result.Status.ERROR
        # User is interested in batteries only.
        assert result.error_message == "No data for the given batteries {106, 206}"


class TestBrokenComponents:
    """Test if BrokenComponents class is working as expected."""

    def test_broken_components(self, mocker: MockerFixture) -> None:
        """Check if components are blocked for 30 seconds.

        Args:
            mocker: pytest mocker
        """
        datetime_mock = mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.datetime"
        )

        expected_datetime = [
            datetime.fromisoformat("2001-01-01T00:00:00+00:00"),
            datetime.fromisoformat("2001-01-01T00:00:10+00:00"),
            datetime.fromisoformat("2001-01-01T00:00:20+00:00"),
        ]
        expected_datetime.extend(
            20 * [datetime.fromisoformat("2001-01-01T00:00:31+00:00")]
        )

        datetime_mock.now.side_effect = expected_datetime

        # After 30 seconds components should be considered as working
        broken = _BrokenComponents(30)

        for component_id in range(3):
            broken.mark_as_broken(component_id)

        # Component 0 was marked as broken 30 seconds ago. Other components, not.
        assert not broken.is_broken(0)
        assert broken.is_broken(1)
        assert broken.is_broken(2)
        assert broken.get_working_subset({0, 1}) == {0}
        assert broken.get_working_subset({0}) == {0}

        # If all requested components are marked as broken,
        # then we should mark them as working to not block user command.
        assert broken.get_working_subset({1, 2}) == {1, 2}
