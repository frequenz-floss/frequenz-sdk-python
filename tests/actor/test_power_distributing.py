# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests power distributor"""
import asyncio
import re
from dataclasses import dataclass
from functools import partial
from typing import Dict, Set, Tuple, TypeVar, Union
from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Bidirectional, Broadcast, Receiver, Sender
from google.protobuf.empty_pb2 import Empty  # pylint: disable=no-name-in-module

from frequenz.sdk.actor.power_distributing import PowerDistributingActor, Request
from frequenz.sdk.actor.power_distributing._battery_pool_status import BatteryPoolStatus
from frequenz.sdk.actor.power_distributing.result import (
    Error,
    Ignored,
    OutOfBound,
    Result,
    Success,
)
from frequenz.sdk.microgrid import Microgrid
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

    def create_mock_microgrid(
        self,
    ) -> Tuple[
        Microgrid, Dict[int, Broadcast[BatteryData]], Dict[int, Broadcast[InverterData]]
    ]:
        """Create mock microgrid.

        Returns:
            Tuple with:
                * mock microgrid instance
                * channels for all batteries in microgrid
                * channels for all inverters in microgrid
        """
        components, connections = self.default_components_graph()
        graph = _MicrogridComponentGraph(components, connections)
        bat_channels = self.create_bat_channels(components)
        inv_channels = self.create_inv_channels(components)
        mock_api = self.mock_api(components, bat_channels, inv_channels)

        kwargs = {"api_client": mock_api, "component_graph": graph}
        mock_microgrid = MagicMock(spec=Microgrid, **kwargs)

        return mock_microgrid, bat_channels, inv_channels

    async def test_constructor(self) -> None:
        """Test if gets all necessary data."""
        mock_microgrid, _, _ = self.create_mock_microgrid()

        with mock.patch("frequenz.sdk.microgrid.get", return_value=mock_microgrid):
            channel = Bidirectional[Request, Result]("user1", "power_distributor")
            distributor = PowerDistributingActor({"user1": channel.service_handle})

            assert distributor._bat_inv_map == {106: 105, 206: 205, 306: 305}
            assert distributor._inv_bat_map == {105: 106, 205: 206, 305: 306}
            await distributor._stop()  # type: ignore # pylint: disable=no-member

    async def test_power_distributor_one_user(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        mock_microgrid, bat_channels, inv_channels = self.create_mock_microgrid()

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

        request = Request(
            power=1200,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
        )

        with mock.patch("asyncio.sleep", new_callable=AsyncMock) and mock.patch(
            "frequenz.sdk.microgrid.get", return_value=mock_microgrid
        ):
            distributor = PowerDistributingActor({"user1": channel.service_handle})

            # Mock that all requested batteries are working.
            distributor._battery_pool = MagicMock(spec=BatteryPoolStatus)
            distributor._battery_pool.get_working_batteries.return_value = (
                request.batteries
            )

            client_handle = channel.client_handle
            await client_handle.send(request)

            done, pending = await asyncio.wait(
                [asyncio.create_task(client_handle.receive())],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeed_power == 1000
        assert result.excess_power == 200
        assert result.request == request

    async def test_power_distributor_two_users(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with two users."""
        mock_microgrid, bat_channels, inv_channels = self.create_mock_microgrid()

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

        with mock.patch("asyncio.sleep", new_callable=AsyncMock) and mock.patch(
            "frequenz.sdk.microgrid.get", return_value=mock_microgrid
        ):
            distributor = PowerDistributingActor(service_channels)

            # Mock that all requested batteries are working.
            distributor._battery_pool = MagicMock(spec=BatteryPoolStatus)
            distributor._battery_pool.get_working_batteries.return_value = {106, 206}

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
                [
                    asyncio.create_task(user1_handle.receive()),
                    asyncio.create_task(user2_handle.receive()),
                ],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 2

        assert any(map(lambda x: isinstance(x.result(), Success), done))
        assert any(map(lambda x: isinstance(x.result(), Ignored), done))

    async def test_power_distributor_invalid_battery_id(self) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution raises error if any battery id is invalid."""
        mock_microgrid, bat_channels, inv_channels = self.create_mock_microgrid()

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

        request = Request(
            power=1200, batteries={106, 208}, request_timeout_sec=SAFETY_TIMEOUT
        )
        with mock.patch("asyncio.sleep", new_callable=AsyncMock) and mock.patch(
            "frequenz.sdk.microgrid.get", return_value=mock_microgrid
        ):

            distributor = PowerDistributingActor(service_channels)

            # Mock that all requested batteries are working.
            distributor._battery_pool = MagicMock(spec=BatteryPoolStatus)
            distributor._battery_pool.get_working_batteries.return_value = (
                request.batteries
            )

            user1_handle = channel1.client_handle
            await user1_handle.send(request)

            done, _ = await asyncio.wait(
                [asyncio.create_task(user1_handle.receive())],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(done) == 1
        result: Result = done.pop().result()
        assert isinstance(result, Error)
        assert result.request == request
        err_msg = re.search(r"^No battery 208, available batteries:", result.msg)
        assert err_msg is not None

    async def test_power_distributor_overlapping_batteries(self) -> None:
        # pylint: disable=too-many-locals
        """Test if requests with overlapping set of batteries are processed."""
        mock_microgrid, bat_channels, inv_channels = self.create_mock_microgrid()

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

        with mock.patch("asyncio.sleep", new_callable=AsyncMock) and mock.patch(
            "frequenz.sdk.microgrid.get", return_value=mock_microgrid
        ):

            distributor = PowerDistributingActor(service_channels)

            # Mock that all requested batteries are working.
            distributor._battery_pool = MagicMock(spec=BatteryPoolStatus)
            distributor._battery_pool.get_working_batteries.side_effect = [
                {106, 206},
                {106, 306},
                {106, 206},
            ]

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
                    asyncio.create_task(user1_handle.receive()),
                    asyncio.create_task(user2_handle.receive()),
                    asyncio.create_task(user3_handle.receive()),
                ],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(done) == 3
        success, ignored = 0, 0
        for item in done:
            result = item.result()
            if isinstance(result, Success):
                success += 1
            elif isinstance(result, Ignored):
                ignored += 1
            else:
                assert 0, f"Unexpected type of result message {type(result)}"

        # It is an assert we can't be sure will be executed first
        assert success >= 2
        assert ignored <= 1

    async def test_power_distributor_one_user_adjust_power_consume(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        mock_microgrid, bat_channels, inv_channels = self.create_mock_microgrid()

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

        request = Request(
            power=1200,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        with mock.patch("asyncio.sleep", new_callable=AsyncMock) and mock.patch(
            "frequenz.sdk.microgrid.get", return_value=mock_microgrid
        ):

            distributor = PowerDistributingActor(service_channels)

            # Mock that all requested batteries are working.
            distributor._battery_pool = MagicMock(spec=BatteryPoolStatus)
            distributor._battery_pool.get_working_batteries.return_value = (
                request.batteries
            )

            user1_handle = channel1.client_handle
            await user1_handle.send(request)

            done, pending = await asyncio.wait(
                [asyncio.create_task(user1_handle.receive())],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, OutOfBound)
        assert result is not None
        assert result.request == request
        assert result.bound == 1000

    async def test_power_distributor_one_user_adjust_power_supply(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        mock_microgrid, bat_channels, inv_channels = self.create_mock_microgrid()

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

        request = Request(
            power=-1200,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        with mock.patch("asyncio.sleep", new_callable=AsyncMock) and mock.patch(
            "frequenz.sdk.microgrid.get", return_value=mock_microgrid
        ):

            distributor = PowerDistributingActor(service_channels)

            # Mock that all requested batteries are working.
            distributor._battery_pool = MagicMock(spec=BatteryPoolStatus)
            distributor._battery_pool.get_working_batteries.return_value = (
                request.batteries
            )

            user1_handle = channel1.client_handle
            await user1_handle.send(request)

            done, pending = await asyncio.wait(
                [asyncio.create_task(user1_handle.receive())],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, OutOfBound)
        assert result is not None
        assert result.request == request
        assert result.bound == -1000

    async def test_power_distributor_one_user_adjust_power_success(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        mock_microgrid, bat_channels, inv_channels = self.create_mock_microgrid()

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

        request = Request(
            power=1000,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        with mock.patch("asyncio.sleep", new_callable=AsyncMock) and mock.patch(
            "frequenz.sdk.microgrid.get", return_value=mock_microgrid
        ):

            distributor = PowerDistributingActor(service_channels)

            # Mock that all requested batteries are working.
            distributor._battery_pool = MagicMock(spec=BatteryPoolStatus)
            distributor._battery_pool.get_working_batteries.return_value = (
                request.batteries
            )

            user1_handle = channel1.client_handle
            await user1_handle.send(request)

            done, pending = await asyncio.wait(
                [asyncio.create_task(user1_handle.receive())],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeed_power == 1000
        assert result.excess_power == 0
        assert result.request == request

    async def test_not_all_batteries_are_working(
        self,
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works if not all batteries are working."""
        mock_microgrid, bat_channels, inv_channels = self.create_mock_microgrid()

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

        request = Request(
            power=1200, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
        )
        with mock.patch("asyncio.sleep", new_callable=AsyncMock) and mock.patch(
            "frequenz.sdk.microgrid.get", return_value=mock_microgrid
        ):

            distributor = PowerDistributingActor({"user1": channel.service_handle})

            # Mock that all requested batteries are working.
            distributor._battery_pool = MagicMock(spec=BatteryPoolStatus)
            distributor._battery_pool.get_working_batteries.return_value = (
                request.batteries - {106}
            )

            client_handle = channel.client_handle
            await client_handle.send(request)

            done, pending = await asyncio.wait(
                [asyncio.create_task(client_handle.receive())],
                timeout=SAFETY_TIMEOUT,
            )
            await distributor._stop()  # type: ignore # pylint: disable=no-member

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.excess_power == 700
        assert result.succeed_power == 500
        assert result.request == request
