# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests power distributor"""
import asyncio
import re
from dataclasses import dataclass
from typing import Set, Tuple, TypeVar
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Bidirectional, Receiver, Sender
from pytest_mock import MockerFixture

from frequenz.sdk.actor.power_distributing import PowerDistributingActor, Request
from frequenz.sdk.actor.power_distributing._battery_pool_status import BatteryPoolStatus
from frequenz.sdk.actor.power_distributing.result import (
    Error,
    Ignored,
    OutOfBound,
    Result,
    Success,
)
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory

from ..conftest import SAFETY_TIMEOUT
from ..power.test_distribution_algorithm import Bound, Metric, battery_msg, inverter_msg
from ..utils.mock_microgrid import MockMicrogridClient

T = TypeVar("T")  # Declare type variable


@dataclass
class User:
    """User definition."""

    user_id: str
    sender: Sender[Request]
    receiver: Receiver[Result]


class TestPowerDistributingActor:
    # pylint: disable=protected-access
    """Test tool to distribute power"""

    def component_graph(self) -> Tuple[Set[Component], Set[Connection]]:
        """Create graph components

        Returns:
            Tuple where first element is set of components and second element is
                set of connections.
        """
        components = {
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

    async def test_constructor(self, mocker: MockerFixture) -> None:
        """Test if gets all necessary data."""
        components, connections = self.component_graph()
        mock_microgrid = MockMicrogridClient(components, connections)
        await mock_microgrid.initialize(mocker)

        channel = Bidirectional[Request, Result]("user1", "power_distributor")
        distributor = PowerDistributingActor({"user1": channel.service_handle})

        assert distributor._bat_inv_map == {106: 105, 206: 205, 306: 305}
        assert distributor._inv_bat_map == {105: 106, 205: 206, 305: 306}
        await distributor._stop_actor()

    async def init_mock_microgrid(self, mocker: MockerFixture) -> MockMicrogridClient:
        """Create mock microgrid and send initial data from the components.

        Returns:
            Mock microgrid instance.
        """
        components, connections = self.component_graph()
        microgrid = MockMicrogridClient(components, connections)
        await microgrid.initialize(mocker)

        graph = microgrid.component_graph
        for battery in graph.components(component_category={ComponentCategory.BATTERY}):
            assert await microgrid.send(
                battery_msg(
                    battery.component_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=Bound(-1000, 1000),
                )
            )

        inverters = graph.components(component_category={ComponentCategory.INVERTER})
        for inverter in inverters:
            assert await microgrid.send(
                inverter_msg(
                    inverter.component_id,
                    power=Bound(-500, 500),
                )
            )

        return microgrid

    async def test_power_distributor_one_user(self, mocker: MockerFixture) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        await self.init_mock_microgrid(mocker)

        channel = Bidirectional[Request, Result]("user1", "power_distributor")

        request = Request(
            power=1200,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        battery_pool_mock = MagicMock(spec=BatteryPoolStatus, **attrs)
        BatteryPoolStatus.async_new = AsyncMock(  # type: ignore
            return_value=battery_pool_mock
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        distributor = PowerDistributingActor({"user1": channel.service_handle})

        client_handle = channel.client_handle
        await client_handle.send(request)

        done, pending = await asyncio.wait(
            [asyncio.create_task(client_handle.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeed_power == 1000
        assert result.excess_power == 200
        assert result.request == request

    async def test_power_distributor_two_users(self, mocker: MockerFixture) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with two users."""
        await self.init_mock_microgrid(mocker)

        channel1 = Bidirectional[Request, Result]("user1", "power_distributor")
        channel2 = Bidirectional[Request, Result]("user2", "power_distributor")
        service_channels = {
            "user1": channel1.service_handle,
            "user2": channel2.service_handle,
        }

        attrs = {"get_working_batteries.return_value": {106, 206}}
        battery_pool_mock = MagicMock(spec=BatteryPoolStatus, **attrs)
        BatteryPoolStatus.async_new = AsyncMock(  # type: ignore
            return_value=battery_pool_mock
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        distributor = PowerDistributingActor(service_channels)

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
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 2

        assert any(map(lambda x: isinstance(x.result(), Success), done))
        assert any(map(lambda x: isinstance(x.result(), Ignored), done))

    async def test_power_distributor_invalid_battery_id(
        self, mocker: MockerFixture
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution raises error if any battery id is invalid."""
        await self.init_mock_microgrid(mocker)

        channel1 = Bidirectional[Request, Result]("user1", "power_distributor")
        service_channels = {
            "user1": channel1.service_handle,
        }

        request = Request(
            power=1200, batteries={106, 208}, request_timeout_sec=SAFETY_TIMEOUT
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        battery_pool_mock = MagicMock(spec=BatteryPoolStatus, **attrs)
        BatteryPoolStatus.async_new = AsyncMock(  # type: ignore
            return_value=battery_pool_mock
        )
        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        distributor = PowerDistributingActor(service_channels)

        user1_handle = channel1.client_handle
        await user1_handle.send(request)

        done, _ = await asyncio.wait(
            [asyncio.create_task(user1_handle.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(done) == 1
        result: Result = done.pop().result()
        assert isinstance(result, Error)
        assert result.request == request
        err_msg = re.search(r"^No battery 208, available batteries:", result.msg)
        assert err_msg is not None

    async def test_power_distributor_overlapping_batteries(
        self, mocker: MockerFixture
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if requests with overlapping set of batteries are processed."""
        await self.init_mock_microgrid(mocker)

        channel1 = Bidirectional[Request, Result]("user1", "power_distributor")
        channel2 = Bidirectional[Request, Result]("user2", "power_distributor")
        channel3 = Bidirectional[Request, Result]("user3", "power_distributor")
        service_channels = {
            "user1": channel1.service_handle,
            "user2": channel2.service_handle,
            "user3": channel3.service_handle,
        }

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        attrs = {
            "get_working_batteries.side_effect": [{106, 206}, {106, 306}, {106, 206}]
        }
        battery_pool_mock = MagicMock(spec=BatteryPoolStatus, **attrs)
        BatteryPoolStatus.async_new = AsyncMock(  # type: ignore
            return_value=battery_pool_mock
        )

        distributor = PowerDistributingActor(service_channels)

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
        await distributor._stop_actor()

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
        self, mocker: MockerFixture
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        await self.init_mock_microgrid(mocker)

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

        attrs = {"get_working_batteries.return_value": request.batteries}
        battery_pool_mock = MagicMock(spec=BatteryPoolStatus, **attrs)
        BatteryPoolStatus.async_new = AsyncMock(  # type: ignore
            return_value=battery_pool_mock
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        distributor = PowerDistributingActor(service_channels)

        user1_handle = channel1.client_handle
        await user1_handle.send(request)

        done, pending = await asyncio.wait(
            [asyncio.create_task(user1_handle.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, OutOfBound)
        assert result is not None
        assert result.request == request
        assert result.bound == 1000

    async def test_power_distributor_one_user_adjust_power_supply(
        self, mocker: MockerFixture
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        await self.init_mock_microgrid(mocker)

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

        attrs = {"get_working_batteries.return_value": request.batteries}
        battery_pool_mock = MagicMock(spec=BatteryPoolStatus, **attrs)
        BatteryPoolStatus.async_new = AsyncMock(  # type: ignore
            return_value=battery_pool_mock
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        distributor = PowerDistributingActor(service_channels)

        user1_handle = channel1.client_handle
        await user1_handle.send(request)

        done, pending = await asyncio.wait(
            [asyncio.create_task(user1_handle.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, OutOfBound)
        assert result is not None
        assert result.request == request
        assert result.bound == -1000

    async def test_power_distributor_one_user_adjust_power_success(
        self, mocker: MockerFixture
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        await self.init_mock_microgrid(mocker)

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

        attrs = {"get_working_batteries.return_value": request.batteries}
        battery_pool_mock = MagicMock(spec=BatteryPoolStatus, **attrs)
        BatteryPoolStatus.async_new = AsyncMock(  # type: ignore
            return_value=battery_pool_mock
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        distributor = PowerDistributingActor(service_channels)

        user1_handle = channel1.client_handle
        await user1_handle.send(request)

        done, pending = await asyncio.wait(
            [asyncio.create_task(user1_handle.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeed_power == 1000
        assert result.excess_power == 0
        assert result.request == request

    async def test_not_all_batteries_are_working(self, mocker: MockerFixture) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works if not all batteries are working."""
        await self.init_mock_microgrid(mocker)

        channel = Bidirectional[Request, Result]("user1", "power_distributor")

        request = Request(
            power=1200, batteries={106, 206}, request_timeout_sec=SAFETY_TIMEOUT
        )

        attrs = {"get_working_batteries.return_value": request.batteries - {106}}
        battery_pool_mock = MagicMock(spec=BatteryPoolStatus, **attrs)
        BatteryPoolStatus.async_new = AsyncMock(  # type: ignore
            return_value=battery_pool_mock
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        distributor = PowerDistributingActor({"user1": channel.service_handle})

        client_handle = channel.client_handle
        await client_handle.send(request)

        done, pending = await asyncio.wait(
            [asyncio.create_task(client_handle.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.excess_power == 700
        assert result.succeed_power == 500
        assert result.request == request
