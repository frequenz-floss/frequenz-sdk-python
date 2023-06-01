# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests power distributor"""

from __future__ import annotations

import asyncio
import re
from typing import TypeVar
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Broadcast
from pytest import approx
from pytest_mock import MockerFixture

from frequenz.sdk.actor import ChannelRegistry
from frequenz.sdk.actor.power_distributing import (
    BatteryStatus,
    PowerDistributingActor,
    Request,
)
from frequenz.sdk.actor.power_distributing._battery_pool_status import BatteryPoolStatus
from frequenz.sdk.actor.power_distributing.result import (
    Error,
    OutOfBound,
    Result,
    Success,
)
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory

from ..conftest import SAFETY_TIMEOUT
from ..power.test_distribution_algorithm import Bound, Metric, battery_msg, inverter_msg
from ..utils.mock_microgrid_client import MockMicrogridClient

T = TypeVar("T")  # Declare type variable


class TestPowerDistributingActor:
    # pylint: disable=protected-access
    """Test tool to distribute power"""

    _namespace = "power_distributor"

    def component_graph(self) -> tuple[set[Component], set[Connection]]:
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
        mock_microgrid.initialize(mocker)

        attrs = {"get_working_batteries.return_value": {306}}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

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
        microgrid.initialize(mocker)

        graph = microgrid.component_graph
        for battery in graph.components(component_category={ComponentCategory.BATTERY}):
            await microgrid.send(
                battery_msg(
                    battery.component_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=Bound(-1000, 1000),
                )
            )

        inverters = graph.components(component_category={ComponentCategory.INVERTER})
        for inverter in inverters:
            await microgrid.send(
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

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_power == approx(1000.0)
        assert result.excess_power == approx(200.0)
        assert result.request == request

    async def test_battery_soc_nan(self, mocker: MockerFixture) -> None:
        # pylint: disable=too-many-locals
        """Test if battery with SoC==NaN is not used."""
        mock_microgrid = await self.init_mock_microgrid(mocker)

        await mock_microgrid.send(
            battery_msg(
                106,
                soc=Metric(float("NaN"), Bound(20, 80)),
                capacity=Metric(98000),
                power=Bound(-1000, 1000),
            )
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {206}
        assert result.succeeded_power == approx(500.0)
        assert result.excess_power == approx(700.0)
        assert result.request == request

    async def test_battery_capacity_nan(self, mocker: MockerFixture) -> None:
        # pylint: disable=too-many-locals
        """Test battery with capacity set to NaN is not used."""
        mock_microgrid = await self.init_mock_microgrid(mocker)

        await mock_microgrid.send(
            battery_msg(
                106,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(float("NaN")),
                power=Bound(-1000, 1000),
            )
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
        )
        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {206}
        assert result.succeeded_power == approx(500.0)
        assert result.excess_power == approx(700.0)
        assert result.request == request

    async def test_battery_power_bounds_nan(self, mocker: MockerFixture) -> None:
        # pylint: disable=too-many-locals
        """Test battery with power bounds set to NaN is not used."""
        mock_microgrid = await self.init_mock_microgrid(mocker)

        # Battery 206 should work even if his inverter sends NaN
        await mock_microgrid.send(
            inverter_msg(
                205,
                power=Bound(float("NaN"), float("NaN")),
            )
        )

        # Battery 106 should not work because both battery and inverter sends NaN
        await mock_microgrid.send(
            inverter_msg(
                105,
                power=Bound(-1000, float("NaN")),
            )
        )

        await mock_microgrid.send(
            battery_msg(
                106,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(float(98000)),
                power=Bound(float("NaN"), float("NaN")),
            )
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
        )
        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {206}
        assert result.succeeded_power == approx(1000.0)
        assert result.excess_power == approx(200.0)
        assert result.request == request

    async def test_power_distributor_invalid_battery_id(
        self, mocker: MockerFixture
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution raises error if any battery id is invalid."""
        await self.init_mock_microgrid(mocker)

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={106, 208},
            request_timeout_sec=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )
        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, _ = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(done) == 1
        result: Result = done.pop().result()
        assert isinstance(result, Error)
        assert result.request == request
        err_msg = re.search(r"^No battery 208, available batteries:", result.msg)
        assert err_msg is not None

    async def test_power_distributor_one_user_adjust_power_consume(
        self, mocker: MockerFixture
    ) -> None:
        # pylint: disable=too-many-locals
        """Test if power distribution works with single user works."""
        await self.init_mock_microgrid(mocker)

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
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

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=-1200.0,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
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

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1000.0,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )
        await distributor._stop_actor()

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_power == approx(1000.0)
        assert result.excess_power == approx(0.0)
        assert result.request == request

    async def test_not_all_batteries_are_working(self, mocker: MockerFixture) -> None:
        """Test if power distribution works if not all batteries are working."""
        await self.init_mock_microgrid(mocker)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {106, 206}

        attrs = {"get_working_batteries.return_value": batteries - {106}}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries=batteries,
            request_timeout_sec=SAFETY_TIMEOUT,
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )

        assert len(pending) == 0
        assert len(done) == 1
        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {206}
        assert result.excess_power == approx(700.0)
        assert result.succeeded_power == approx(500.0)
        assert result.request == request

        await distributor._stop_actor()

    async def test_use_all_batteries_none_is_working(
        self, mocker: MockerFixture
    ) -> None:
        """Test all batteries are used if none of them works."""
        await self.init_mock_microgrid(mocker)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        attrs: dict[str, set[int]] = {"get_working_batteries.return_value": set()}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={106, 206},
            request_timeout_sec=SAFETY_TIMEOUT,
            include_broken_batteries=True,
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )

        assert len(pending) == 0
        assert len(done) == 1
        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {106, 206}
        assert result.excess_power == approx(200.0)
        assert result.succeeded_power == approx(1000.0)
        assert result.request == request

        await distributor._stop_actor()

    async def test_force_request_a_battery_is_not_working(
        self, mocker: MockerFixture
    ) -> None:
        """Test force request when a battery is not working."""
        await self.init_mock_microgrid(mocker)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {106, 206}

        attrs = {"get_working_batteries.return_value": batteries - {106}}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries=batteries,
            request_timeout_sec=SAFETY_TIMEOUT,
            include_broken_batteries=True,
        )

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )

        assert len(pending) == 0
        assert len(done) == 1
        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {106, 206}
        assert result.excess_power == approx(200.0)
        assert result.succeeded_power == approx(1000.0)
        assert result.request == request

        await distributor._stop_actor()

    # pylint: disable=too-many-locals
    async def test_force_request_battery_nan_value_non_cached(
        self, mocker: MockerFixture
    ) -> None:
        """Test battery with NaN in SoC, capacity or power is used if request is forced."""
        mock_microgrid = await self.init_mock_microgrid(mocker)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {106, 206}

        attrs = {"get_working_batteries.return_value": batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries=batteries,
            request_timeout_sec=SAFETY_TIMEOUT,
            include_broken_batteries=True,
        )

        batteries_data = (
            battery_msg(
                106,
                soc=Metric(float("NaN"), Bound(20, 80)),
                capacity=Metric(float("NaN")),
                power=Bound(-1000, 1000),
            ),
            battery_msg(
                206,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(float("NaN")),
                power=Bound(-1000, 1000),
            ),
        )

        for battery in batteries_data:
            await mock_microgrid.send(battery)

        await channel.new_sender().send(request)
        result_rx = channel_registry.new_receiver(self._namespace)

        done, pending = await asyncio.wait(
            [asyncio.create_task(result_rx.receive())],
            timeout=SAFETY_TIMEOUT,
        )

        assert len(pending) == 0
        assert len(done) == 1
        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == batteries
        assert result.succeeded_power == approx(1199.9999)
        assert result.excess_power == approx(0.0)
        assert result.request == request

        await distributor._stop_actor()

    async def test_force_request_batteries_nan_values_cached(
        self, mocker: MockerFixture
    ) -> None:
        """Test battery with NaN in SoC, capacity or power is used if request is forced."""
        mock_microgrid = await self.init_mock_microgrid(mocker)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {106, 206, 306}

        attrs = {"get_working_batteries.return_value": batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries=batteries,
            request_timeout_sec=SAFETY_TIMEOUT,
            include_broken_batteries=True,
        )

        result_rx = channel_registry.new_receiver(self._namespace)

        async def test_result() -> None:
            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT,
            )
            assert len(pending) == 0
            assert len(done) == 1
            result: Result = done.pop().result()
            assert isinstance(result, Success)
            assert result.succeeded_batteries == batteries
            assert result.succeeded_power == approx(1199.9999)
            assert result.excess_power == approx(0.0)
            assert result.request == request

        batteries_data = (
            battery_msg(
                106,
                soc=Metric(float("NaN"), Bound(20, 80)),
                capacity=Metric(98000),
                power=Bound(-1000, 1000),
            ),
            battery_msg(
                206,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(float("NaN")),
                power=Bound(-1000, 1000),
            ),
            battery_msg(
                306,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(float(98000)),
                power=Bound(float("NaN"), float("NaN")),
            ),
        )

        # This request is needed to set the battery metrics cache to have valid
        # metrics so that the distribution algorithm can be used in the next
        # request where the batteries report NaN in the metrics.
        await channel.new_sender().send(request)
        await test_result()

        for battery in batteries_data:
            await mock_microgrid.send(battery)

        await channel.new_sender().send(request)
        await test_result()

        await distributor._stop_actor()
