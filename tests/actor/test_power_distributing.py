# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests power distributor"""

from __future__ import annotations

import asyncio
import math
import re
from typing import TypeVar
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Broadcast
from pytest import approx
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
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
from frequenz.sdk.microgrid.component import ComponentCategory
from tests.timeseries.mock_microgrid import MockMicrogrid

from ..conftest import SAFETY_TIMEOUT
from ..power.test_distribution_algorithm import Bound, Metric, battery_msg, inverter_msg

T = TypeVar("T")  # Declare type variable


class TestPowerDistributingActor:
    # pylint: disable=protected-access
    """Test tool to distribute power"""

    _namespace = "power_distributor"

    async def test_constructor(self, mocker: MockerFixture) -> None:
        """Test if gets all necessary data."""
        mockgrid = MockMicrogrid(grid_side_meter=True)
        mockgrid.add_batteries(2)
        mockgrid.add_batteries(1, no_meter=True)
        await mockgrid.start(mocker)

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        battery_status_channel = Broadcast[BatteryStatus]("battery_status")
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        assert distributor._bat_inv_map == {9: 8, 19: 18, 29: 28}
        assert distributor._inv_bat_map == {8: 9, 18: 19, 28: 29}

        await distributor._stop_actor()
        await mockgrid.cleanup()

        # Test if it works without grid side meter
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(1)
        mockgrid.add_batteries(2, no_meter=True)
        await mockgrid.start(mocker)
        distributor = PowerDistributingActor(
            requests_receiver=channel.new_receiver(),
            channel_registry=channel_registry,
            battery_status_sender=battery_status_channel.new_sender(),
        )

        assert distributor._bat_inv_map == {9: 8, 19: 18, 29: 28}
        assert distributor._inv_bat_map == {8: 9, 18: 19, 28: 29}

        await distributor._stop_actor()
        await mockgrid.cleanup()

    async def init_component_data(self, mockgrid: MockMicrogrid) -> None:
        """Send initial component data, for power distributor to start."""
        graph = microgrid.connection_manager.get().component_graph
        for battery in graph.components(component_category={ComponentCategory.BATTERY}):
            await mockgrid.mock_client.send(
                battery_msg(
                    battery.component_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=Bound(-1000, 1000),
                )
            )

        inverters = graph.components(component_category={ComponentCategory.INVERTER})
        for inverter in inverters:
            await mockgrid.mock_client.send(
                inverter_msg(
                    inverter.component_id,
                    power=Bound(-500, 500),
                )
            )

    async def test_power_distributor_one_user(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with single user works."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={9, 19},
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
        """Test if battery with SoC==NaN is not used."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        await mockgrid.mock_client.send(
            battery_msg(
                9,
                soc=Metric(math.nan, Bound(20, 80)),
                capacity=Metric(98000),
                power=Bound(-1000, 1000),
            )
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={9, 19},
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
        assert result.succeeded_batteries == {19}
        assert result.succeeded_power == approx(500.0)
        assert result.excess_power == approx(700.0)
        assert result.request == request

    async def test_battery_capacity_nan(self, mocker: MockerFixture) -> None:
        """Test battery with capacity set to NaN is not used."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        await mockgrid.mock_client.send(
            battery_msg(
                9,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(math.nan),
                power=Bound(-1000, 1000),
            )
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={9, 19},
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
        assert result.succeeded_batteries == {19}
        assert result.succeeded_power == approx(500.0)
        assert result.excess_power == approx(700.0)
        assert result.request == request

    async def test_battery_power_bounds_nan(self, mocker: MockerFixture) -> None:
        """Test battery with power bounds set to NaN is not used."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        # Battery 19 should work even if his inverter sends NaN
        await mockgrid.mock_client.send(
            inverter_msg(
                18,
                power=Bound(math.nan, math.nan),
            )
        )

        # Battery 106 should not work because both battery and inverter sends NaN
        await mockgrid.mock_client.send(
            inverter_msg(
                8,
                power=Bound(-1000, math.nan),
            )
        )

        await mockgrid.mock_client.send(
            battery_msg(
                9,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(float(98000)),
                power=Bound(math.nan, math.nan),
            )
        )

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={9, 19},
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
        assert result.succeeded_batteries == {19}
        assert result.succeeded_power == approx(1000.0)
        assert result.excess_power == approx(200.0)
        assert result.request == request

    async def test_power_distributor_invalid_battery_id(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution raises error if any battery id is invalid."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")
        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={9, 100},
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
        err_msg = re.search(r"^No battery 100, available batteries:", result.msg)
        assert err_msg is not None

    async def test_power_distributor_one_user_adjust_power_consume(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1200.0,
            batteries={9, 19},
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
        """Test if power distribution works with single user works."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=-1200.0,
            batteries={9, 19},
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
        """Test if power distribution works with single user works."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        channel = Broadcast[Request]("power_distributor")
        channel_registry = ChannelRegistry(name="power_distributor")

        request = Request(
            namespace=self._namespace,
            power=1000.0,
            batteries={9, 19},
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
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {9, 19}

        attrs = {"get_working_batteries.return_value": batteries - {9}}
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
        assert result.succeeded_batteries == {19}
        assert result.excess_power == approx(700.0)
        assert result.succeeded_power == approx(500.0)
        assert result.request == request

        await distributor._stop_actor()

    async def test_use_all_batteries_none_is_working(
        self, mocker: MockerFixture
    ) -> None:
        """Test all batteries are used if none of them works."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

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
            batteries={9, 19},
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
        assert result.succeeded_batteries == {9, 19}
        assert result.excess_power == approx(200.0)
        assert result.succeeded_power == approx(1000.0)
        assert result.request == request

        await distributor._stop_actor()

    async def test_force_request_a_battery_is_not_working(
        self, mocker: MockerFixture
    ) -> None:
        """Test force request when a battery is not working."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {9, 19}

        attrs = {"get_working_batteries.return_value": batteries - {9}}
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
        assert result.succeeded_batteries == {9, 19}
        assert result.excess_power == approx(200.0)
        assert result.succeeded_power == approx(1000.0)
        assert result.request == request

        await distributor._stop_actor()

    async def test_force_request_battery_nan_value_non_cached(
        self, mocker: MockerFixture
    ) -> None:
        """Test battery with NaN in SoC, capacity or power is used if request is forced."""
        # pylint: disable=too-many-locals
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {9, 19}

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
                9,
                soc=Metric(math.nan, Bound(20, 80)),
                capacity=Metric(math.nan),
                power=Bound(-1000, 1000),
            ),
            battery_msg(
                19,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(math.nan),
                power=Bound(-1000, 1000),
            ),
        )

        for battery in batteries_data:
            await mockgrid.mock_client.send(battery)

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
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {9, 19, 29}

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
                9,
                soc=Metric(math.nan, Bound(20, 80)),
                capacity=Metric(98000),
                power=Bound(-1000, 1000),
            ),
            battery_msg(
                19,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(math.nan),
                power=Bound(-1000, 1000),
            ),
            battery_msg(
                29,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(float(98000)),
                power=Bound(math.nan, math.nan),
            ),
        )

        # This request is needed to set the battery metrics cache to have valid
        # metrics so that the distribution algorithm can be used in the next
        # request where the batteries report NaN in the metrics.
        await channel.new_sender().send(request)
        await test_result()

        for battery in batteries_data:
            await mockgrid.mock_client.send(battery)

        await channel.new_sender().send(request)
        await test_result()

        await distributor._stop_actor()
