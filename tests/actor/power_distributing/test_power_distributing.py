# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests power distributor."""

# pylint: disable=too-many-lines

import asyncio
import math
import re
from typing import TypeVar
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Broadcast
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor.power_distributing import (
    ComponentStatus,
    PowerDistributingActor,
    Request,
)
from frequenz.sdk.actor.power_distributing._battery_pool_status import BatteryPoolStatus
from frequenz.sdk.actor.power_distributing.result import (
    Error,
    OutOfBounds,
    PartialFailure,
    PowerBounds,
    Result,
    Success,
)
from frequenz.sdk.microgrid.component import ComponentCategory
from frequenz.sdk.timeseries._quantities import Power
from tests.timeseries.mock_microgrid import MockMicrogrid
from tests.utils.graph_generator import GraphGenerator

from ...conftest import SAFETY_TIMEOUT
from .test_distribution_algorithm import Bound, Metric, battery_msg, inverter_msg

T = TypeVar("T")  # Declare type variable


class TestPowerDistributingActor:
    # pylint: disable=protected-access
    # pylint: disable=too-many-public-methods
    """Test tool to distribute power."""

    _namespace = "power_distributor"

    async def test_constructor(self, mocker: MockerFixture) -> None:
        """Test if gets all necessary data."""
        mockgrid = MockMicrogrid(grid_meter=True)
        mockgrid.add_batteries(2)
        mockgrid.add_batteries(1, no_meter=True)
        await mockgrid.start(mocker)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ) as distributor:
            assert distributor._bat_invs_map == {9: {8}, 19: {18}, 29: {28}}
            assert distributor._inv_bats_map == {8: {9}, 18: {19}, 28: {29}}
        await mockgrid.cleanup()

        # Test if it works without grid side meter
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(1)
        mockgrid.add_batteries(2, no_meter=True)
        await mockgrid.start(mocker)
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ) as distributor:
            assert distributor._bat_invs_map == {9: {8}, 19: {18}, 29: {28}}
            assert distributor._inv_bats_map == {8: {9}, 18: {19}, 28: {29}}
        await mockgrid.cleanup()

    async def init_component_data(
        self,
        mockgrid: MockMicrogrid,
    ) -> None:
        """Send initial component data, for power distributor to start."""
        graph = microgrid.connection_manager.get().component_graph
        for battery in graph.components(component_category={ComponentCategory.BATTERY}):
            await mockgrid.mock_client.send(
                battery_msg(
                    battery.component_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=PowerBounds(-1000, 0, 0, 1000),
                )
            )

        inverters = graph.components(component_category={ComponentCategory.INVERTER})
        for inverter in inverters:
            await mockgrid.mock_client.send(
                inverter_msg(
                    inverter.component_id,
                    power=PowerBounds(-500, 0, 0, 500),
                )
            )

    async def test_power_distributor_one_user(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with a single user."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_kilowatts(1.2),
            batteries={9, 19},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_power.isclose(Power.from_kilowatts(1.0))
        assert result.excess_power.isclose(Power.from_watts(200.0))
        assert result.request == request

        await mockgrid.cleanup()

    async def test_power_distributor_exclusion_bounds(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distributing actor rejects non-zero requests in exclusion bounds."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(2)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        await mockgrid.mock_client.send(
            battery_msg(
                9,
                soc=Metric(60, Bound(20, 80)),
                capacity=Metric(98000),
                power=PowerBounds(-1000, -300, 300, 1000),
            )
        )
        await mockgrid.mock_client.send(
            battery_msg(
                19,
                soc=Metric(60, Bound(20, 80)),
                capacity=Metric(98000),
                power=PowerBounds(-1000, -300, 300, 1000),
            )
        )

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        attrs = {
            "get_working_batteries.return_value": microgrid.battery_pool().battery_ids
        }
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            # zero power requests should pass through despite the exclusion bounds.
            request = Request(
                power=Power.zero(),
                batteries={9, 19},
                request_timeout=SAFETY_TIMEOUT,
            )

            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result: Result = done.pop().result()
            assert isinstance(result, Success)
            assert result.succeeded_power.isclose(Power.zero(), abs_tol=1e-9)
            assert result.excess_power.isclose(Power.zero(), abs_tol=1e-9)
            assert result.request == request

            # non-zero power requests that fall within the exclusion bounds should be
            # rejected.
            request = Request(
                power=Power.from_watts(300.0),
                batteries={9, 19},
                request_timeout=SAFETY_TIMEOUT,
            )

            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result = done.pop().result()
            assert isinstance(result, OutOfBounds)
            assert result.bounds == PowerBounds(-1000, -600, 600, 1000)
            assert result.request == request

        await mockgrid.cleanup()

    # pylint: disable=too-many-locals
    async def test_two_batteries_one_inverters(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with two batteries connected to one inverter."""
        gen = GraphGenerator()
        bat_component1 = gen.component(ComponentCategory.BATTERY)
        bat_component2 = gen.component(ComponentCategory.BATTERY)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (
                        ComponentCategory.METER,
                        (
                            ComponentCategory.INVERTER,
                            [
                                bat_component1,
                                bat_component2,
                            ],
                        ),
                    ),
                ],
            )
        )

        mockgrid = MockMicrogrid(graph=graph)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")
        request = Request(
            power=Power.from_watts(1200.0),
            batteries={bat_component1.component_id, bat_component2.component_id},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}

        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")

        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            battery_status_sender=battery_status_channel.new_sender(),
            results_sender=results_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result: Result = done.pop().result()
            assert isinstance(result, Success)
            # Inverter bounded at 500
            assert result.succeeded_power.isclose(Power.from_watts(500.0))
            assert result.excess_power.isclose(Power.from_watts(700.0))
            assert result.request == request

        await mockgrid.cleanup()

    async def test_two_batteries_one_broken_one_inverters(
        self, mocker: MockerFixture
    ) -> None:
        """Test power distribution with two batteries (one broken) connected to one inverter."""
        gen = GraphGenerator()
        bat_components = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (
                        ComponentCategory.METER,
                        (
                            ComponentCategory.INVERTER,
                            [
                                bat_components[0],
                                bat_components[1],
                            ],
                        ),
                    )
                ],
            )
        )

        mockgrid = MockMicrogrid(graph=graph)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        await mockgrid.mock_client.send(
            battery_msg(
                bat_components[0].component_id,
                soc=Metric(math.nan, Bound(20, 80)),
                capacity=Metric(98000),
                power=PowerBounds(-1000, 0, 0, 1000),
            )
        )

        requests_channel = Broadcast[Request]("power_distributor")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_watts(1200.0),
            batteries=set(battery.component_id for battery in bat_components),
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}

        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")

        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            battery_status_sender=battery_status_channel.new_sender(),
            results_sender=results_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result: Result = done.pop().result()

            assert isinstance(result, Error)
            assert result.request == request
            assert (
                result.msg == "No data for at least one of the given batteries {9, 19}"
            )

        await mockgrid.cleanup()

    async def test_battery_two_inverters(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with two inverters for one battery."""
        gen = GraphGenerator()
        bat_component = gen.component(ComponentCategory.BATTERY)
        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (
                        ComponentCategory.METER,
                        [
                            (
                                ComponentCategory.INVERTER,
                                bat_component,
                            ),
                            (
                                ComponentCategory.INVERTER,
                                bat_component,
                            ),
                        ],
                    ),
                ],
            )
        )

        mockgrid = MockMicrogrid(graph=graph)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_watts(1200.0),
            batteries={bat_component.component_id},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}

        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")

        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            battery_status_sender=battery_status_channel.new_sender(),
            results_sender=results_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result: Result = done.pop().result()
            assert isinstance(result, Success)
            # Inverters each bounded at 500, together 1000
            assert result.succeeded_power.isclose(Power.from_watts(1000.0))
            assert result.excess_power.isclose(Power.from_watts(200.0))
            assert result.request == request

        await mockgrid.cleanup()

    async def test_two_batteries_three_inverters(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with two batteries connected to three inverters."""
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (
                        ComponentCategory.METER,
                        [
                            (
                                ComponentCategory.INVERTER,
                                [*batteries],
                            ),
                            (
                                ComponentCategory.INVERTER,
                                [*batteries],
                            ),
                            (
                                ComponentCategory.INVERTER,
                                [*batteries],
                            ),
                        ],
                    ),
                ],
            )
        )

        mockgrid = MockMicrogrid(graph=graph)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_watts(1700.0),
            batteries={batteries[0].component_id, batteries[1].component_id},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}

        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")

        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            battery_status_sender=battery_status_channel.new_sender(),
            results_sender=results_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result: Result = done.pop().result()
            assert isinstance(result, Success)
            # each inverter is bounded at 500 and we have 3 inverters
            assert result.succeeded_power.isclose(Power.from_watts(1500.0))
            assert result.excess_power.isclose(Power.from_watts(200.0))
            assert result.request == request

        await mockgrid.cleanup()

    async def test_two_batteries_one_inverter_different_exclusion_bounds_2(
        self, mocker: MockerFixture
    ) -> None:
        """Test power distribution with two batteries having different exclusion bounds.

        Test if power distribution works with two batteries connected to one
        inverter, each having different exclusion bounds.
        """
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)
        inverter = gen.component(ComponentCategory.INVERTER)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                (ComponentCategory.METER, [(inverter, [*batteries])]),
            )
        )

        mockgrid = MockMicrogrid(graph=graph)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)
        await mockgrid.mock_client.send(
            inverter_msg(
                inverter.component_id,
                power=PowerBounds(-1000, -500, 500, 1000),
            )
        )
        await mockgrid.mock_client.send(
            battery_msg(
                batteries[0].component_id,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(10_000),
                power=PowerBounds(-1000, -200, 200, 1000),
            )
        )
        await mockgrid.mock_client.send(
            battery_msg(
                batteries[1].component_id,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(10_000),
                power=PowerBounds(-1000, -100, 100, 1000),
            )
        )

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_watts(300.0),
            batteries={batteries[0].component_id, batteries[1].component_id},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}

        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")

        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            battery_status_sender=battery_status_channel.new_sender(),
            results_sender=results_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result: Result = done.pop().result()
            assert isinstance(result, OutOfBounds)
            assert result.request == request
            assert result.bounds == PowerBounds(-1000, -500, 500, 1000)

        await mockgrid.cleanup()

    async def test_two_batteries_one_inverter_different_exclusion_bounds(
        self, mocker: MockerFixture
    ) -> None:
        """Test power distribution with two batteries having different exclusion bounds.

        Test if power distribution works with two batteries connected to one
        inverter, each having different exclusion bounds.
        """
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                (
                    ComponentCategory.METER,
                    [
                        (
                            ComponentCategory.INVERTER,
                            [*batteries],
                        ),
                    ],
                ),
            )
        )

        mockgrid = MockMicrogrid(graph=graph)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)
        await mockgrid.mock_client.send(
            battery_msg(
                batteries[0].component_id,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(10_000),
                power=PowerBounds(-1000, -200, 200, 1000),
            )
        )
        await mockgrid.mock_client.send(
            battery_msg(
                batteries[1].component_id,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(10_000),
                power=PowerBounds(-1000, -100, 100, 1000),
            )
        )

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_watts(300.0),
            batteries={batteries[0].component_id, batteries[1].component_id},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}

        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")

        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            battery_status_sender=battery_status_channel.new_sender(),
            results_sender=results_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result: Result = done.pop().result()
            assert isinstance(result, OutOfBounds)
            assert result.request == request
            # each inverter is bounded at 500
            assert result.bounds == PowerBounds(-500, -400, 400, 500)

        await mockgrid.cleanup()

    async def test_connected_but_not_requested_batteries(
        self, mocker: MockerFixture
    ) -> None:
        """Test behavior when an inverter has more batteries connected than requested."""
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (
                        ComponentCategory.METER,
                        [
                            (
                                ComponentCategory.INVERTER,
                                [*batteries],
                            ),
                        ],
                    ),
                ],
            )
        )

        mockgrid = MockMicrogrid(graph=graph)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_watts(600.0),
            batteries={batteries[0].component_id},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}

        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")

        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            battery_status_sender=battery_status_channel.new_sender(),
            results_sender=results_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1

            result: Result = done.pop().result()
            assert isinstance(result, Error)
            assert result.request == request

            err_msg = re.search(
                r"'Inverters \{48\} are connected to batteries that were not requested: \{19\}'",
                result.msg,
            )
            assert err_msg is not None

        await mockgrid.cleanup()

    async def test_battery_soc_nan(self, mocker: MockerFixture) -> None:
        """Test if battery with SoC==NaN is not used."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        await mockgrid.mock_client.send(
            battery_msg(
                9,
                soc=Metric(math.nan, Bound(20, 80)),
                capacity=Metric(98000),
                power=PowerBounds(-1000, 0, 0, 1000),
            )
        )

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_kilowatts(1.2),
            batteries={9, 19},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            attrs = {"get_working_batteries.return_value": request.batteries}
            mocker.patch(
                "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
                return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
            )

            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {19}
        assert result.succeeded_power.isclose(Power.from_watts(500.0))
        assert result.excess_power.isclose(Power.from_watts(700.0))
        assert result.request == request

        await mockgrid.cleanup()

    async def test_battery_capacity_nan(self, mocker: MockerFixture) -> None:
        """Test battery with capacity set to NaN is not used."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        await mockgrid.mock_client.send(
            battery_msg(
                9,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(math.nan),
                power=PowerBounds(-1000, 0, 0, 1000),
            )
        )

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_kilowatts(1.2),
            batteries={9, 19},
            request_timeout=SAFETY_TIMEOUT,
        )
        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {19}
        assert result.succeeded_power.isclose(Power.from_watts(500.0))
        assert result.excess_power.isclose(Power.from_watts(700.0))
        assert result.request == request

        await mockgrid.cleanup()

    async def test_battery_power_bounds_nan(self, mocker: MockerFixture) -> None:
        """Test battery with power bounds set to NaN is not used."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        await mockgrid.mock_client.send(
            inverter_msg(
                18,
                power=PowerBounds(-1000, 0, 0, 1000),
            )
        )

        # Battery 9 should not work because both battery and inverter sends NaN
        await mockgrid.mock_client.send(
            inverter_msg(
                8,
                power=PowerBounds(-1000, 0, 0, math.nan),
            )
        )

        await mockgrid.mock_client.send(
            battery_msg(
                9,
                soc=Metric(40, Bound(20, 80)),
                capacity=Metric(float(98000)),
                power=PowerBounds(math.nan, 0, 0, math.nan),
            )
        )

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_kilowatts(1.2),
            batteries={9, 19},
            request_timeout=SAFETY_TIMEOUT,
        )
        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

        assert len(pending) == 0
        assert len(done) == 1

        result: Result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_batteries == {19}
        assert result.succeeded_power.isclose(Power.from_kilowatts(1.0))
        assert result.excess_power.isclose(Power.from_watts(200.0))
        assert result.request == request

        await mockgrid.cleanup()

    async def test_power_distributor_invalid_battery_id(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution raises error if any battery id is invalid."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")
        request = Request(
            power=Power.from_kilowatts(1.2),
            batteries={9, 100},
            request_timeout=SAFETY_TIMEOUT,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )
        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, _ = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

        assert len(done) == 1
        result: Result = done.pop().result()
        assert isinstance(result, Error)
        assert result.request == request
        err_msg = re.search(r"No battery 100, available batteries:", result.msg)
        assert err_msg is not None

        await mockgrid.cleanup()

    async def test_power_distributor_one_user_adjust_power_consume(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_kilowatts(1.2),
            batteries={9, 19},
            request_timeout=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, OutOfBounds)
        assert result is not None
        assert result.request == request
        assert result.bounds.inclusion_upper == 1000

        await mockgrid.cleanup()

    async def test_power_distributor_one_user_adjust_power_supply(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=-Power.from_kilowatts(1.2),
            batteries={9, 19},
            request_timeout=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, OutOfBounds)
        assert result is not None
        assert result.request == request
        assert result.bounds.inclusion_lower == -1000

        await mockgrid.cleanup()

    async def test_power_distributor_one_user_adjust_power_success(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_kilowatts(1.0),
            batteries={9, 19},
            request_timeout=SAFETY_TIMEOUT,
            adjust_power=False,
        )

        attrs = {"get_working_batteries.return_value": request.batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(spec=BatteryPoolStatus, **attrs),
        )

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

        assert len(pending) == 0
        assert len(done) == 1

        result = done.pop().result()
        assert isinstance(result, Success)
        assert result.succeeded_power.isclose(Power.from_kilowatts(1.0))
        assert result.excess_power.isclose(Power.zero(), abs_tol=1e-9)
        assert result.request == request

        await mockgrid.cleanup()

    async def test_not_all_batteries_are_working(self, mocker: MockerFixture) -> None:
        """Test if power distribution works if not all batteries are working."""
        mockgrid = MockMicrogrid(grid_meter=False)
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

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            request = Request(
                power=Power.from_kilowatts(1.2),
                batteries=batteries,
                request_timeout=SAFETY_TIMEOUT,
            )

            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )

            assert len(pending) == 0
            assert len(done) == 1
            result = done.pop().result()
            assert isinstance(result, Success)
            assert result.succeeded_batteries == {19}
            assert result.excess_power.isclose(Power.from_watts(700.0))
            assert result.succeeded_power.isclose(Power.from_watts(500.0))
            assert result.request == request

        await mockgrid.cleanup()

    async def test_partial_failure_result(self, mocker: MockerFixture) -> None:
        """Test power results when the microgrid failed to set power for one of the batteries."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        await self.init_component_data(mockgrid)

        mocker.patch("asyncio.sleep", new_callable=AsyncMock)

        batteries = {9, 19, 29}
        failed_batteries = {9}
        failed_power = 500.0

        attrs = {"get_working_batteries.return_value": batteries}
        mocker.patch(
            "frequenz.sdk.actor.power_distributing.power_distributing.BatteryPoolStatus",
            return_value=MagicMock(
                spec=BatteryPoolStatus,
                **attrs,
            ),
        )

        mocker.patch(
            "frequenz.sdk.actor.power_distributing.PowerDistributingActor._parse_result",
            return_value=(failed_power, failed_batteries),
        )

        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        battery_status_channel = Broadcast[ComponentStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            battery_status_sender=battery_status_channel.new_sender(),
        ):
            request = Request(
                power=Power.from_kilowatts(1.70),
                batteries=batteries,
                request_timeout=SAFETY_TIMEOUT,
            )

            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            done, pending = await asyncio.wait(
                [asyncio.create_task(result_rx.receive())],
                timeout=SAFETY_TIMEOUT.total_seconds(),
            )
            assert len(pending) == 0
            assert len(done) == 1
            result = done.pop().result()
            assert isinstance(result, PartialFailure)
            assert result.succeeded_batteries == batteries - failed_batteries
            assert result.failed_batteries == failed_batteries
            assert result.succeeded_power.isclose(Power.from_watts(1000.0))
            assert result.failed_power.isclose(Power.from_watts(failed_power))
            assert result.excess_power.isclose(Power.from_watts(200.0))
            assert result.request == request

        await mockgrid.cleanup()
