# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests power distributor."""

from __future__ import annotations

import asyncio
import dataclasses
import math
import re
from collections import abc
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncIterator, TypeVar
from unittest.mock import MagicMock

from frequenz.channels import Broadcast, Sender
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig
from frequenz.sdk.actor.power_distributing import (
    ComponentPoolStatus,
    PowerDistributingActor,
    Request,
)
from frequenz.sdk.actor.power_distributing._component_managers._battery_manager import (
    BatteryManager,
)
from frequenz.sdk.actor.power_distributing._component_pool_status_tracker import (
    ComponentPoolStatusTracker,
)
from frequenz.sdk.actor.power_distributing.result import (
    Error,
    OutOfBounds,
    PartialFailure,
    PowerBounds,
    Result,
    Success,
)
from frequenz.sdk.microgrid.component import ComponentCategory
from frequenz.sdk.microgrid.component_graph import _MicrogridComponentGraph
from frequenz.sdk.timeseries import Power

from ...conftest import SAFETY_TIMEOUT
from ...timeseries.mock_microgrid import MockMicrogrid
from ...utils.component_data_streamer import MockComponentDataStreamer
from ...utils.graph_generator import GraphGenerator
from .test_battery_distribution_algorithm import (
    Bound,
    Metric,
    battery_msg,
    inverter_msg,
)

# pylint: disable=too-many-lines


T = TypeVar("T")  # Declare type variable


@dataclasses.dataclass(frozen=True)
class _Mocks:
    """Mocks for the tests."""

    microgrid: MockMicrogrid
    """A mock microgrid instance."""

    streamer: MockComponentDataStreamer
    """A mock component data streamer."""

    battery_status_sender: Sender[ComponentPoolStatus]
    """Sender for sending status of the batteries."""

    @classmethod
    async def new(
        cls,
        mocker: MockerFixture,
        graph: _MicrogridComponentGraph | None = None,
        grid_meter: bool | None = None,
    ) -> _Mocks:
        """Initialize the mocks."""
        mockgrid = MockMicrogrid(graph=graph, grid_meter=grid_meter, mocker=mocker)
        if not graph:
            mockgrid.add_batteries(3)
        await mockgrid.start()

        # pylint: disable=protected-access
        if microgrid._data_pipeline._DATA_PIPELINE is not None:
            microgrid._data_pipeline._DATA_PIPELINE = None
        await microgrid._data_pipeline.initialize(
            ResamplerConfig(resampling_period=timedelta(seconds=0.1))
        )
        streamer = MockComponentDataStreamer(mockgrid.mock_client)

        dp = microgrid._data_pipeline._DATA_PIPELINE
        assert dp is not None

        return cls(
            mockgrid,
            streamer,
            dp._battery_power_wrapper.status_channel.new_sender(),
        )

    async def stop(self) -> None:
        """Stop the mocks."""
        # pylint: disable=protected-access
        assert microgrid._data_pipeline._DATA_PIPELINE is not None
        await asyncio.gather(
            *[
                microgrid._data_pipeline._DATA_PIPELINE._stop(),
                self.streamer.stop(),
                self.microgrid.cleanup(),
            ]
        )
        # pylint: enable=protected-access


@asynccontextmanager
async def _mocks(
    mocker: MockerFixture,
    *,
    graph: _MicrogridComponentGraph | None = None,
    grid_meter: bool | None = None,
) -> AsyncIterator[_Mocks]:
    """Initialize the mocks."""
    mocks = await _Mocks.new(mocker, graph=graph, grid_meter=grid_meter)
    try:
        yield mocks
    finally:
        await mocks.stop()


class TestPowerDistributingActor:
    # pylint: disable=protected-access
    # pylint: disable=too-many-public-methods
    """Test tool to distribute power."""

    _namespace = "power_distributor"

    async def _patch_battery_pool_status(
        self,
        mocks: _Mocks,
        mocker: MockerFixture,
        battery_ids: abc.Set[int] | None = None,
    ) -> None:
        """Patch the battery pool status.

        If `battery_ids` is not None, the mock will always return `battery_ids`.
        Otherwise, it will return the requested batteries.
        """
        if battery_ids:
            mock = MagicMock(spec=ComponentPoolStatusTracker)
            mock.get_working_components.return_value = battery_ids
            mocker.patch(
                "frequenz.sdk.actor.power_distributing._component_managers._battery_manager"
                ".ComponentPoolStatusTracker",
                return_value=mock,
            )
        else:
            mock = MagicMock(spec=ComponentPoolStatusTracker)
            mock.get_working_components.side_effect = set
            mocker.patch(
                "frequenz.sdk.actor.power_distributing._component_managers._battery_manager"
                ".ComponentPoolStatusTracker",
                return_value=mock,
            )
        await mocks.battery_status_sender.send(
            ComponentPoolStatus(
                working=set(mocks.microgrid.battery_ids), uncertain=set()
            )
        )

    async def test_constructor_with_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the constructor works with a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_batteries(2)
        mockgrid.add_batteries(1, no_meter=True)

        async with mockgrid:
            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
            ) as distributor:
                assert isinstance(distributor._component_manager, BatteryManager)
                assert distributor._component_manager._bat_invs_map == {
                    9: {8},
                    19: {18},
                    29: {28},
                }
                assert distributor._component_manager._inv_bats_map == {
                    8: {9},
                    18: {19},
                    28: {29},
                }

    async def test_constructor_without_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the constructor works without a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_batteries(1)
        mockgrid.add_batteries(2, no_meter=True)

        async with mockgrid:
            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
            ) as distributor:
                assert isinstance(distributor._component_manager, BatteryManager)
                assert distributor._component_manager._bat_invs_map == {
                    9: {8},
                    19: {18},
                    29: {28},
                }
                assert distributor._component_manager._inv_bats_map == {
                    8: {9},
                    18: {19},
                    28: {29},
                }

    async def init_component_data(
        self,
        mocks: _Mocks,
        *,
        skip_batteries: abc.Set[int] | None = None,
        skip_inverters: abc.Set[int] | None = None,
    ) -> None:
        """Send initial component data, for power distributor to start."""
        for battery_id in set(mocks.microgrid.battery_ids) - (skip_batteries or set()):
            mocks.streamer.start_streaming(
                battery_msg(
                    battery_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=PowerBounds(-1000, 0, 0, 1000),
                ),
                0.05,
            )

        for inverter_id in set(mocks.microgrid.battery_inverter_ids) - (
            skip_inverters or set()
        ):
            mocks.streamer.start_streaming(
                inverter_msg(
                    inverter_id,
                    power=PowerBounds(-500, 0, 0, 500),
                ),
                0.05,
            )

    async def test_power_distributor_one_user(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with a single user."""
        mocks = await _Mocks.new(mocker)
        requests_channel = Broadcast[Request]("power_distributor requests")
        results_channel = Broadcast[Result]("power_distributor results")

        request = Request(
            power=Power.from_kilowatts(1.2),
            component_ids={9, 19},
            request_timeout=SAFETY_TIMEOUT,
        )

        await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
        await self.init_component_data(mocks)

        battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
        async with PowerDistributingActor(
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            component_pool_status_sender=battery_status_channel.new_sender(),
            wait_for_data_sec=0.1,
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

        await mocks.stop()

    async def test_power_distributor_exclusion_bounds(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distributing actor rejects non-zero requests in exclusion bounds."""
        async with _mocks(mocker) as mocks:
            await self._patch_battery_pool_status(mocks, mocker, {9, 19})
            await self.init_component_data(mocks, skip_batteries={9, 19})

            mocks.streamer.start_streaming(
                battery_msg(
                    9,
                    soc=Metric(60, Bound(20, 80)),
                    capacity=Metric(98000),
                    power=PowerBounds(-1000, -300, 300, 1000),
                ),
                0.05,
            )

            mocks.streamer.start_streaming(
                battery_msg(
                    19,
                    soc=Metric(60, Bound(20, 80)),
                    capacity=Metric(98000),
                    power=PowerBounds(-1000, -300, 300, 1000),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
            ):
                # zero power requests should pass through despite the exclusion bounds.
                request = Request(
                    power=Power.zero(),
                    component_ids={9, 19},
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
                    component_ids={9, 19},
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
                assert isinstance(
                    result, OutOfBounds
                ), f"Expected OutOfBounds, got {result}"
                assert result.bounds == PowerBounds(-1000, -600, 600, 1000)
                assert result.request == request

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

        async with _mocks(mocker, graph=graph) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")
            request = Request(
                power=Power.from_watts(1200.0),
                component_ids={
                    bat_component1.component_id,
                    bat_component2.component_id,
                },
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")

            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                wait_for_data_sec=0.1,
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

        async with _mocks(mocker, graph=graph) as mocks:
            await self.init_component_data(
                mocks, skip_batteries={bat_components[0].component_id}
            )

            mocks.streamer.start_streaming(
                battery_msg(
                    bat_components[0].component_id,
                    soc=Metric(math.nan, Bound(20, 80)),
                    capacity=Metric(98000),
                    power=PowerBounds(-1000, 0, 0, 1000),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request]("power_distributor")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_watts(1200.0),
                component_ids=set(battery.component_id for battery in bat_components),
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")

            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                wait_for_data_sec=0.1,
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
                    result.msg
                    == "No data for at least one of the given batteries {9, 19}"
                )

    async def test_battery_two_inverters(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with two inverters for one battery."""
        gen = GraphGenerator()
        bat_component = gen.component(ComponentCategory.BATTERY)
        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [gen.battery_with_inverter(bat_component, 2)],
            )
        )

        async with _mocks(mocker, graph=graph) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_watts(1200.0),
                component_ids={bat_component.component_id},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")

            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                wait_for_data_sec=0.1,
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

    async def test_two_batteries_three_inverters(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with two batteries connected to three inverters."""
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (ComponentCategory.METER, gen.batteries_with_inverter(batteries, 3))
        )

        async with _mocks(mocker, graph=graph) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_watts(1700.0),
                component_ids={batteries[0].component_id, batteries[1].component_id},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")

            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                wait_for_data_sec=0.1,
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

        async with _mocks(mocker, graph=graph) as mocks:
            mocks.streamer.start_streaming(
                inverter_msg(
                    inverter.component_id,
                    power=PowerBounds(-1000, -500, 500, 1000),
                ),
                0.05,
            )
            mocks.streamer.start_streaming(
                battery_msg(
                    batteries[0].component_id,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(10_000),
                    power=PowerBounds(-1000, -200, 200, 1000),
                ),
                0.05,
            )
            mocks.streamer.start_streaming(
                battery_msg(
                    batteries[1].component_id,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(10_000),
                    power=PowerBounds(-1000, -100, 100, 1000),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_watts(300.0),
                component_ids={batteries[0].component_id, batteries[1].component_id},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")

            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                wait_for_data_sec=0.1,
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

        async with _mocks(mocker, graph=graph) as mocks:
            await self.init_component_data(
                mocks, skip_batteries={bat.component_id for bat in batteries}
            )
            mocks.streamer.start_streaming(
                battery_msg(
                    batteries[0].component_id,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(10_000),
                    power=PowerBounds(-1000, -200, 200, 1000),
                ),
                0.05,
            )
            mocks.streamer.start_streaming(
                battery_msg(
                    batteries[1].component_id,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(10_000),
                    power=PowerBounds(-1000, -100, 100, 1000),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_watts(300.0),
                component_ids={batteries[0].component_id, batteries[1].component_id},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")

            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                wait_for_data_sec=0.1,
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

        async with _mocks(mocker, graph=graph) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_watts(600.0),
                component_ids={batteries[0].component_id},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")

            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                wait_for_data_sec=0.1,
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
                    r"'Inverters \{48\} are connected to batteries that were not "
                    r"requested: \{19\}'",
                    result.msg,
                )
                assert err_msg is not None

    async def test_battery_soc_nan(self, mocker: MockerFixture) -> None:
        """Test if battery with SoC==NaN is not used."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(mocks, skip_batteries={9})

            mocks.streamer.start_streaming(
                battery_msg(
                    9,
                    soc=Metric(math.nan, Bound(20, 80)),
                    capacity=Metric(98000),
                    power=PowerBounds(-1000, 0, 0, 1000),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={9, 19},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
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
            assert result.succeeded_components == {19}
            assert result.succeeded_power.isclose(Power.from_watts(500.0))
            assert result.excess_power.isclose(Power.from_watts(700.0))
            assert result.request == request

    async def test_battery_capacity_nan(self, mocker: MockerFixture) -> None:
        """Test battery with capacity set to NaN is not used."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(mocks, skip_batteries={9})

            mocks.streamer.start_streaming(
                battery_msg(
                    9,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(math.nan),
                    power=PowerBounds(-1000, 0, 0, 1000),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={9, 19},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
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
            assert result.succeeded_components == {19}
            assert result.succeeded_power.isclose(Power.from_watts(500.0))
            assert result.excess_power.isclose(Power.from_watts(700.0))
            assert result.request == request

    async def test_battery_power_bounds_nan(self, mocker: MockerFixture) -> None:
        """Test battery with power bounds set to NaN is not used."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(
                mocks, skip_batteries={9}, skip_inverters={8, 18}
            )

            mocks.streamer.start_streaming(
                inverter_msg(
                    18,
                    power=PowerBounds(-1000, 0, 0, 1000),
                ),
                0.05,
            )

            # Battery 9 should not work because both battery and inverter sends NaN
            mocks.streamer.start_streaming(
                inverter_msg(
                    8,
                    power=PowerBounds(-1000, 0, 0, math.nan),
                ),
                0.05,
            )

            mocks.streamer.start_streaming(
                battery_msg(
                    9,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(float(98000)),
                    power=PowerBounds(math.nan, 0, 0, math.nan),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={9, 19},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
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
            assert result.succeeded_components == {19}
            assert result.succeeded_power.isclose(Power.from_kilowatts(1.0))
            assert result.excess_power.isclose(Power.from_watts(200.0))
            assert result.request == request

    async def test_power_distributor_invalid_battery_id(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution raises error if any battery id is invalid."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")
            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={9, 100},
                request_timeout=SAFETY_TIMEOUT,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
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

    async def test_power_distributor_one_user_adjust_power_consume(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request]("power_distributor")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={9, 19},
                request_timeout=SAFETY_TIMEOUT,
                adjust_power=False,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
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

    async def test_power_distributor_one_user_adjust_power_supply(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=-Power.from_kilowatts(1.2),
                component_ids={9, 19},
                request_timeout=SAFETY_TIMEOUT,
                adjust_power=False,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
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

    async def test_power_distributor_one_user_adjust_power_success(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.0),
                component_ids={9, 19},
                request_timeout=SAFETY_TIMEOUT,
                adjust_power=False,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
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

    async def test_not_all_batteries_are_working(self, mocker: MockerFixture) -> None:
        """Test if power distribution works if not all batteries are working."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            batteries = {9, 19}

            await self._patch_battery_pool_status(mocks, mocker, batteries - {9})

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
            ):
                request = Request(
                    power=Power.from_kilowatts(1.2),
                    component_ids=batteries,
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
                assert result.succeeded_components == {19}
                assert result.excess_power.isclose(Power.from_watts(700.0))
                assert result.succeeded_power.isclose(Power.from_watts(500.0))
                assert result.request == request

    async def test_partial_failure_result(self, mocker: MockerFixture) -> None:
        """Test power results when the microgrid failed to set power for one of the batteries."""
        async with _mocks(mocker, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            batteries = {9, 19, 29}
            failed_batteries = {9}
            failed_power = 500.0

            await self._patch_battery_pool_status(mocks, mocker, batteries)

            mocker.patch(
                "frequenz.sdk.actor.power_distributing._component_managers._battery_manager"
                ".BatteryManager._parse_result",
                return_value=(failed_power, failed_batteries),
            )

            requests_channel = Broadcast[Request]("power_distributor requests")
            results_channel = Broadcast[Result]("power_distributor results")

            battery_status_channel = Broadcast[ComponentPoolStatus]("battery_status")
            async with PowerDistributingActor(
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                wait_for_data_sec=0.1,
            ):
                request = Request(
                    power=Power.from_kilowatts(1.70),
                    component_ids=batteries,
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
                assert result.succeeded_components == batteries - failed_batteries
                assert result.failed_components == failed_batteries
                assert result.succeeded_power.isclose(Power.from_watts(1000.0))
                assert result.failed_power.isclose(Power.from_watts(failed_power))
                assert result.excess_power.isclose(Power.from_watts(200.0))
                assert result.request == request
