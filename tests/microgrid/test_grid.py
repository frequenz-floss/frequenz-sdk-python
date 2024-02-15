# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `Grid` module."""

from contextlib import AsyncExitStack

from pytest_mock import MockerFixture

import frequenz.sdk.microgrid.component_graph as gr
from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import (
    Component,
    ComponentCategory,
    ComponentMetricId,
    GridMetadata,
)
from frequenz.sdk.timeseries import Current, Fuse, Power

from ..timeseries._formula_engine.utils import equal_float_lists, get_resampled_stream
from ..timeseries.mock_microgrid import MockMicrogrid


async def test_grid_1(mocker: MockerFixture) -> None:
    """Test the grid connection module."""
    # The tests here need to be in this exact sequence, because the grid connection
    # is a singleton. Once it gets created, it stays in memory for the duration of
    # the tests, unless we explicitly delete it.

    # validate that islands with no grid connection are accepted.
    components = {
        Component(1, ComponentCategory.NONE),
        Component(2, ComponentCategory.METER),
    }
    connections = {
        Connection(1, 2),
    }

    graph = gr._MicrogridComponentGraph(  # pylint: disable=protected-access
        components=components, connections=connections
    )

    async with MockMicrogrid(graph=graph, mocker=mocker), AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid is not None
        stack.push_async_callback(grid.stop)

        assert grid
        assert grid.fuse
        assert grid.fuse.max_current == Current.from_amperes(0.0)


def _create_fuse() -> Fuse:
    """Create a fuse with a fixed current.

    Returns:
        Fuse: The fuse.
    """
    fuse_current = Current.from_amperes(123.0)
    fuse = Fuse(fuse_current)
    return fuse


async def test_grid_2(mocker: MockerFixture) -> None:
    """Validate that microgrids with one grid connection are accepted."""
    components = {
        Component(1, ComponentCategory.GRID, None, GridMetadata(_create_fuse())),
        Component(2, ComponentCategory.METER),
    }
    connections = {
        Connection(1, 2),
    }

    graph = gr._MicrogridComponentGraph(  # pylint: disable=protected-access
        components=components, connections=connections
    )

    async with MockMicrogrid(graph=graph, mocker=mocker), AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid is not None
        stack.push_async_callback(grid.stop)

        expected_fuse_current = Current.from_amperes(123.0)
        expected_fuse = Fuse(expected_fuse_current)

        assert grid.fuse == expected_fuse


async def test_grid_3(mocker: MockerFixture) -> None:
    """Validate that microgrids with a grid connection without a fuse are instantiated."""
    components = {
        Component(1, ComponentCategory.GRID, None, GridMetadata(None)),
        Component(2, ComponentCategory.METER),
    }
    connections = {
        Connection(1, 2),
    }

    graph = gr._MicrogridComponentGraph(  # pylint: disable=protected-access
        components=components, connections=connections
    )

    async with MockMicrogrid(graph=graph, mocker=mocker), AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid is not None
        stack.push_async_callback(grid.stop)
        assert grid.fuse is None


async def test_grid_power_1(mocker: MockerFixture) -> None:
    """Test the grid power formula with a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    mockgrid.add_batteries(2)
    mockgrid.add_solar_inverters(1)

    results = []
    grid_meter_data = []
    async with mockgrid, AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        stack.push_async_callback(grid.stop)

        grid_power_recv = grid.power.new_receiver()

        grid_meter_recv = get_resampled_stream(
            grid._formula_pool._namespace,  # pylint: disable=protected-access
            mockgrid.meter_ids[0],
            ComponentMetricId.ACTIVE_POWER,
            Power.from_watts,
        )

        for count in range(10):
            await mockgrid.mock_resampler.send_meter_power(
                [20.0 + count, 12.0, -13.0, -5.0]
            )
            val = await grid_meter_recv.receive()
            assert (
                val is not None
                and val.value is not None
                and val.value.as_watts() != 0.0
            )
            grid_meter_data.append(val.value)

            val = await grid_power_recv.receive()
            assert val is not None and val.value is not None
            results.append(val.value)

    assert equal_float_lists(results, grid_meter_data)


async def test_grid_power_2(mocker: MockerFixture) -> None:
    """Test the grid power formula without a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_consumer_meters(1)
    mockgrid.add_batteries(1, no_meter=False)
    mockgrid.add_batteries(1, no_meter=True)
    mockgrid.add_solar_inverters(1)

    results: list[float] = []
    meter_sums: list[float] = []
    async with mockgrid, AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        stack.push_async_callback(grid.stop)

        grid_power_recv = grid.power.new_receiver()

        component_receivers = [
            get_resampled_stream(
                grid._formula_pool._namespace,  # pylint: disable=protected-access
                component_id,
                ComponentMetricId.ACTIVE_POWER,
                Power.from_watts,
            )
            for component_id in [
                *mockgrid.meter_ids,
                # The last battery has no meter, so we get the power from the inverter
                mockgrid.battery_inverter_ids[-1],
            ]
        ]

        for count in range(10):
            await mockgrid.mock_resampler.send_meter_power([20.0 + count, 12.0, -13.0])
            await mockgrid.mock_resampler.send_bat_inverter_power([0.0, -5.0])
            meter_sum = 0.0
            for recv in component_receivers:
                val = await recv.receive()
                assert (
                    val is not None
                    and val.value is not None
                    and val.value.as_watts() != 0.0
                )
                meter_sum += val.value.as_watts()

            val = await grid_power_recv.receive()
            assert val is not None and val.value is not None
            results.append(float(val.value))
            meter_sums.append(meter_sum)

    assert len(results) == 10
    assert equal_float_lists(results, meter_sums)


async def test_grid_power_3_phase_side_meter(mocker: MockerFixture) -> None:
    """Test the grid 3-phase power with a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    mockgrid.add_batteries(1, no_meter=True)
    mockgrid.add_batteries(1, no_meter=False)

    async with mockgrid, AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        stack.push_async_callback(grid.stop)

        grid_power_3_phase_recv = (
            grid._power_3_phase.new_receiver()  # pylint: disable=protected-access
        )

        for count in range(10):
            watts_delta = 1 if count % 2 == 0 else -1
            watts_phases: list[float | None] = [
                220.0 * watts_delta,
                219.8 * watts_delta,
                220.2 * watts_delta,
            ]

            await mockgrid.mock_resampler.send_meter_power_3_phase(
                [watts_phases, watts_phases]
            )

            val = await grid_power_3_phase_recv.receive()
            assert val is not None
            assert val.value_p1 and val.value_p2 and val.value_p3
            assert val.value_p1.as_watts() == watts_phases[0]
            assert val.value_p2.as_watts() == watts_phases[1]
            assert val.value_p3.as_watts() == watts_phases[2]


async def test_grid_power_3_phase_none_values(mocker: MockerFixture) -> None:
    """Test the grid 3-phase power with None values."""
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    mockgrid.add_batteries(2, no_meter=False)

    async with mockgrid, AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        stack.push_async_callback(grid.stop)

        grid_power_3_phase_recv = (
            grid._power_3_phase.new_receiver()  # pylint: disable=protected-access
        )

        for count in range(10):
            watts_delta = 1 if count % 2 == 0 else -1
            watts_phases: list[float | None] = [
                220.0 * watts_delta,
                219.8 * watts_delta,
                220.2 * watts_delta,
            ]

            await mockgrid.mock_resampler.send_meter_power_3_phase(
                [watts_phases, [None, None, None], [None, 219.8, 220.2]]
            )

            val = await grid_power_3_phase_recv.receive()
            assert val is not None
            assert val.value_p1 and val.value_p2 and val.value_p3
            assert val.value_p1.as_watts() == watts_phases[0]
            assert val.value_p2.as_watts() == watts_phases[1]
            assert val.value_p3.as_watts() == watts_phases[2]


async def test_grid_production_consumption_power_consumer_meter(
    mocker: MockerFixture,
) -> None:
    """Test the grid production and consumption power formulas."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_consumer_meters()
    mockgrid.add_batteries(2)
    mockgrid.add_solar_inverters(1)

    async with mockgrid, AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        stack.push_async_callback(grid.stop)

        grid_recv = grid.power.new_receiver()

        await mockgrid.mock_resampler.send_meter_power([1.0, 2.0, 3.0, 4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(10.0)

        await mockgrid.mock_resampler.send_meter_power([1.0, 2.0, -3.0, -4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(-4.0)


async def test_grid_production_consumption_power_no_grid_meter(
    mocker: MockerFixture,
) -> None:
    """Test the grid production and consumption power formulas."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_batteries(2)
    mockgrid.add_solar_inverters(1)

    async with mockgrid, AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        stack.push_async_callback(grid.stop)

        grid_recv = grid.power.new_receiver()

        await mockgrid.mock_resampler.send_meter_power([2.5, 3.5, 4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(10.0)

        await mockgrid.mock_resampler.send_meter_power([3.0, -3.0, -4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(-4.0)


async def test_consumer_power_2_grid_meters(mocker: MockerFixture) -> None:
    """Test the grid power formula with two grid meters."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    # with no further successor these will be detected as grid meters
    mockgrid.add_consumer_meters(2)

    async with mockgrid, AsyncExitStack() as stack:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        stack.push_async_callback(grid.stop)

        grid_recv = grid.power.new_receiver()

        await mockgrid.mock_resampler.send_meter_power([1.0, 2.0])
        assert (await grid_recv.receive()).value == Power.from_watts(3.0)
