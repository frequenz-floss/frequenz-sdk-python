# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `Grid` module."""

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
from frequenz.sdk.timeseries import Current, Fuse, Power, Quantity

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
    # pylint: disable=protected-access
    graph = gr._MicrogridComponentGraph(components=components, connections=connections)

    async with MockMicrogrid(graph=graph, mocker=mocker):
        grid = microgrid.grid()

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

    # pylint: disable=protected-access
    graph = gr._MicrogridComponentGraph(components=components, connections=connections)

    async with MockMicrogrid(graph=graph, mocker=mocker):
        grid = microgrid.grid()
        assert grid is not None

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

    # pylint: disable=protected-access
    graph = gr._MicrogridComponentGraph(components=components, connections=connections)

    async with MockMicrogrid(graph=graph, mocker=mocker):
        grid = microgrid.grid()
        assert grid is not None
        assert grid.fuse is None



async def test_grid_power_1(mocker: MockerFixture) -> None:
    """Test the grid power formula with a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    mockgrid.add_batteries(2)
    mockgrid.add_solar_inverters(1)

    async with mockgrid:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"

        grid_power_recv = grid.power.new_receiver()

        grid_meter_recv = get_resampled_stream(
            grid._formula_pool._namespace,  # pylint: disable=protected-access
            mockgrid.meter_ids[0],
            ComponentMetricId.ACTIVE_POWER,
            Power.from_watts,
        )

        results = []
        grid_meter_data = []
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

        await grid.stop()

        assert equal_float_lists(results, grid_meter_data)


async def test_grid_power_2(mocker: MockerFixture) -> None:
    """Test the grid power formula without a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_consumer_meters(1)
    mockgrid.add_batteries(1, no_meter=False)
    mockgrid.add_batteries(1, no_meter=True)
    mockgrid.add_solar_inverters(1)

    async with mockgrid:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"

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

        results: list[Quantity] = []
        meter_sums: list[Quantity] = []
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
            results.append(val.value)
            meter_sums.append(Quantity(meter_sum))

        await grid.stop()

        assert len(results) == 10
        assert equal_float_lists(results, meter_sums)


async def test_grid_production_consumption_power_consumer_meter(
    mocker: MockerFixture,
) -> None:
    """Test the grid production and consumption power formulas."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_consumer_meters()
    mockgrid.add_batteries(2)
    mockgrid.add_solar_inverters(1)

    async with mockgrid:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        grid_recv = grid.power.new_receiver()

        await mockgrid.mock_resampler.send_meter_power([1.0, 2.0, 3.0, 4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(10.0)

        await mockgrid.mock_resampler.send_meter_power([1.0, 2.0, -3.0, -4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(-4.0)

        await grid.stop()


async def test_grid_production_consumption_power_no_grid_meter(
    mocker: MockerFixture,
) -> None:
    """Test the grid production and consumption power formulas."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_batteries(2)
    mockgrid.add_solar_inverters(1)

    async with mockgrid:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        grid_recv = grid.power.new_receiver()

        await mockgrid.mock_resampler.send_meter_power([2.5, 3.5, 4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(10.0)

        await mockgrid.mock_resampler.send_meter_power([3.0, -3.0, -4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(-4.0)

        await grid.stop()


async def test_consumer_power_2_grid_meters(mocker: MockerFixture) -> None:
    """Test the grid power formula with two grid meters."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    # with no further successor these will be detected as grid meters
    mockgrid.add_consumer_meters(2)

    async with mockgrid:
        grid = microgrid.grid()
        assert grid, "Grid is not initialized"
        grid_recv = grid.power.new_receiver()

        await mockgrid.mock_resampler.send_meter_power([1.0, 2.0])
        assert (await grid_recv.receive()).value == Power.from_watts(3.0)

        await grid.stop()
