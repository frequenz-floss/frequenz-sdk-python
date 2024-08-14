# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""


from contextlib import AsyncExitStack

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.timeseries._quantities import Power

from .mock_microgrid import MockMicrogrid

# pylint: disable=too-many-locals
# pylint: disable=protected-access


class TestLogicalMeter:  # pylint: disable=too-many-public-methods
    """Tests for the logical meter."""

    async def test_chp_power(self, mocker: MockerFixture) -> None:
        """Test the chp power formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_chps(1)
        mockgrid.add_batteries(2)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)

            chp_power_receiver = logical_meter.chp_power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([2.0, 3.0, 4.0])
            assert (await chp_power_receiver.receive()).value == Power.from_watts(2.0)

            await mockgrid.mock_resampler.send_meter_power([-12.0, None, 10.2])
            assert (await chp_power_receiver.receive()).value == Power.from_watts(-12.0)

    async def test_pv_power(self, mocker: MockerFixture) -> None:
        """Test the pv power formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            pv_pool = microgrid.new_pv_pool(priority=5)
            stack.push_async_callback(pv_pool.stop)
            pv_power_receiver = pv_pool.power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([-1.0, -2.0])
            await mockgrid.mock_resampler.send_pv_inverter_power([-10.0, -20.0])
            assert (await pv_power_receiver.receive()).value == Power.from_watts(-3.0)

    async def test_pv_power_no_meter(self, mocker: MockerFixture) -> None:
        """Test the pv power formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2, no_meter=True)

        async with mockgrid, AsyncExitStack() as stack:
            pv_pool = microgrid.new_pv_pool(priority=5)
            stack.push_async_callback(pv_pool.stop)
            pv_power_receiver = pv_pool.power.new_receiver()

            await mockgrid.mock_resampler.send_pv_inverter_power([-1.0, -2.0])
            assert (await pv_power_receiver.receive()).value == Power.from_watts(-3.0)

    async def test_pv_power_no_pv_components(self, mocker: MockerFixture) -> None:
        """Test the pv power formula without having any pv components."""
        async with (
            MockMicrogrid(grid_meter=True, mocker=mocker) as mockgrid,
            AsyncExitStack() as stack,
        ):
            pv_pool = microgrid.new_pv_pool(priority=5)
            stack.push_async_callback(pv_pool.stop)
            pv_power_receiver = pv_pool.power.new_receiver()

            await mockgrid.mock_resampler.send_non_existing_component_value()
            assert (await pv_power_receiver.receive()).value == Power.zero()
