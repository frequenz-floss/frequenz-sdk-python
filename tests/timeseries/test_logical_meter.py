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
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            pv_power_receiver = logical_meter.pv_power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([-1.0, -2.0])
            assert (await pv_power_receiver.receive()).value == Power.from_watts(-3.0)

    async def test_pv_power_no_meter(self, mocker: MockerFixture) -> None:
        """Test the pv power formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2, no_meter=True)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            pv_power_receiver = logical_meter.pv_power.new_receiver()

            await mockgrid.mock_resampler.send_pv_inverter_power([-1.0, -2.0])
            assert (await pv_power_receiver.receive()).value == Power.from_watts(-3.0)

    async def test_pv_power_no_pv_components(self, mocker: MockerFixture) -> None:
        """Test the pv power formula without having any pv components."""
        async with MockMicrogrid(
            grid_meter=True, mocker=mocker
        ) as mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            pv_power_receiver = logical_meter.pv_power.new_receiver()

            await mockgrid.mock_resampler.send_non_existing_component_value()
            assert (await pv_power_receiver.receive()).value == Power.zero()

    async def test_consumer_power_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the consumer power formula with a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            consumer_power_receiver = logical_meter.consumer_power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([20.0, 2.0, 3.0, 4.0, 5.0])
            assert (await consumer_power_receiver.receive()).value == Power.from_watts(
                6.0
            )

    async def test_consumer_power_no_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the consumer power formula without a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_consumer_meters()
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            consumer_power_receiver = logical_meter.consumer_power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([20.0, 2.0, 3.0, 4.0, 5.0])
            assert (await consumer_power_receiver.receive()).value == Power.from_watts(
                20.0
            )

    async def test_consumer_power_no_grid_meter_no_consumer_meter(
        self, mocker: MockerFixture
    ) -> None:
        """Test the consumer power formula without a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            consumer_power_receiver = logical_meter.consumer_power.new_receiver()

            await mockgrid.mock_resampler.send_non_existing_component_value()
            assert (await consumer_power_receiver.receive()).value == Power.from_watts(
                0.0
            )

    async def test_producer_power(self, mocker: MockerFixture) -> None:
        """Test the producer power formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2)
        mockgrid.add_chps(2)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            producer_power_receiver = logical_meter.producer_power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([2.0, 3.0, 4.0, 5.0])
            assert (await producer_power_receiver.receive()).value == Power.from_watts(
                14.0
            )

    async def test_producer_power_no_chp(self, mocker: MockerFixture) -> None:
        """Test the producer power formula without a chp."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            producer_power_receiver = logical_meter.producer_power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([2.0, 3.0])
            assert (await producer_power_receiver.receive()).value == Power.from_watts(
                5.0
            )

    async def test_producer_power_no_pv_no_consumer_meter(
        self, mocker: MockerFixture
    ) -> None:
        """Test the producer power formula without pv and without consumer meter."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_chps(1, True)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            producer_power_receiver = logical_meter.producer_power.new_receiver()

            await mockgrid.mock_resampler.send_chp_power([2.0])
            assert (await producer_power_receiver.receive()).value == Power.from_watts(
                2.0
            )

    async def test_producer_power_no_pv(self, mocker: MockerFixture) -> None:
        """Test the producer power formula without pv."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_consumer_meters()
        mockgrid.add_chps(1)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            producer_power_receiver = logical_meter.producer_power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([20.0, 2.0])
            assert (await producer_power_receiver.receive()).value == Power.from_watts(
                2.0
            )

    async def test_no_producer_power(self, mocker: MockerFixture) -> None:
        """Test the producer power formula without producers."""
        async with MockMicrogrid(
            grid_meter=True, mocker=mocker
        ) as mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            producer_power_receiver = logical_meter.producer_power.new_receiver()

            await mockgrid.mock_resampler.send_non_existing_component_value()
            assert (await producer_power_receiver.receive()).value == Power.from_watts(
                0.0
            )
