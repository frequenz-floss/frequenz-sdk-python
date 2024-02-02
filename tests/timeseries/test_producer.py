# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Test the logical component for calculating high level producer metrics."""

from contextlib import AsyncExitStack

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.timeseries._quantities import Power

from .mock_microgrid import MockMicrogrid


class TestProducer:
    """Tests for the producer power formula."""

    async def test_producer_power(self, mocker: MockerFixture) -> None:
        """Test the producer power formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2)
        mockgrid.add_chps(2)

        async with mockgrid, AsyncExitStack() as stack:
            producer = microgrid.producer()
            stack.push_async_callback(producer.stop)
            producer_power_receiver = producer.power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([2.0, 3.0, 4.0, 5.0])
            assert (await producer_power_receiver.receive()).value == Power.from_watts(
                14.0
            )

    async def test_producer_power_no_chp(self, mocker: MockerFixture) -> None:
        """Test the producer power formula without a chp."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            producer = microgrid.producer()
            stack.push_async_callback(producer.stop)
            producer_power_receiver = producer.power.new_receiver()

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
            producer = microgrid.producer()
            stack.push_async_callback(producer.stop)
            producer_power_receiver = producer.power.new_receiver()

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
            producer = microgrid.producer()
            stack.push_async_callback(producer.stop)
            producer_power_receiver = producer.power.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([20.0, 2.0])
            assert (await producer_power_receiver.receive()).value == Power.from_watts(
                2.0
            )

    async def test_no_producer_power(self, mocker: MockerFixture) -> None:
        """Test the producer power formula without producers."""
        async with (
            MockMicrogrid(grid_meter=True, mocker=mocker) as mockgrid,
            AsyncExitStack() as stack,
        ):
            producer = microgrid.producer()
            stack.push_async_callback(producer.stop)
            producer_power_receiver = producer.power.new_receiver()

            await mockgrid.mock_resampler.send_non_existing_component_value()
            assert (await producer_power_receiver.receive()).value == Power.from_watts(
                0.0
            )
