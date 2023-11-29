# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Test the logical component for calculating high level consumer metrics."""

from contextlib import AsyncExitStack

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.timeseries._quantities import Power

from .mock_microgrid import MockMicrogrid


class TestConsumer:
    """Tests for the consumer power formula."""

    async def test_consumer_power_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the consumer power formula with a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            consumer = microgrid.consumer()
            stack.push_async_callback(consumer.stop)
            consumer_power_receiver = consumer.power.new_receiver()

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
            consumer = microgrid.consumer()
            stack.push_async_callback(consumer.stop)
            consumer_power_receiver = consumer.power.new_receiver()

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
            consumer = microgrid.consumer()
            stack.push_async_callback(consumer.stop)
            consumer_power_receiver = consumer.power.new_receiver()

            await mockgrid.mock_resampler.send_non_existing_component_value()
            assert (await consumer_power_receiver.receive()).value == Power.from_watts(
                0.0
            )
