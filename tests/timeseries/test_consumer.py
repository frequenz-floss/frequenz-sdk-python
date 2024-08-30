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

    async def test_consumer_power_fallback_formula_with_grid_meter(
        self, mocker: MockerFixture
    ) -> None:
        """Test the consumer power formula with a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_batteries(1)
        mockgrid.add_solar_inverters(1)
        mockgrid.add_solar_inverters(1, no_meter=True)

        # formula is grid_meter - battery - pv1 - pv2

        async with mockgrid, AsyncExitStack() as stack:
            consumer = microgrid.consumer()
            stack.push_async_callback(consumer.stop)
            consumer_power_formula = consumer.power
            print(consumer_power_formula)
            consumer_power_receiver = consumer_power_formula.new_receiver()

            # Note: ConsumerPowerFormula has a "nones-are-zero" rule, that says:
            # * if the meter value is None, it should be treated as None.
            # * for other components None is treated as 0.

            # fmt: off
            expected_input_output: list[
                tuple[list[float | None], list[float | None], list[float | None], Power | None]
            ] = [
                # ([grid_meter, bat_meter, pv1_meter], [bat_inv], [pv1_inv, pv2_inv], expected_power) # noqa: E501
                ([100, 100, -50], [100], [-200, -300], Power.from_watts(350)),
                ([500, -200, -100], [100], [-200, -100], Power.from_watts(900)),
                # Case 2: The meter is unavailable (None).
                # Subscribe to the fallback inverter, but return None as the result,
                # according to the "nones-are-zero" rule
                ([500, None, -100], [100], [-200, -100], None),
                ([500, None, -100], [100], [-200, -100], Power.from_watts(600)),
                # Case 3: Second meter is unavailable (None).
                ([500, None, None], [100], [-200, -100], None),
                ([500, None, None], [100], [-200, -100], Power.from_watts(700)),
                # Case 3: pv2_inv is unavailable (None).
                # It has no fallback, so return 0 as its value according to
                # the "nones-are-zero" rule.
                ([500, None, None], [100], [-200, None], Power.from_watts(600)),
                # Case 4: Grid meter is unavailable (None).
                # It has no fallback, so return None according to the "nones-are-zero" rule.
                ([None, 100, -50], [100], [-200, -300], None),
                ([None, 200, -50], [100], [-200, -300], None),
                ([100, 100, -50], [100], [-200, -300], Power.from_watts(350)),
                # Case 5: Only grid meter is working
                ([100, None, None], [None], [None, None], Power.from_watts(100)),
                ([-500, None, None], [None], [None, None], Power.from_watts(-500)),
                # Case 6: Nothing is working
                ([None, None, None], [None], [None, None], None),
            ]
            # fmt: on

            for idx, (
                meter_power,
                bat_inv_power,
                pv_inv_power,
                expected_power,
            ) in enumerate(expected_input_output):
                await mockgrid.mock_resampler.send_meter_power(meter_power)
                await mockgrid.mock_resampler.send_bat_inverter_power(bat_inv_power)
                await mockgrid.mock_resampler.send_pv_inverter_power(pv_inv_power)
                mockgrid.mock_resampler.next_ts()

                result = await consumer_power_receiver.receive()
                assert result.value == expected_power, (
                    f"Test case {idx} failed:"
                    + f" meter_power: {meter_power}"
                    + f" bat_inverter_power {bat_inv_power}"
                    + f" pv_inverter_power {pv_inv_power}"
                    + f" expected_power: {expected_power}"
                    + f" actual_power: {result.value}"
                )

    async def test_consumer_power_fallback_formula_without_grid_meter(
        self, mocker: MockerFixture
    ) -> None:
        """Test the consumer power formula with a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_consumer_meters(2)
        mockgrid.add_batteries(1)
        mockgrid.add_solar_inverters(1, no_meter=True)

        # formula is sum of consumer meters

        async with mockgrid, AsyncExitStack() as stack:
            consumer = microgrid.consumer()
            stack.push_async_callback(consumer.stop)
            consumer_power_receiver = consumer.power.new_receiver()

            # Note: ConsumerPowerFormula has a "nones-are-zero" rule, that says:
            # * if the meter value is None, it should be treated as None.
            # * for other components None is treated as 0.

            # fmt: off
            expected_input_output: list[
                tuple[list[float | None], list[float | None], list[float | None], Power | None]
            ] = [
                # ([consumer_meter1, consumer_meter2, bat_meter], [bat_inv], [pv_inv], expected_power) # noqa: E501
                ([100, 100, -50], [100], [-200,], Power.from_watts(200)),
                ([500, 100, -50], [100], [-200,], Power.from_watts(600)),
                # One of the meters is invalid - should return None according to none-are-zero rule
                ([None, 100, -50], [100], [-200,], None),
                ([None, None, -50], [100], [-200,], None),
                ([500, None, -50], [100], [-200,], None),
                ([2000, 1000, None], [None], [None], Power.from_watts(3000)),
            ]
            # fmt: on

            for idx, (
                meter_power,
                bat_inv_power,
                pv_inv_power,
                expected_power,
            ) in enumerate(expected_input_output):
                await mockgrid.mock_resampler.send_meter_power(meter_power)
                await mockgrid.mock_resampler.send_bat_inverter_power(bat_inv_power)
                await mockgrid.mock_resampler.send_pv_inverter_power(pv_inv_power)
                mockgrid.mock_resampler.next_ts()

                result = await consumer_power_receiver.receive()
                assert result.value == expected_power, (
                    f"Test case {idx} failed:"
                    + f" meter_power: {meter_power}"
                    + f" bat_inverter_power {bat_inv_power}"
                    + f" pv_inverter_power {pv_inv_power}"
                    + f" expected_power: {expected_power}"
                    + f" actual_power: {result.value}"
                )
