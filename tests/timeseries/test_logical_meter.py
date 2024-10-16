# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""


from contextlib import AsyncExitStack

from frequenz.quantities import Power
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid

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

    async def test_pv_power_with_failing_meter(self, mocker: MockerFixture) -> None:
        """Test the pv power formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            pv_pool = microgrid.new_pv_pool(priority=5)
            stack.push_async_callback(pv_pool.stop)
            pv_power_receiver = pv_pool.power.new_receiver()

            # Note: PvPowerFormula has a "nones-are-zero" rule, that says:
            # * if the meter value is None, it should be treated as None.
            # * for other components None is treated as 0.

            expected_input_output: list[
                tuple[list[float | None], list[float | None], Power | None]
            ] = [
                # ([meter_power], [pv_inverter_power], expected_power)
                #
                # Case 1: Both meters are available, so inverters are not used.
                ([-1.0, -2.0], [None, -5.0], Power.from_watts(-3.0)),
                ([-1.0, -2.0], [-10.0, -20.0], Power.from_watts(-3.0)),
                # Case 2: The first meter is unavailable (None).
                # Subscribe to the fallback inverter, but return None as the result,
                # according to the "nones-are-zero" rule
                ([None, -2.0], [-10.0, -20.0], None),
                # Case 3: First meter is unavailable (None). Fallback inverter provides
                # a value.
                ([None, -2.0], [-10.0, -20.0], Power.from_watts(-12.0)),
                ([None, -2.0], [-11.0, -20.0], Power.from_watts(-13.0)),
                # Case 4: Both first meter and its fallback inverter are unavailable
                # (None). Return 0 according to the "nones-are-zero" rule.
                ([None, -2.0], [None, -20.0], Power.from_watts(-2.0)),
                ([None, -2.0], [-11.0, -20.0], Power.from_watts(-13.0)),
                # Case 5: Both meters are unavailable (None).
                # Subscribe to the fallback inverter, but return None as the result,
                # according "nones-are-zero" rule
                ([None, None], [-5.0, -20.0], None),
                # Case 6: Both meters are unavailable (None). Fallback inverter provides
                # a values.
                ([None, None], [-5.0, -20.0], Power.from_watts(-25.0)),
                # Case 7: All components are unavailable (None).
                # Return 0 according to the "nones-are-zero" rule.
                ([None, None], [None, None], Power.from_watts(0.0)),
                ([None, None], [-5.0, -20.0], Power.from_watts(-25.0)),
                # Case 8: Meters becomes available and inverter values are not used.
                ([-10.0, None], [-5.0, -20.0], Power.from_watts(-30.0)),
                ([-10.0, -2.0], [-5.0, -20.0], Power.from_watts(-12.0)),
            ]

            for idx, (meter_power, pv_inverter_power, expected_power) in enumerate(
                expected_input_output
            ):
                await mockgrid.mock_resampler.send_meter_power(meter_power)
                await mockgrid.mock_resampler.send_pv_inverter_power(pv_inverter_power)
                mockgrid.mock_resampler.next_ts()

                result = await pv_power_receiver.receive()
                assert result.value == expected_power, (
                    f"Test case {idx} failed:"
                    + f" meter_power: {meter_power}"
                    + f" pv_inverter_power {pv_inverter_power}"
                    + f" expected_power: {expected_power}"
                    + f" actual_power: {result.value}"
                )
