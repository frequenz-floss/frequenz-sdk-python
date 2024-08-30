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

    async def test_producer_fallback_formula(self, mocker: MockerFixture) -> None:
        """Test the producer power formula with fallback formulas."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2)
        # CHP has no meter, so no fallback component
        mockgrid.add_chps(1, no_meters=True)

        async with mockgrid, AsyncExitStack() as stack:
            producer = microgrid.producer()
            stack.push_async_callback(producer.stop)
            producer_power_receiver = producer.power.new_receiver()

            # Note: ProducerPowerFormula has a "nones-are-zero" rule, that says:
            # * if the meter value is None, it should be treated as None.
            # * for other components None is treated as 0.

            # fmt: off
            expected_input_output: list[
                tuple[list[float | None], list[float | None], list[float | None], Power | None]
            ] = [
                # ([pv_meter_power], [pv_inverter_power], [chp_power], expected_power)
                # Add power from meters and chp
                ([-1.0, -2.0], [None, -200.0], [300], Power.from_watts(297.0)),
                ([-1.0, -10], [-100.0, -200.0], [400], Power.from_watts(389.0)),
                # Case 2: The first meter is unavailable (None).
                # Subscribe to the fallback inverter, but return None as the result,
                # according to the "nones-are-zero" rule
                ([None, -2.0], [-100, -200.0], [400], None),
                # Case 3: First meter is unavailable (None). Fallback inverter provides
                # a value.
                # Add second meter, first inverter and chp power
                ([None, -2.0], [-100, -200.0], [400], Power.from_watts(298.0)),
                ([None, -2.0], [-50, -200.0], [300], Power.from_watts(248.0)),
                # Case 4: Both first meter and its fallback inverter are unavailable
                # (None). Return 0 from failing component according to the
                # "nones-are-zero" rule.
                ([None, -2.0], [None, -200.0], [300], Power.from_watts(298.0)),
                ([None, -10.0], [-20.0, -200.0], [300], Power.from_watts(270.0)),
                # Case 5: CHP is unavailable. Return 0 from failing component
                # according to the "nones-are-zero" rule.
                ([None, -10.0], [-20.0, -200.0], [None], Power.from_watts(-30.0)),
                # Case 6: Both meters are unavailable (None). Subscribe for fallback inverter
                ([None, None], [-20.0, -200.0], [None], None),
                ([None, None], [-20.0, -200.0], [None], Power.from_watts(-220.0)),
                ([None, None], [None, -200.0], [None], Power.from_watts(-200.0)),
                # Case 7: All components are unavailable (None). Return 0 according to the
                # "nones-are-zero" rule.
                ([None, None], [None, None], [None], Power.from_watts(0)),
                ([None, None], [None, None], [None], Power.from_watts(0)),
                ([None, None], [None, None], [300.0], Power.from_watts(300.0)),
                ([-200.0, None], [None, -100.0], [50.0], Power.from_watts(-250.0)),
                ([-200.0, -200.0], [-10.0, -20.0], [50.0], Power.from_watts(-350.0)),
                # Case 8: Meter is unavailable but we already subscribed for inverter
                # So don't return None in this case. Just proper formula result.
                ([None, -200.0], [-10.0, -100.0], [50.0], Power.from_watts(-160.0)),

            ]
            # fmt: on

            for idx, (
                meter_power,
                pv_inverter_power,
                chp_power,
                expected_power,
            ) in enumerate(expected_input_output):
                await mockgrid.mock_resampler.send_chp_power(chp_power)
                await mockgrid.mock_resampler.send_meter_power(meter_power)
                await mockgrid.mock_resampler.send_pv_inverter_power(pv_inverter_power)
                mockgrid.mock_resampler.next_ts()

                result = await producer_power_receiver.receive()
                assert result.value == expected_power, (
                    f"Test case {idx} failed:"
                    + f" meter_power: {meter_power}"
                    + f" pv_inverter_power {pv_inverter_power}"
                    + f" chp_power {chp_power}"
                    + f" expected_power: {expected_power}"
                    + f" actual_power: {result.value}"
                )
