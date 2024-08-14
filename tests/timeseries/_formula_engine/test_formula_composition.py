# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for formula composition."""


import math
from contextlib import AsyncExitStack

import pytest
from frequenz.client.microgrid import ComponentMetricId
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.timeseries import Power, Sample

from ..mock_microgrid import MockMicrogrid
from .utils import get_resampled_stream


class TestFormulaComposition:
    """Tests for formula composition."""

    async def test_formula_composition(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the composition of formulas."""
        mockgrid = MockMicrogrid(grid_meter=False, num_namespaces=2, mocker=mocker)
        mockgrid.add_consumer_meters()
        mockgrid.add_batteries(3)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)

            battery_pool = microgrid.new_battery_pool(priority=5)
            stack.push_async_callback(battery_pool.stop)

            pv_pool = microgrid.new_pv_pool(priority=5)
            stack.push_async_callback(pv_pool.stop)

            grid = microgrid.grid()
            stack.push_async_callback(grid.stop)

            grid_meter_recv = get_resampled_stream(
                grid._formula_pool._namespace,  # pylint: disable=protected-access
                mockgrid.meter_ids[0],
                ComponentMetricId.ACTIVE_POWER,
                Power.from_watts,
            )
            grid_power_recv = grid.power.new_receiver()
            battery_power_recv = battery_pool.power.new_receiver()
            pv_power_recv = pv_pool.power.new_receiver()

            engine = (pv_pool.power + battery_pool.power).build("inv_power")
            stack.push_async_callback(engine._stop)  # pylint: disable=protected-access

            inv_calc_recv = engine.new_receiver()

            await mockgrid.mock_resampler.send_bat_inverter_power([10.0, 12.0, 14.0])
            await mockgrid.mock_resampler.send_meter_power(
                [100.0, 10.0, 12.0, 14.0, -100.0, -200.0]
            )
            await mockgrid.mock_resampler.send_pv_inverter_power([-100.0, -200.0])

            grid_pow = await grid_power_recv.receive()
            pv_pow = await pv_power_recv.receive()
            bat_pow = await battery_power_recv.receive()
            main_pow = await grid_meter_recv.receive()
            inv_calc_pow = await inv_calc_recv.receive()

            assert (
                grid_pow is not None
                and grid_pow.value is not None
                and math.isclose(grid_pow.value.base_value, -164.0)
            )  # 100 + 10 + 12 + 14 + -100 + -200
            assert (
                bat_pow is not None
                and bat_pow.value is not None
                and math.isclose(bat_pow.value.base_value, 36.0)
            )  # 10 + 12 + 14
            assert (
                pv_pow is not None
                and pv_pow.value is not None
                and math.isclose(pv_pow.value.base_value, -300.0)
            )  # -100 + -200
            assert (
                inv_calc_pow is not None
                and inv_calc_pow.value is not None
                and math.isclose(inv_calc_pow.value.base_value, -264.0)  # -300 + 36
            )
            assert (
                main_pow is not None
                and main_pow.value is not None
                and math.isclose(main_pow.value.base_value, 100.0)
            )

            assert math.isclose(
                inv_calc_pow.value.base_value,
                pv_pow.value.base_value + bat_pow.value.base_value,
            )
            assert math.isclose(
                grid_pow.value.base_value,
                inv_calc_pow.value.base_value + main_pow.value.base_value,
            )

    async def test_formula_composition_missing_pv(self, mocker: MockerFixture) -> None:
        """Test the composition of formulas with missing PV power data."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_batteries(3)

        count = 0
        async with mockgrid, AsyncExitStack() as stack:
            battery_pool = microgrid.new_battery_pool(priority=5)
            stack.push_async_callback(battery_pool.stop)

            pv_pool = microgrid.new_pv_pool(priority=5)
            stack.push_async_callback(pv_pool.stop)

            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)

            battery_power_recv = battery_pool.power.new_receiver()
            pv_power_recv = pv_pool.power.new_receiver()
            engine = (pv_pool.power + battery_pool.power).build("inv_power")
            stack.push_async_callback(engine._stop)  # pylint: disable=protected-access

            inv_calc_recv = engine.new_receiver()

            for _ in range(10):
                await mockgrid.mock_resampler.send_bat_inverter_power(
                    [10.0 + count, 12.0 + count, 14.0 + count]
                )
                await mockgrid.mock_resampler.send_non_existing_component_value()

                bat_pow = await battery_power_recv.receive()
                pv_pow = await pv_power_recv.receive()
                inv_pow = await inv_calc_recv.receive()

                assert inv_pow == bat_pow
                assert (
                    pv_pow.timestamp == inv_pow.timestamp
                    and pv_pow.value == Power.from_watts(0.0)
                )
                count += 1

        assert count == 10

    async def test_formula_composition_missing_bat(self, mocker: MockerFixture) -> None:
        """Test the composition of formulas with missing battery power data."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_solar_inverters(2, no_meter=True)

        count = 0
        async with mockgrid, AsyncExitStack() as stack:
            battery_pool = microgrid.new_battery_pool(priority=5)
            stack.push_async_callback(battery_pool.stop)

            pv_pool = microgrid.new_pv_pool(priority=5)
            stack.push_async_callback(pv_pool.stop)

            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)

            battery_power_recv = battery_pool.power.new_receiver()
            pv_power_recv = pv_pool.power.new_receiver()
            engine = (pv_pool.power + battery_pool.power).build("inv_power")
            stack.push_async_callback(engine._stop)  # pylint: disable=protected-access

            inv_calc_recv = engine.new_receiver()

            for _ in range(10):
                await mockgrid.mock_resampler.send_pv_inverter_power(
                    [12.0 + count, 14.0 + count]
                )
                await mockgrid.mock_resampler.send_non_existing_component_value()
                bat_pow = await battery_power_recv.receive()
                pv_pow = await pv_power_recv.receive()
                inv_pow = await inv_calc_recv.receive()

                assert inv_pow == pv_pow
                assert (
                    bat_pow.timestamp == inv_pow.timestamp
                    and bat_pow.value == Power.from_watts(0.0)
                )
                count += 1

        assert count == 10

    async def test_formula_composition_min_max(self, mocker: MockerFixture) -> None:
        """Test the composition of formulas with the min and max."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_chps(1)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)

            grid = microgrid.grid()
            stack.push_async_callback(grid.stop)

            engine_min = grid.power.min(logical_meter.chp_power).build("grid_power_min")
            stack.push_async_callback(
                engine_min._stop  # pylint: disable=protected-access
            )
            engine_min_rx = engine_min.new_receiver()

            engine_max = grid.power.max(logical_meter.chp_power).build("grid_power_max")
            stack.push_async_callback(
                engine_max._stop  # pylint: disable=protected-access
            )
            engine_max_rx = engine_max.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([100.0, 200.0])

            # Test min
            min_pow = await engine_min_rx.receive()
            assert (
                min_pow
                and min_pow.value
                and min_pow.value.isclose(Power.from_watts(100.0))
            )

            # Test max
            max_pow = await engine_max_rx.receive()
            assert (
                max_pow
                and max_pow.value
                and max_pow.value.isclose(Power.from_watts(200.0))
            )

            await mockgrid.mock_resampler.send_meter_power([-100.0, -200.0])

            # Test min
            min_pow = await engine_min_rx.receive()
            assert (
                min_pow
                and min_pow.value
                and min_pow.value.isclose(Power.from_watts(-200.0))
            )

            # Test max
            max_pow = await engine_max_rx.receive()
            assert (
                max_pow
                and max_pow.value
                and max_pow.value.isclose(Power.from_watts(-100.0))
            )

    async def test_formula_composition_min_max_const(
        self, mocker: MockerFixture
    ) -> None:
        """Test the compositing formulas and constants with the min and max functions."""
        async with (
            MockMicrogrid(grid_meter=True, mocker=mocker) as mockgrid,
            AsyncExitStack() as stack,
        ):
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)

            grid = microgrid.grid()
            stack.push_async_callback(grid.stop)

            engine_min = grid.power.min(Power.zero()).build("grid_power_min")
            stack.push_async_callback(
                engine_min._stop  # pylint: disable=protected-access
            )
            engine_min_rx = engine_min.new_receiver()

            engine_max = grid.power.max(Power.zero()).build("grid_power_max")
            stack.push_async_callback(
                engine_max._stop  # pylint: disable=protected-access
            )
            engine_max_rx = engine_max.new_receiver()

            await mockgrid.mock_resampler.send_meter_power([100.0])

            # Test min
            min_pow = await engine_min_rx.receive()
            assert min_pow and min_pow.value and min_pow.value.isclose(Power.zero())

            # Test max
            max_pow = await engine_max_rx.receive()
            assert (
                max_pow
                and max_pow.value
                and max_pow.value.isclose(Power.from_watts(100.0))
            )

            await mockgrid.mock_resampler.send_meter_power([-100.0])

            # Test min
            min_pow = await engine_min_rx.receive()
            assert (
                min_pow
                and min_pow.value
                and min_pow.value.isclose(Power.from_watts(-100.0))
            )

            # Test max
            max_pow = await engine_max_rx.receive()
            assert max_pow and max_pow.value and max_pow.value.isclose(Power.zero())

    async def test_formula_composition_constant(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the composition of formulas with constant values."""
        async with (
            MockMicrogrid(grid_meter=True, mocker=mocker) as mockgrid,
            AsyncExitStack() as stack,
        ):
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)
            grid = microgrid.grid()
            stack.push_async_callback(grid.stop)
            engine_add = (grid.power + Power.from_watts(50)).build(
                "grid_power_addition"
            )
            stack.push_async_callback(
                engine_add._stop  # pylint: disable=protected-access
            )
            engine_sub = (grid.power - Power.from_watts(100)).build(
                "grid_power_subtraction"
            )
            stack.push_async_callback(
                engine_sub._stop  # pylint: disable=protected-access
            )
            engine_mul = (grid.power * 2.0).build("grid_power_multiplication")
            stack.push_async_callback(
                engine_mul._stop  # pylint: disable=protected-access
            )
            engine_div = (grid.power / 2.0).build("grid_power_division")
            stack.push_async_callback(
                engine_div._stop  # pylint: disable=protected-access
            )

            engine_composite = (
                (
                    (grid.power + Power.from_watts(50.0)) / 2.0
                    + grid.power
                    - Power.from_watts(20.0)
                )
                * 2.0
            ).build("grid_power_composite")

            await mockgrid.mock_resampler.send_meter_power([100.0])

            # Test addition
            grid_power_addition: Sample[Power] = (
                await engine_add.new_receiver().receive()
            )
            assert grid_power_addition.value is not None
            assert math.isclose(
                grid_power_addition.value.as_watts(),
                150.0,
            )

            # Test subtraction
            grid_power_subtraction: Sample[Power] = (
                await engine_sub.new_receiver().receive()
            )
            assert grid_power_subtraction.value is not None
            assert math.isclose(
                grid_power_subtraction.value.as_watts(),
                0.0,
            )

            # Test multiplication
            grid_power_multiplication: Sample[Power] = (
                await engine_mul.new_receiver().receive()
            )
            assert grid_power_multiplication.value is not None
            assert math.isclose(
                grid_power_multiplication.value.as_watts(),
                200.0,
            )

            # Test division
            grid_power_division: Sample[Power] = (
                await engine_div.new_receiver().receive()
            )
            assert grid_power_division.value is not None
            assert math.isclose(
                grid_power_division.value.as_watts(),
                50.0,
            )

            # Test composite formula
            grid_power_composite: Sample[Power] = (
                await engine_composite.new_receiver().receive()
            )
            assert grid_power_composite.value is not None
            assert math.isclose(grid_power_composite.value.as_watts(), 310.0)

            # Test multiplication with a Quantity
            with pytest.raises(RuntimeError):
                engine_assert = (grid.power * Power.from_watts(2.0)).build(  # type: ignore
                    "grid_power_multiplication"
                )
                await engine_assert.new_receiver().receive()

            # Test addition with a float
            with pytest.raises(RuntimeError):
                engine_assert = (grid.power + 2.0).build(  # type: ignore
                    "grid_power_multiplication"
                )
                await engine_assert.new_receiver().receive()

    async def test_3_phase_formulas(self, mocker: MockerFixture) -> None:
        """Test 3 phase formulas current formulas and their composition."""
        mockgrid = MockMicrogrid(
            grid_meter=False, sample_rate_s=0.05, num_namespaces=2, mocker=mocker
        )
        mockgrid.add_batteries(3)
        mockgrid.add_ev_chargers(1)

        count = 0
        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)

            ev_pool = microgrid.new_ev_charger_pool(priority=5)
            stack.push_async_callback(ev_pool.stop)

            grid = microgrid.grid()
            stack.push_async_callback(grid.stop)

            grid_current_recv = grid.current_per_phase.new_receiver()
            ev_current_recv = ev_pool.current_per_phase.new_receiver()

            engine = (grid.current_per_phase - ev_pool.current_per_phase).build(
                "net_current"
            )
            stack.push_async_callback(engine._stop)  # pylint: disable=protected-access
            net_current_recv = engine.new_receiver()

            for _ in range(10):
                await mockgrid.mock_resampler.send_meter_current(
                    [
                        [10.0, 12.0, 14.0],
                        [10.0, 12.0, 14.0],
                        [10.0, 12.0, 14.0],
                    ]
                )
                await mockgrid.mock_resampler.send_evc_current(
                    [[10.0 + count, 12.0 + count, 14.0 + count]]
                )

                grid_amps = await grid_current_recv.receive()
                ev_amps = await ev_current_recv.receive()
                net_amps = await net_current_recv.receive()

                assert (
                    grid_amps.value_p1 is not None
                    and grid_amps.value_p1.base_value > 0.0
                )
                assert (
                    grid_amps.value_p2 is not None
                    and grid_amps.value_p2.base_value > 0.0
                )
                assert (
                    grid_amps.value_p3 is not None
                    and grid_amps.value_p3.base_value > 0.0
                )
                assert (
                    ev_amps.value_p1 is not None and ev_amps.value_p1.base_value > 0.0
                )
                assert (
                    ev_amps.value_p2 is not None and ev_amps.value_p2.base_value > 0.0
                )
                assert (
                    ev_amps.value_p3 is not None and ev_amps.value_p3.base_value > 0.0
                )
                assert (
                    net_amps.value_p1 is not None and net_amps.value_p1.base_value > 0.0
                )
                assert (
                    net_amps.value_p2 is not None and net_amps.value_p2.base_value > 0.0
                )
                assert (
                    net_amps.value_p3 is not None and net_amps.value_p3.base_value > 0.0
                )

                assert (
                    net_amps.value_p1.base_value
                    == grid_amps.value_p1.base_value - ev_amps.value_p1.base_value
                )
                assert (
                    net_amps.value_p2.base_value
                    == grid_amps.value_p2.base_value - ev_amps.value_p2.base_value
                )
                assert (
                    net_amps.value_p3.base_value
                    == grid_amps.value_p3.base_value - ev_amps.value_p3.base_value
                )
                count += 1

        assert count == 10
