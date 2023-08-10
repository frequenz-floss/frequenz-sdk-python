# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for formula composition."""


import math

import pytest
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries._quantities import Power

from ..mock_microgrid import MockMicrogrid
from .utils import get_resampled_stream


class TestFormulaComposition:
    """Tests for formula composition."""

    async def test_formula_composition(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the composition of formulas."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_consumer_meters()
        mockgrid.add_batteries(3)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)

        logical_meter = microgrid.logical_meter()
        battery_pool = microgrid.battery_pool()
        grid_meter_recv = get_resampled_stream(
            logical_meter._namespace,  # pylint: disable=protected-access
            mockgrid.meter_ids[0],
            ComponentMetricId.ACTIVE_POWER,
            Power.from_watts,
        )
        grid_power_recv = logical_meter.grid_power.new_receiver()
        battery_power_recv = battery_pool.power.new_receiver()
        pv_power_recv = logical_meter.pv_power.new_receiver()

        engine = (logical_meter.pv_power + battery_pool.power).build("inv_power")
        inv_calc_recv = engine.new_receiver()

        await mockgrid.mock_resampler.send_bat_inverter_power([10.0, 12.0, 14.0])
        await mockgrid.mock_resampler.send_meter_power(
            [100.0, 10.0, 12.0, 14.0, -100.0, -200.0]
        )

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

        await mockgrid.cleanup()
        await engine._stop()  # pylint: disable=protected-access
        await battery_pool.stop()
        await logical_meter.stop()

    async def test_formula_composition_missing_pv(self, mocker: MockerFixture) -> None:
        """Test the composition of formulas with missing PV power data."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        battery_pool = microgrid.battery_pool()
        logical_meter = microgrid.logical_meter()

        battery_power_recv = battery_pool.power.new_receiver()
        pv_power_recv = logical_meter.pv_power.new_receiver()
        engine = (logical_meter.pv_power + battery_pool.power).build("inv_power")
        inv_calc_recv = engine.new_receiver()

        count = 0
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

        await mockgrid.cleanup()
        await engine._stop()  # pylint: disable=protected-access
        await battery_pool.stop()
        await logical_meter.stop()

        assert count == 10

    async def test_formula_composition_missing_bat(self, mocker: MockerFixture) -> None:
        """Test the composition of formulas with missing battery power data."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)
        battery_pool = microgrid.battery_pool()
        logical_meter = microgrid.logical_meter()

        battery_power_recv = battery_pool.power.new_receiver()
        pv_power_recv = logical_meter.pv_power.new_receiver()
        engine = (logical_meter.pv_power + battery_pool.power).build("inv_power")
        inv_calc_recv = engine.new_receiver()

        count = 0
        for _ in range(10):
            await mockgrid.mock_resampler.send_meter_power([12.0 + count, 14.0 + count])
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

        await mockgrid.cleanup()
        await engine._stop()  # pylint: disable=protected-access
        await battery_pool.stop()
        await logical_meter.stop()

        assert count == 10

    async def test_formula_composition_constant(self, mocker: MockerFixture) -> None:
        """Test the composition of formulas with constant values."""
        mockgrid = MockMicrogrid(grid_meter=True)
        await mockgrid.start(mocker)

        logical_meter = microgrid.logical_meter()
        engine_add = (logical_meter.grid_power + Power.from_watts(50)).build(
            "grid_power_addition"
        )
        engine_sub = (logical_meter.grid_power - Power.from_watts(100)).build(
            "grid_power_subtraction"
        )
        engine_mul = (logical_meter.grid_power * 2.0).build("grid_power_multiplication")
        engine_div = (logical_meter.grid_power / 2.0).build("grid_power_division")

        await mockgrid.mock_resampler.send_meter_power([100.0])

        # Test addition
        grid_power_addition = await engine_add.new_receiver().receive()
        assert grid_power_addition.value is not None
        assert math.isclose(
            grid_power_addition.value.as_watts(),
            150.0,
        )

        # Test subtraction
        grid_power_subtraction = await engine_sub.new_receiver().receive()
        assert grid_power_subtraction.value is not None
        assert math.isclose(
            grid_power_subtraction.value.as_watts(),
            0.0,
        )

        # Test multiplication
        grid_power_multiplication = await engine_mul.new_receiver().receive()
        assert grid_power_multiplication.value is not None
        assert math.isclose(
            grid_power_multiplication.value.as_watts(),
            200.0,
        )

        # Test division
        grid_power_division = await engine_div.new_receiver().receive()
        assert grid_power_division.value is not None
        assert math.isclose(
            grid_power_division.value.as_watts(),
            50.0,
        )

        # Test multiplication with a Quantity
        with pytest.raises(RuntimeError):
            engine_assert = (
                logical_meter.grid_power * Power.from_watts(2.0)  # type: ignore
            ).build("grid_power_multiplication")
            await engine_assert.new_receiver().receive()

        # Test addition with a float
        with pytest.raises(RuntimeError):
            engine_assert = (logical_meter.grid_power + 2.0).build(  # type: ignore
                "grid_power_multiplication"
            )
            await engine_assert.new_receiver().receive()

        await engine_add._stop()  # pylint: disable=protected-access
        await engine_sub._stop()  # pylint: disable=protected-access
        await engine_mul._stop()  # pylint: disable=protected-access
        await engine_div._stop()  # pylint: disable=protected-access
        await mockgrid.cleanup()
        await logical_meter.stop()

    async def test_3_phase_formulas(self, mocker: MockerFixture) -> None:
        """Test 3 phase formulas current formulas and their composition."""
        mockgrid = MockMicrogrid(grid_meter=False, sample_rate_s=0.05, num_namespaces=2)
        mockgrid.add_batteries(3)
        mockgrid.add_ev_chargers(1)
        await mockgrid.start(mocker)
        logical_meter = microgrid.logical_meter()
        ev_pool = microgrid.ev_charger_pool()

        grid_current_recv = logical_meter.grid_current.new_receiver()
        ev_current_recv = ev_pool.current.new_receiver()

        engine = (logical_meter.grid_current - ev_pool.current).build("net_current")
        net_current_recv = engine.new_receiver()

        count = 0

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
                grid_amps.value_p1 is not None and grid_amps.value_p1.base_value > 0.0
            )
            assert (
                grid_amps.value_p2 is not None and grid_amps.value_p2.base_value > 0.0
            )
            assert (
                grid_amps.value_p3 is not None and grid_amps.value_p3.base_value > 0.0
            )
            assert ev_amps.value_p1 is not None and ev_amps.value_p1.base_value > 0.0
            assert ev_amps.value_p2 is not None and ev_amps.value_p2.base_value > 0.0
            assert ev_amps.value_p3 is not None and ev_amps.value_p3.base_value > 0.0
            assert net_amps.value_p1 is not None and net_amps.value_p1.base_value > 0.0
            assert net_amps.value_p2 is not None and net_amps.value_p2.base_value > 0.0
            assert net_amps.value_p3 is not None and net_amps.value_p3.base_value > 0.0

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

        await mockgrid.cleanup()
        await engine._stop()  # pylint: disable=protected-access
        await logical_meter.stop()
        await ev_pool.stop()

        assert count == 10
