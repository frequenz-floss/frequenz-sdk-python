# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for formula composition."""


from math import isclose

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import ComponentMetricId

from ..mock_microgrid import MockMicrogrid
from .utils import get_resampled_stream, synchronize_receivers


class TestFormulaComposition:
    """Tests for formula composition."""

    async def test_formula_composition(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the composition of formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False, sample_rate_s=0.15)
        mockgrid.add_batteries(3)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)
        logical_meter = microgrid.logical_meter()

        main_meter_recv = get_resampled_stream(
            4,
            ComponentMetricId.ACTIVE_POWER,
        )
        grid_power_recv = logical_meter.grid_power.new_receiver()
        battery_power_recv = logical_meter.battery_power.new_receiver()
        pv_power_recv = logical_meter.pv_power.new_receiver()

        engine = (logical_meter.pv_power + logical_meter.battery_power).build(
            "inv_power"
        )
        inv_calc_recv = engine.new_receiver()

        count = 0
        for _ in range(10):
            grid_pow = await grid_power_recv.receive()
            pv_pow = await pv_power_recv.receive()
            bat_pow = await battery_power_recv.receive()
            main_pow = await main_meter_recv.receive()
            inv_calc_pow = await inv_calc_recv.receive()

            assert grid_pow is not None and grid_pow.value is not None
            assert inv_calc_pow is not None and inv_calc_pow.value is not None
            assert bat_pow is not None and bat_pow.value is not None
            assert pv_pow is not None and pv_pow.value is not None
            assert main_pow is not None and main_pow.value is not None
            assert isclose(inv_calc_pow.value, pv_pow.value + bat_pow.value)
            assert isclose(grid_pow.value, inv_calc_pow.value + main_pow.value)
            count += 1

        await mockgrid.cleanup()
        await engine._stop()  # pylint: disable=protected-access

        assert count == 10

    async def test_formula_composition_missing_pv(self, mocker: MockerFixture) -> None:
        """Test the composition of formulas with missing PV power data."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        await mockgrid.start(mocker)
        logical_meter = microgrid.logical_meter()

        battery_power_recv = logical_meter.battery_power.new_receiver()
        pv_power_recv = logical_meter.pv_power.new_receiver()
        engine = (logical_meter.pv_power + logical_meter.battery_power).build(
            "inv_power"
        )
        inv_calc_recv = engine.new_receiver()

        count = 0
        for _ in range(10):
            bat_pow = await battery_power_recv.receive()
            pv_pow = await pv_power_recv.receive()
            inv_pow = await inv_calc_recv.receive()

            assert inv_pow == bat_pow
            assert pv_pow.timestamp == inv_pow.timestamp and pv_pow.value == 0.0
            count += 1

        await mockgrid.cleanup()
        await engine._stop()  # pylint: disable=protected-access

        assert count == 10

    async def test_formula_composition_missing_bat(self, mocker: MockerFixture) -> None:
        """Test the composition of formulas with missing battery power data."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)
        logical_meter = microgrid.logical_meter()

        battery_power_recv = logical_meter.battery_power.new_receiver()
        pv_power_recv = logical_meter.pv_power.new_receiver()
        engine = (logical_meter.pv_power + logical_meter.battery_power).build(
            "inv_power"
        )
        inv_calc_recv = engine.new_receiver()

        count = 0
        for _ in range(10):
            bat_pow = await battery_power_recv.receive()
            pv_pow = await pv_power_recv.receive()
            inv_pow = await inv_calc_recv.receive()

            assert inv_pow == pv_pow
            assert bat_pow.timestamp == inv_pow.timestamp and bat_pow.value == 0.0
            count += 1

        await mockgrid.cleanup()
        await engine._stop()  # pylint: disable=protected-access

        assert count == 10

    async def test_3_phase_formulas(self, mocker: MockerFixture) -> None:
        """Test 3 phase formulas current formulas and their composition."""
        mockgrid = MockMicrogrid(grid_side_meter=False, sample_rate_s=0.05)
        mockgrid.add_batteries(3)
        mockgrid.add_ev_chargers(1)
        await mockgrid.start(mocker)
        logical_meter = microgrid.logical_meter()
        ev_pool = microgrid.ev_charger_pool()

        grid_current_recv = logical_meter.grid_current.new_receiver()
        ev_current_recv = ev_pool.current.new_receiver()

        engine = (logical_meter.grid_current - ev_pool.current).build("net_current")
        net_current_recv = engine.new_receiver()

        await synchronize_receivers(
            [grid_current_recv, ev_current_recv, net_current_recv]
        )

        for _ in range(10):
            grid_amps = await grid_current_recv.receive()
            ev_amps = await ev_current_recv.receive()
            net_amps = await net_current_recv.receive()

            assert grid_amps.value_p1 is not None and grid_amps.value_p1 > 0.0
            assert grid_amps.value_p2 is not None and grid_amps.value_p2 > 0.0
            assert grid_amps.value_p3 is not None and grid_amps.value_p3 > 0.0
            assert ev_amps.value_p1 is not None and ev_amps.value_p1 > 0.0
            assert ev_amps.value_p2 is not None and ev_amps.value_p2 > 0.0
            assert ev_amps.value_p3 is not None and ev_amps.value_p3 > 0.0
            assert net_amps.value_p1 is not None and net_amps.value_p1 > 0.0
            assert net_amps.value_p2 is not None and net_amps.value_p2 > 0.0
            assert net_amps.value_p3 is not None and net_amps.value_p3 > 0.0

            assert net_amps.value_p1 == grid_amps.value_p1 - ev_amps.value_p1
            assert net_amps.value_p2 == grid_amps.value_p2 - ev_amps.value_p2
            assert net_amps.value_p3 == grid_amps.value_p3 - ev_amps.value_p3

        await mockgrid.cleanup()
        await engine._stop()  # pylint: disable=protected-access
