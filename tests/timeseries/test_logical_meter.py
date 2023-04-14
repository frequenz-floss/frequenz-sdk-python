# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""

from __future__ import annotations

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import ComponentMetricId

from ._formula_engine.utils import (
    equal_float_lists,
    get_resampled_stream,
    synchronize_receivers,
)
from .mock_microgrid import MockMicrogrid

# pylint: disable=too-many-locals


class TestLogicalMeter:
    """Tests for the logical meter."""

    async def test_grid_power_1(self, mocker: MockerFixture) -> None:
        """Test the grid power formula with a grid side meter."""
        mockgrid = MockMicrogrid(grid_side_meter=True)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        await mockgrid.start(mocker)
        logical_meter = microgrid.logical_meter()

        grid_power_recv = logical_meter.grid_power.new_receiver()

        main_meter_recv = get_resampled_stream(
            mockgrid.main_meter_id,
            ComponentMetricId.ACTIVE_POWER,
        )

        await synchronize_receivers([grid_power_recv, main_meter_recv])
        results = []
        main_meter_data = []
        for _ in range(10):
            val = await main_meter_recv.receive()
            assert val is not None and val.value is not None and val.value > 0.0
            main_meter_data.append(val.value)

            val = await grid_power_recv.receive()
            assert val is not None and val.value is not None
            results.append(val.value)
        await mockgrid.cleanup()
        assert equal_float_lists(results, main_meter_data)

    async def test_grid_power_2(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the grid power formula without a grid side meter."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        await mockgrid.start(mocker)
        logical_meter = microgrid.logical_meter()

        grid_power_recv = logical_meter.grid_power.new_receiver()

        meter_receivers = [
            get_resampled_stream(
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for meter_id in mockgrid.meter_ids
        ]

        await synchronize_receivers([grid_power_recv, *meter_receivers])

        results = []
        meter_sums = []
        for _ in range(10):
            meter_sum = 0.0
            for recv in meter_receivers:
                val = await recv.receive()
                assert val is not None and val.value is not None and val.value > 0.0
                meter_sum += val.value

            val = await grid_power_recv.receive()
            assert val is not None and val.value is not None
            results.append(val.value)
            meter_sums.append(meter_sum)

        await mockgrid.cleanup()

        assert len(results) == 10
        assert equal_float_lists(results, meter_sums)

    async def test_battery_and_pv_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the battery power and pv power formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)
        logical_meter = microgrid.logical_meter()

        battery_power_recv = logical_meter.battery_power.new_receiver()
        pv_power_recv = logical_meter.pv_power.new_receiver()

        bat_inv_receivers = [
            get_resampled_stream(
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for meter_id in mockgrid.battery_inverter_ids
        ]

        pv_inv_receivers = [
            get_resampled_stream(
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for meter_id in mockgrid.pv_inverter_ids
        ]

        await synchronize_receivers(
            [battery_power_recv, pv_power_recv, *bat_inv_receivers, *pv_inv_receivers]
        )

        battery_results = []
        pv_results = []
        battery_inv_sums = []
        pv_inv_sums = []
        for _ in range(10):
            bat_inv_sum = 0.0
            pv_inv_sum = 0.0
            for recv in bat_inv_receivers:
                val = await recv.receive()
                assert val is not None and val.value is not None and val.value > 0.0
                bat_inv_sum += val.value
            battery_inv_sums.append(bat_inv_sum)

            for recv in pv_inv_receivers:
                val = await recv.receive()
                assert val is not None and val.value is not None and val.value > 0.0
                pv_inv_sum += val.value
            pv_inv_sums.append(pv_inv_sum)

            val = await battery_power_recv.receive()
            assert val is not None and val.value is not None
            battery_results.append(val.value)

            val = await pv_power_recv.receive()
            assert val is not None and val.value is not None
            pv_results.append(val.value)

        await mockgrid.cleanup()

        assert len(battery_results) == 10
        assert equal_float_lists(battery_results, battery_inv_sums)
        assert len(pv_results) == 10
        assert equal_float_lists(pv_results, pv_inv_sums)
