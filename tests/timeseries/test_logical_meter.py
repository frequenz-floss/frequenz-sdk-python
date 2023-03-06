# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""

from __future__ import annotations

from math import isclose

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries.logical_meter import LogicalMeter

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
        request_chan, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_chan.new_sender(),
            microgrid.get().component_graph,
        )

        grid_power_recv = await logical_meter.grid_power()

        main_meter_recv = await get_resampled_stream(
            logical_meter,
            channel_registry,
            request_chan.new_sender(),
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
        request_chan, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_chan.new_sender(),
            microgrid.get().component_graph,
        )

        grid_power_recv = await logical_meter.grid_power()

        meter_receivers = [
            await get_resampled_stream(
                logical_meter,
                channel_registry,
                request_chan.new_sender(),
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
        request_chan, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_chan.new_sender(),
            microgrid.get().component_graph,
        )

        battery_power_recv = await logical_meter.battery_power()
        pv_power_recv = await logical_meter.pv_power()

        bat_inv_receivers = [
            await get_resampled_stream(
                logical_meter,
                channel_registry,
                request_chan.new_sender(),
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for meter_id in mockgrid.battery_inverter_ids
        ]

        pv_inv_receivers = [
            await get_resampled_stream(
                logical_meter,
                channel_registry,
                request_chan.new_sender(),
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

    async def test_soc(self, mocker: MockerFixture) -> None:
        """Test the soc calculation."""
        mockgrid = MockMicrogrid(grid_side_meter=False, sample_rate_s=0.02)
        mockgrid.add_solar_inverters(2)
        mockgrid._id_increment = 8  # pylint: disable=protected-access
        mockgrid.add_batteries(3)
        request_chan, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_chan.new_sender(),
            microgrid.get().component_graph,
        )

        soc_recv = await logical_meter._soc()  # pylint: disable=protected-access

        bat_receivers = {
            bat_id: await get_resampled_stream(
                logical_meter,
                channel_registry,
                request_chan.new_sender(),
                bat_id,
                ComponentMetricId.SOC,
            )
            for bat_id in mockgrid.battery_ids
        }

        bat_inv_map = mockgrid.bat_inv_map
        inv_receivers = {
            inverter_id: await get_resampled_stream(
                logical_meter,
                channel_registry,
                request_chan.new_sender(),
                inverter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for inverter_id in mockgrid.bat_inv_map.values()
        }

        await synchronize_receivers(
            [soc_recv, *bat_receivers.values(), *inv_receivers.values()]
        )

        for _ in range(10):
            bat_vals = []
            for bat_id, bat_recv in bat_receivers.items():
                inv_id = bat_inv_map[bat_id]
                inv_recv = inv_receivers[inv_id]
                inv_msg = await inv_recv.receive()
                # We won't use batteries where adjacent inverter is not sending active
                # power.
                if inv_msg.value is None:
                    continue

                val = await bat_recv.receive()
                assert val is not None and val.value is not None
                bat_vals.append(val.value)

            soc_sample = await soc_recv.receive()
            assert soc_sample is not None and soc_sample.value is not None

            assert isclose(soc_sample.value, sum(bat_vals) / len(bat_vals))

        await mockgrid.cleanup()
