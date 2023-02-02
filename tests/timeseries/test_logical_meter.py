# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""

from __future__ import annotations

from datetime import datetime
from math import isclose

from frequenz.channels import Receiver, Sender
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import ChannelRegistry, ComponentMetricRequest
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import Sample, Sample3Phase
from frequenz.sdk.timeseries.logical_meter import LogicalMeter
from frequenz.sdk.timeseries.logical_meter._formula_engine import (
    FormulaReceiver,
    FormulaReceiver3Phase,
)
from frequenz.sdk.timeseries.logical_meter._resampled_formula_builder import (
    ResampledFormulaBuilder,
)

from .mock_microgrid import MockMicrogrid


class TestLogicalMeter:
    """Tests for the logical meter."""

    async def _get_resampled_stream(  # pylint: disable=too-many-arguments
        self,
        logical_meter: LogicalMeter,
        channel_registry: ChannelRegistry,
        request_sender: Sender[ComponentMetricRequest],
        comp_id: int,
        metric_id: ComponentMetricId,
    ) -> Receiver[Sample]:
        """Return the resampled data stream for the given component."""
        # Create a `FormulaBuilder` instance, just in order to reuse its
        # `_get_resampled_receiver` function implementation.

        # pylint: disable=protected-access
        builder = ResampledFormulaBuilder(
            logical_meter._namespace,
            "",
            channel_registry,
            request_sender,
            metric_id,
        )
        return await builder._get_resampled_receiver(
            comp_id,
            metric_id,
        )
        # pylint: enable=protected-access

    def _equal_float_lists(self, list1: list[float], list2: list[float]) -> bool:
        return (
            len(list1) > 0
            and len(list1) == len(list2)
            and all(isclose(v1, v2) for v1, v2 in zip(list1, list2))
        )

    async def _synchronize_receivers(
        self,
        receivers: list[FormulaReceiver | FormulaReceiver3Phase | Receiver[Sample]],
    ) -> None:
        by_ts: dict[
            datetime, list[FormulaReceiver | FormulaReceiver3Phase | Receiver[Sample]]
        ] = {}
        for recv in receivers:
            while True:
                sample = await recv.receive()
                assert sample is not None
                if isinstance(sample, Sample) and sample.value is None:
                    continue
                if isinstance(sample, Sample3Phase) and sample.value_p1 is None:
                    continue
                by_ts.setdefault(sample.timestamp, []).append(recv)
                break
        latest_ts = max(by_ts)

        for sample_ts, recvs in by_ts.items():
            if sample_ts == latest_ts:
                continue
            while sample_ts < latest_ts:
                for recv in recvs:
                    val = await recv.receive()
                    assert val is not None
                    sample_ts = val.timestamp

    async def test_grid_power_1(self, mocker: MockerFixture) -> None:
        """Test the grid power formula with a grid side meter."""
        mockgrid = MockMicrogrid(grid_side_meter=True)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        grid_power_recv = await logical_meter.grid_power()

        main_meter_recv = await self._get_resampled_stream(
            logical_meter,
            channel_registry,
            request_sender,
            mockgrid.main_meter_id,
            ComponentMetricId.ACTIVE_POWER,
        )

        await self._synchronize_receivers([grid_power_recv, main_meter_recv])
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
        assert self._equal_float_lists(results, main_meter_data)

    async def test_grid_power_2(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the grid power formula without a grid side meter."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        grid_power_recv = await logical_meter.grid_power()

        meter_receivers = [
            await self._get_resampled_stream(
                logical_meter,
                channel_registry,
                request_sender,
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for meter_id in mockgrid.meter_ids
        ]

        await self._synchronize_receivers([grid_power_recv, *meter_receivers])

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
        assert self._equal_float_lists(results, meter_sums)

    async def test_battery_and_pv_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the battery power and pv power formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        mockgrid.add_solar_inverters(2)
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        battery_power_recv = await logical_meter.battery_power()
        pv_power_recv = await logical_meter.pv_power()

        bat_inv_receivers = [
            await self._get_resampled_stream(
                logical_meter,
                channel_registry,
                request_sender,
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for meter_id in mockgrid.battery_inverter_ids
        ]

        pv_inv_receivers = [
            await self._get_resampled_stream(
                logical_meter,
                channel_registry,
                request_sender,
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for meter_id in mockgrid.pv_inverter_ids
        ]

        await self._synchronize_receivers(
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
        assert self._equal_float_lists(battery_results, battery_inv_sums)
        assert len(pv_results) == 10
        assert self._equal_float_lists(pv_results, pv_inv_sums)

    async def test_soc(self, mocker: MockerFixture) -> None:
        """Test the soc calculation."""
        mockgrid = MockMicrogrid(grid_side_meter=False, sample_rate_s=0.02)
        mockgrid.add_solar_inverters(2)
        mockgrid._id_increment = 8  # pylint: disable=protected-access
        mockgrid.add_batteries(3)
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        soc_recv = await logical_meter._soc()  # pylint: disable=protected-access

        bat_receivers = [
            await self._get_resampled_stream(
                logical_meter,
                channel_registry,
                request_sender,
                bat_id,
                ComponentMetricId.SOC,
            )
            for bat_id in mockgrid.battery_ids
        ]

        await self._synchronize_receivers([soc_recv, *bat_receivers])

        for ctr in range(10):
            bat_vals = []
            for recv in bat_receivers:
                val = await recv.receive()
                assert val is not None and val.value is not None
                bat_vals.append(val.value)

            assert len(bat_vals) == 3
            # After 5 values, the inverter with component_id > 100 stops sending
            # data. And the values from the last battery goes out of the calculation.
            # So we drop it from out control value as well.
            if ctr >= 5:
                bat_vals = bat_vals[:2]

            soc_sample = await soc_recv.receive()
            assert soc_sample is not None and soc_sample.value is not None

            assert isclose(soc_sample.value, sum(bat_vals) / len(bat_vals))

        await mockgrid.cleanup()

    async def test_formula_composition(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the composition of formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False, sample_rate_s=0.05)
        mockgrid.add_batteries(3)
        mockgrid.add_solar_inverters(2)
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        main_meter_recv = await self._get_resampled_stream(
            logical_meter,
            channel_registry,
            request_sender,
            4,
            ComponentMetricId.ACTIVE_POWER,
        )
        grid_power_recv = await logical_meter.grid_power()
        battery_power_recv = await logical_meter.battery_power()
        pv_power_recv = await logical_meter.pv_power()

        engine = (pv_power_recv.clone() + battery_power_recv.clone()).build("inv_power")
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
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        battery_power_recv = await logical_meter.battery_power()
        pv_power_recv = await logical_meter.pv_power()
        engine = (pv_power_recv.clone() + battery_power_recv.clone()).build("inv_power")
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
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        battery_power_recv = await logical_meter.battery_power()
        pv_power_recv = await logical_meter.pv_power()
        engine = (pv_power_recv.clone() + battery_power_recv.clone()).build("inv_power")
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
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        mockgrid.add_ev_chargers(1)
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )
        grid_current_recv = await logical_meter.grid_current()
        ev_current_recv = await logical_meter.ev_charger_current()

        engine = (grid_current_recv.clone() - ev_current_recv.clone()).build(
            "net_current"
        )
        net_current_recv = engine.new_receiver()

        await self._synchronize_receivers(
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

    async def test_ev_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the battery power and pv power formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_ev_chargers(5)
        request_sender, channel_registry = await mockgrid.start(mocker)
        logical_meter = LogicalMeter(
            channel_registry,
            request_sender,
            microgrid.get().component_graph,
        )

        main_meter_recv = await self._get_resampled_stream(
            logical_meter,
            channel_registry,
            request_sender,
            mockgrid.main_meter_id,
            ComponentMetricId.ACTIVE_POWER,
        )
        grid_power_recv = await logical_meter.grid_power()
        ev_power_recv = await logical_meter.ev_charger_power()

        await self._synchronize_receivers(
            [grid_power_recv, main_meter_recv, ev_power_recv]
        )

        ev_results = []
        for _ in range(10):
            grid_pow = await grid_power_recv.receive()
            ev_pow = await ev_power_recv.receive()
            main_pow = await main_meter_recv.receive()

            assert grid_pow is not None and grid_pow.value is not None
            assert ev_pow is not None and ev_pow.value is not None
            assert main_pow is not None and main_pow.value is not None
            assert isclose(grid_pow.value, ev_pow.value + main_pow.value)

            ev_results.append(ev_pow.value)

        await mockgrid.cleanup()
        assert len(ev_results) == 10
