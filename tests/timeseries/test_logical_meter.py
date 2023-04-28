# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from frequenz.channels import Broadcast, Receiver, Sender
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import Sample

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
            logical_meter._namespace,  # pylint: disable=protected-access
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
                logical_meter._namespace,  # pylint: disable=protected-access
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

    async def test_grid_production_consumption_power(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the grid production and consumption power formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        await mockgrid.start(mocker)

        channels: dict[int, Broadcast[Sample]] = {
            meter_id: Broadcast(f"#{meter_id}") for meter_id in mockgrid.meter_ids
        }
        senders: list[Sender[Sample]] = [
            channels[meter_id].new_sender() for meter_id in mockgrid.meter_ids
        ]

        async def send_resampled_data(
            now: datetime,
            meter_data: tuple[float | None, float | None, float | None, float | None],
        ) -> None:
            """Send resampled data to the channels."""
            for sender, value in zip(senders, meter_data):
                await sender.send(Sample(now, value))

        def mock_resampled_receiver(
            _1: Any, component_id: int, _2: ComponentMetricId
        ) -> Receiver[Sample]:
            return channels[component_id].new_receiver()

        mocker.patch(
            "frequenz.sdk.timeseries._formula_engine._resampled_formula_builder"
            ".ResampledFormulaBuilder._get_resampled_receiver",
            mock_resampled_receiver,
        )

        logical_meter = microgrid.logical_meter()
        grid_recv = logical_meter.grid_power.new_receiver()
        grid_production_recv = logical_meter.grid_production_power.new_receiver()
        grid_consumption_recv = logical_meter.grid_consumption_power.new_receiver()

        now = datetime.now()
        await send_resampled_data(now, (1.0, 2.0, 3.0, 4.0))
        assert (await grid_recv.receive()).value == 10.0
        assert (await grid_production_recv.receive()).value == 0.0
        assert (await grid_consumption_recv.receive()).value == 10.0

        await send_resampled_data(now, (1.0, 2.0, -3.0, -4.0))
        assert (await grid_recv.receive()).value == -4.0
        assert (await grid_production_recv.receive()).value == 4.0
        assert (await grid_consumption_recv.receive()).value == 0.0

    async def test_battery_and_pv_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the battery power and pv power formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(3)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)
        battery_pool = microgrid.battery_pool()
        logical_meter = microgrid.logical_meter()

        battery_power_recv = battery_pool.power.new_receiver()
        pv_power_recv = logical_meter.pv_power.new_receiver()

        bat_inv_receivers = [
            get_resampled_stream(
                battery_pool._namespace,  # pylint: disable=protected-access
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
            )
            for meter_id in mockgrid.battery_inverter_ids
        ]

        pv_inv_receivers = [
            get_resampled_stream(
                logical_meter._namespace,  # pylint: disable=protected-access
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

    async def test_chp_power(self, mocker: MockerFixture) -> None:
        """Test the chp power formula."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_chps(1)
        mockgrid.add_batteries(2)
        await mockgrid.start(mocker)

        assert len(mockgrid.meter_ids) == 4

        channels: dict[int, Broadcast[Sample]] = {
            meter_id: Broadcast(f"#{meter_id}") for meter_id in [*mockgrid.meter_ids]
        }
        senders: list[Sender[Sample]] = [
            channels[component_id].new_sender() for component_id in mockgrid.meter_ids
        ]

        async def send_resampled_data(
            now: datetime,
            meter_data: list[float | None],
        ) -> None:
            """Send resampled data to the channels."""
            for sender, value in zip(senders, meter_data):
                await sender.send(Sample(now, value))

        def mock_resampled_receiver(
            _1: Any, component_id: int, _2: ComponentMetricId
        ) -> Receiver[Sample]:
            return channels[component_id].new_receiver()

        mocker.patch(
            "frequenz.sdk.timeseries._formula_engine._resampled_formula_builder"
            ".ResampledFormulaBuilder._get_resampled_receiver",
            mock_resampled_receiver,
        )

        logical_meter = microgrid.logical_meter()
        chp_power_receiver = logical_meter.chp_power.new_receiver()
        chp_production_power_receiver = (
            logical_meter.chp_production_power.new_receiver()
        )
        chp_consumption_power_receiver = (
            logical_meter.chp_consumption_power.new_receiver()
        )

        now = datetime.now(tz=timezone.utc)
        await send_resampled_data(now, [1.0, 2.0, 3.0, 4.0])
        assert (await chp_power_receiver.receive()).value == 2.0
        assert (await chp_production_power_receiver.receive()).value == 0.0
        assert (await chp_consumption_power_receiver.receive()).value == 2.0

        await send_resampled_data(now, [-4.0, -12.0, None, 10.2])
        assert (await chp_power_receiver.receive()).value == -12.0
        assert (await chp_production_power_receiver.receive()).value == 12.0
        assert (await chp_consumption_power_receiver.receive()).value == 0.0

    async def test_pv_power(self, mocker: MockerFixture) -> None:
        """Test the pv power formula."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)

        assert len(mockgrid.pv_inverter_ids) == 2

        channels: dict[int, Broadcast[Sample]] = {
            inv_id: Broadcast(f"#{inv_id}") for inv_id in [*mockgrid.pv_inverter_ids]
        }
        senders: list[Sender[Sample]] = [
            channels[component_id].new_sender()
            for component_id in mockgrid.pv_inverter_ids
        ]

        async def send_resampled_data(
            now: datetime,
            meter_data: list[float | None],
        ) -> None:
            """Send resampled data to the channels."""
            for sender, value in zip(senders, meter_data):
                await sender.send(Sample(now, value))

        def mock_resampled_receiver(
            _1: Any, component_id: int, _2: ComponentMetricId
        ) -> Receiver[Sample]:
            return channels[component_id].new_receiver()

        mocker.patch(
            "frequenz.sdk.timeseries._formula_engine._resampled_formula_builder"
            ".ResampledFormulaBuilder._get_resampled_receiver",
            mock_resampled_receiver,
        )

        logical_meter = microgrid.logical_meter()
        pv_power_receiver = logical_meter.pv_power.new_receiver()
        pv_production_power_receiver = logical_meter.pv_production_power.new_receiver()
        pv_consumption_power_receiver = (
            logical_meter.pv_consumption_power.new_receiver()
        )

        now = datetime.now(tz=timezone.utc)
        await send_resampled_data(now, [-1.0, -2.0])
        assert (await pv_power_receiver.receive()).value == -3.0
        assert (await pv_production_power_receiver.receive()).value == 3.0
        assert (await pv_consumption_power_receiver.receive()).value == 0.0

    async def test_consumer_power_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the consumer power formula with a grid meter."""
        mockgrid = MockMicrogrid(grid_side_meter=True)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)

        assert len(mockgrid.meter_ids) == 5

        channels: dict[int, Broadcast[Sample]] = {
            meter_id: Broadcast(f"#{meter_id}") for meter_id in [*mockgrid.meter_ids]
        }
        senders: list[Sender[Sample]] = [
            channels[component_id].new_sender() for component_id in mockgrid.meter_ids
        ]

        async def send_resampled_data(
            now: datetime,
            meter_data: list[float | None],
        ) -> None:
            """Send resampled data to the channels."""
            for sender, value in zip(senders, meter_data):
                await sender.send(Sample(now, value))

        def mock_resampled_receiver(
            _1: Any, component_id: int, _2: ComponentMetricId
        ) -> Receiver[Sample]:
            return channels[component_id].new_receiver()

        mocker.patch(
            "frequenz.sdk.timeseries._formula_engine._resampled_formula_builder"
            ".ResampledFormulaBuilder._get_resampled_receiver",
            mock_resampled_receiver,
        )

        logical_meter = microgrid.logical_meter()
        consumer_power_receiver = logical_meter.consumer_power.new_receiver()

        now = datetime.now(tz=timezone.utc)
        await send_resampled_data(now, [20.0, 2.0, 3.0, 4.0, 5.0])
        assert (await consumer_power_receiver.receive()).value == 6.0

    async def test_consumer_power_no_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the consumer power formula without a grid meter."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)

        assert len(mockgrid.meter_ids) == 5

        channels: dict[int, Broadcast[Sample]] = {
            meter_id: Broadcast(f"#{meter_id}") for meter_id in [*mockgrid.meter_ids]
        }
        senders: list[Sender[Sample]] = [
            channels[component_id].new_sender() for component_id in mockgrid.meter_ids
        ]

        async def send_resampled_data(
            now: datetime,
            meter_data: list[float | None],
        ) -> None:
            """Send resampled data to the channels."""
            for sender, value in zip(senders, meter_data):
                await sender.send(Sample(now, value))

        def mock_resampled_receiver(
            _1: Any, component_id: int, _2: ComponentMetricId
        ) -> Receiver[Sample]:
            return channels[component_id].new_receiver()

        mocker.patch(
            "frequenz.sdk.timeseries._formula_engine._resampled_formula_builder"
            ".ResampledFormulaBuilder._get_resampled_receiver",
            mock_resampled_receiver,
        )

        logical_meter = microgrid.logical_meter()
        consumer_power_receiver = logical_meter.consumer_power.new_receiver()

        now = datetime.now(tz=timezone.utc)
        await send_resampled_data(now, [20.0, 2.0, 3.0, 4.0, 5.0])
        assert (await consumer_power_receiver.receive()).value == 20.0
