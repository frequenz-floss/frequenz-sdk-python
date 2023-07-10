# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""

from __future__ import annotations

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries._quantities import Power, Quantity

from ._formula_engine.utils import equal_float_lists, get_resampled_stream
from .mock_microgrid import MockMicrogrid

# pylint: disable=too-many-locals


class TestLogicalMeter:
    """Tests for the logical meter."""

    async def test_grid_active_power_1(self, mocker: MockerFixture) -> None:
        """Test the grid active_power formula with a grid side meter."""
        mockgrid = MockMicrogrid(grid_side_meter=True)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        await mockgrid.start_mock_datapipeline(mocker)
        logical_meter = microgrid.logical_meter()

        grid_active_power_recv = logical_meter.grid_active_power.new_receiver()

        main_meter_recv = get_resampled_stream(
            logical_meter._namespace,  # pylint: disable=protected-access
            mockgrid.main_meter_id,
            ComponentMetricId.ACTIVE_POWER,
            Power.from_watts,
        )

        results = []
        main_meter_data = []
        for count in range(10):
            await mockgrid.mock_data.send_meter_active_power(
                [20.0 + count, 12.0, -13.0, -5.0]
            )
            val = await main_meter_recv.receive()
            assert (
                val is not None
                and val.value is not None
                and val.value.as_watts() != 0.0
            )
            main_meter_data.append(val.value)

            val = await grid_active_power_recv.receive()
            assert val is not None and val.value is not None
            results.append(val.value)
        await mockgrid.cleanup()
        assert equal_float_lists(results, main_meter_data)

    async def test_grid_active_power_2(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the grid active_power formula without a grid side meter."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        await mockgrid.start_mock_datapipeline(mocker)
        logical_meter = microgrid.logical_meter()

        grid_active_power_recv = logical_meter.grid_active_power.new_receiver()

        meter_receivers = [
            get_resampled_stream(
                logical_meter._namespace,  # pylint: disable=protected-access
                meter_id,
                ComponentMetricId.ACTIVE_POWER,
                Power.from_watts,
            )
            for meter_id in mockgrid.meter_ids
        ]

        results: list[Quantity] = []
        meter_sums: list[Quantity] = []
        for count in range(10):
            await mockgrid.mock_data.send_meter_active_power(
                [20.0 + count, 12.0, -13.0, -5.0]
            )
            meter_sum = 0.0
            for recv in meter_receivers:
                val = await recv.receive()
                assert (
                    val is not None
                    and val.value is not None
                    and val.value.as_watts() != 0.0
                )
                meter_sum += val.value.as_watts()

            val = await grid_active_power_recv.receive()
            assert val is not None and val.value is not None
            results.append(val.value)
            meter_sums.append(Quantity(meter_sum))

        await mockgrid.cleanup()

        assert len(results) == 10
        assert equal_float_lists(results, meter_sums)

    async def test_grid_production_consumption_active_power(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the grid production and consumption active_power formulas."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(1)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        grid_recv = logical_meter.grid_active_power.new_receiver()
        grid_production_recv = logical_meter.grid_production_active_power.new_receiver()
        grid_consumption_recv = (
            logical_meter.grid_consumption_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_meter_active_power([1.0, 2.0, 3.0, 4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(10.0)
        assert (await grid_production_recv.receive()).value == Power.from_watts(0.0)
        assert (await grid_consumption_recv.receive()).value == Power.from_watts(10.0)

        await mockgrid.mock_data.send_meter_active_power([1.0, 2.0, -3.0, -4.0])
        assert (await grid_recv.receive()).value == Power.from_watts(-4.0)
        assert (await grid_production_recv.receive()).value == Power.from_watts(4.0)
        assert (await grid_consumption_recv.receive()).value == Power.from_watts(0.0)

    async def test_chp_active_power(self, mocker: MockerFixture) -> None:
        """Test the chp active_power formula."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_chps(1)
        mockgrid.add_batteries(2)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        chp_active_power_receiver = logical_meter.chp_active_power.new_receiver()
        chp_production_active_power_receiver = (
            logical_meter.chp_production_active_power.new_receiver()
        )
        chp_consumption_active_power_receiver = (
            logical_meter.chp_consumption_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_meter_active_power([1.0, 2.0, 3.0, 4.0])
        assert (await chp_active_power_receiver.receive()).value == Power.from_watts(
            2.0
        )
        assert (
            await chp_production_active_power_receiver.receive()
        ).value == Power.from_watts(0.0)
        assert (
            await chp_consumption_active_power_receiver.receive()
        ).value == Power.from_watts(2.0)

        await mockgrid.mock_data.send_meter_active_power([-4.0, -12.0, None, 10.2])
        assert (await chp_active_power_receiver.receive()).value == Power.from_watts(
            -12.0
        )
        assert (
            await chp_production_active_power_receiver.receive()
        ).value == Power.from_watts(12.0)
        assert (
            await chp_consumption_active_power_receiver.receive()
        ).value == Power.from_watts(0.0)

    async def test_pv_active_power(self, mocker: MockerFixture) -> None:
        """Test the pv active_power formula."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        pv_active_power_receiver = logical_meter.pv_active_power.new_receiver()
        pv_production_active_power_receiver = (
            logical_meter.pv_production_active_power.new_receiver()
        )
        pv_consumption_active_power_receiver = (
            logical_meter.pv_consumption_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_meter_active_power([10.0, -1.0, -2.0])
        assert (await pv_active_power_receiver.receive()).value == Power.from_watts(
            -3.0
        )
        assert (
            await pv_production_active_power_receiver.receive()
        ).value == Power.from_watts(3.0)
        assert (
            await pv_consumption_active_power_receiver.receive()
        ).value == Power.from_watts(0.0)

    async def test_consumer_active_power_grid_meter(
        self, mocker: MockerFixture
    ) -> None:
        """Test the consumer active_power formula with a grid meter."""
        mockgrid = MockMicrogrid(grid_side_meter=True)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        consumer_active_power_receiver = (
            logical_meter.consumer_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_meter_active_power([20.0, 2.0, 3.0, 4.0, 5.0])
        assert (
            await consumer_active_power_receiver.receive()
        ).value == Power.from_watts(6.0)

    async def test_consumer_active_power_no_grid_meter(
        self, mocker: MockerFixture
    ) -> None:
        """Test the consumer active_power formula without a grid meter."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_batteries(2)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        consumer_active_power_receiver = (
            logical_meter.consumer_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_meter_active_power([20.0, 2.0, 3.0, 4.0, 5.0])
        assert (
            await consumer_active_power_receiver.receive()
        ).value == Power.from_watts(20.0)

    async def test_producer_active_power(self, mocker: MockerFixture) -> None:
        """Test the producer active_power formula."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_solar_inverters(2)
        mockgrid.add_chps(2)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        producer_active_power_receiver = (
            logical_meter.producer_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_meter_active_power([20.0, 2.0, 3.0, 4.0, 5.0])
        assert (
            await producer_active_power_receiver.receive()
        ).value == Power.from_watts(14.0)

    async def test_producer_active_power_no_chp(self, mocker: MockerFixture) -> None:
        """Test the producer active_power formula without a chp."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        producer_active_power_receiver = (
            logical_meter.producer_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_meter_active_power([20.0, 2.0, 3.0])
        assert (
            await producer_active_power_receiver.receive()
        ).value == Power.from_watts(5.0)

    async def test_producer_active_power_no_pv(self, mocker: MockerFixture) -> None:
        """Test the producer active_power formula without pv."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        mockgrid.add_chps(1)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        producer_active_power_receiver = (
            logical_meter.producer_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_meter_active_power([20.0, 2.0])
        assert (
            await producer_active_power_receiver.receive()
        ).value == Power.from_watts(2.0)

    async def test_no_producer_active_power(self, mocker: MockerFixture) -> None:
        """Test the producer active_power formula without producers."""
        mockgrid = MockMicrogrid(grid_side_meter=False)
        await mockgrid.start_mock_datapipeline(mocker)

        logical_meter = microgrid.logical_meter()
        producer_active_power_receiver = (
            logical_meter.producer_active_power.new_receiver()
        )

        await mockgrid.mock_data.send_non_existing_component_value()
        assert (
            await producer_active_power_receiver.receive()
        ).value == Power.from_watts(0.0)
