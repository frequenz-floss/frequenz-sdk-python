# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the logical meter."""


import asyncio
from datetime import datetime, timezone

import pytest
from frequenz.quantities import Frequency
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from tests.utils import component_data_wrapper

from ._formulas.utils import equal_float_lists
from .mock_microgrid import MockMicrogrid

# pylint: disable=protected-access


@pytest.mark.parametrize("use_subscribe", [True, False])
async def test_grid_frequency_none(mocker: MockerFixture, use_subscribe: bool) -> None:
    """Test the grid frequency formula."""
    mockgrid = MockMicrogrid(grid_meter=True)
    mockgrid.add_batteries(2)
    mockgrid.add_solar_inverters(1)
    await mockgrid.start(mocker)

    grid_freq = microgrid.frequency()
    if use_subscribe:
        grid_freq_recv = await grid_freq.subscribe()
    else:
        # Deprecated
        grid_freq_recv = grid_freq.new_receiver()
        assert grid_freq._task is not None
        # We have to wait for the metric request to be sent
        await grid_freq._task
        # And consumed
        await asyncio.sleep(0)

    await mockgrid.mock_client.send(
        component_data_wrapper.MeterDataWrapper(
            mockgrid.meter_ids[0], datetime.now(tz=timezone.utc)
        ).to_samples()
    )

    val = await grid_freq_recv.receive()
    assert val is not None
    assert val.value is None
    await mockgrid.cleanup()


@pytest.mark.parametrize("use_subscribe", [True, False])
async def test_grid_frequency_1(mocker: MockerFixture, use_subscribe: bool) -> None:
    """Test the grid frequency formula."""
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    mockgrid.add_batteries(2)
    mockgrid.add_solar_inverters(1)

    async with mockgrid:
        grid_freq = microgrid.frequency()
        if use_subscribe:
            grid_freq_recv = await grid_freq.subscribe()
        else:
            # Deprecated
            grid_freq_recv = grid_freq.new_receiver()
            assert grid_freq._task is not None
            # We have to wait for the metric request to be sent
            await grid_freq._task
            # And consumed
            await asyncio.sleep(0)

        results = []
        grid_meter_data = []
        for count in range(10):
            freq = float(50.0 + (1 if count % 2 == 0 else -1) * 0.01)
            await mockgrid.mock_client.send(
                component_data_wrapper.MeterDataWrapper(
                    mockgrid.meter_ids[0], datetime.now(tz=timezone.utc), frequency=freq
                ).to_samples()
            )

            grid_meter_data.append(Frequency.from_hertz(freq))
            val = await grid_freq_recv.receive()
            assert val is not None and val.value is not None
            assert val.value.as_hertz() == freq
            results.append(val.value)

    assert equal_float_lists(results, grid_meter_data)


@pytest.mark.parametrize("use_subscribe", [True, False])
async def test_grid_frequency_no_grid_meter_no_consumer_meter(
    mocker: MockerFixture, use_subscribe: bool
) -> None:
    """Test the grid frequency formula without a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_consumer_meters()
    mockgrid.add_batteries(1, no_meter=False)
    mockgrid.add_batteries(1, no_meter=False)

    async with mockgrid:
        grid_freq = microgrid.frequency()

        if use_subscribe:
            grid_freq_recv = await grid_freq.subscribe()
        else:
            # Deprecated
            grid_freq_recv = grid_freq.new_receiver()
            assert grid_freq._task is not None
            # We have to wait for the metric request to be sent
            await grid_freq._task
            # And consumed
            await asyncio.sleep(0)

        results = []
        meter_data = []
        for count in range(10):
            freq = float(50.0 + (1 if count % 2 == 0 else -1) * 0.01)
            await mockgrid.mock_client.send(
                component_data_wrapper.MeterDataWrapper(
                    mockgrid.meter_ids[0], datetime.now(tz=timezone.utc), frequency=freq
                ).to_samples()
            )
            meter_data.append(Frequency.from_hertz(freq))

            val = await grid_freq_recv.receive()
            assert val is not None and val.value is not None
            assert val.value.as_hertz() == freq
            results.append(val.value)

    assert equal_float_lists(results, meter_data)


@pytest.mark.parametrize("use_subscribe", [True, False])
async def test_grid_frequency_no_grid_meter(
    mocker: MockerFixture, use_subscribe: bool
) -> None:
    """Test the grid frequency formula without a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_batteries(1, no_meter=False)
    mockgrid.add_batteries(1, no_meter=True)

    async with mockgrid:
        grid_freq = microgrid.frequency()

        if use_subscribe:
            grid_freq_recv = await grid_freq.subscribe()
        else:
            # Deprecated
            grid_freq_recv = grid_freq.new_receiver()
            assert grid_freq._task is not None
            # We have to wait for the metric request to be sent
            await grid_freq._task
            # And consumed
            await asyncio.sleep(0)

        results = []
        meter_data = []
        for count in range(10):
            freq = float(50.0 + (1 if count % 2 == 0 else -1) * 0.01)
            await mockgrid.mock_client.send(
                component_data_wrapper.MeterDataWrapper(
                    mockgrid.meter_ids[0], datetime.now(tz=timezone.utc), frequency=freq
                ).to_samples()
            )
            meter_data.append(Frequency.from_hertz(freq))

            val = await grid_freq_recv.receive()
            assert val is not None and val.value is not None
            assert val.value.as_hertz() == freq
            results.append(val.value)

    assert equal_float_lists(results, meter_data)


@pytest.mark.parametrize("use_subscribe", [True, False])
async def test_grid_frequency_only_inverter(
    mocker: MockerFixture, use_subscribe: bool
) -> None:
    """Test the grid frequency formula without any meter but only inverters."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_batteries(2, no_meter=True)

    async with mockgrid:
        grid_freq = microgrid.frequency()
        if use_subscribe:
            grid_freq_recv = await grid_freq.subscribe()
        else:
            # Deprecated
            grid_freq_recv = grid_freq.new_receiver()
            assert grid_freq._task is not None
            # We have to wait for the metric request to be sent
            await grid_freq._task
            # And consumed
            await asyncio.sleep(0)

        results = []
        meter_data = []
        for count in range(10):
            freq = float(50.0 + (1 if count % 2 == 0 else -1) * 0.01)

            await mockgrid.mock_client.send(
                component_data_wrapper.InverterDataWrapper(
                    mockgrid.battery_inverter_ids[0],
                    datetime.now(tz=timezone.utc),
                    frequency=freq,
                ).to_samples()
            )

            meter_data.append(Frequency.from_hertz(freq))
            val = await grid_freq_recv.receive()
            assert val is not None and val.value is not None
            assert val.value.as_hertz() == freq
            results.append(val.value)

    assert equal_float_lists(results, meter_data)
