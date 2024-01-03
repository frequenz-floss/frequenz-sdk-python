# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for fetching and streaming the phase-to-neutral voltage."""


import asyncio

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid

from .mock_microgrid import MockMicrogrid

# pylint: disable=protected-access


async def test_voltage_1(mocker: MockerFixture) -> None:
    """Test the phase-to-neutral voltage with a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    mockgrid.add_batteries(1, no_meter=True)
    mockgrid.add_batteries(1, no_meter=False)

    async with mockgrid:
        voltage = microgrid.voltage()
        voltage_recv = voltage.new_receiver()

        assert voltage._task is not None
        # Wait for voltage requests to be sent, one request per phase.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        for count in range(10):
            volt_delta = 1 if count % 2 == 0 else -1
            volt_phases: list[float | None] = [
                220.0 * volt_delta,
                219.8 * volt_delta,
                220.2 * volt_delta,
            ]

            await mockgrid.mock_resampler.send_meter_voltage([volt_phases, volt_phases])

            val = await voltage_recv.receive()
            assert val is not None
            assert val.value_p1 and val.value_p2 and val.value_p3
            assert val.value_p1.as_volts() == volt_phases[0]
            assert val.value_p2.as_volts() == volt_phases[1]
            assert val.value_p3.as_volts() == volt_phases[2]


async def test_voltage_2(mocker: MockerFixture) -> None:
    """Test the phase-to-neutral voltage without a grid side meter."""
    mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
    mockgrid.add_batteries(1, no_meter=False)
    mockgrid.add_batteries(1, no_meter=True)

    async with mockgrid:
        voltage = microgrid.voltage()
        voltage_recv = voltage.new_receiver()

        assert voltage._task is not None
        # Wait for voltage requests to be sent, one request per phase.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        for count in range(10):
            volt_delta = 1 if count % 2 == 0 else -1
            volt_phases: list[float | None] = [
                220.0 * volt_delta,
                219.8 * volt_delta,
                220.2 * volt_delta,
            ]

            await mockgrid.mock_resampler.send_meter_voltage([volt_phases])

            val = await voltage_recv.receive()
            assert val is not None
            assert val.value_p1 and val.value_p2 and val.value_p3
            assert val.value_p1.as_volts() == volt_phases[0]
            assert val.value_p2.as_volts() == volt_phases[1]
            assert val.value_p3.as_volts() == volt_phases[2]


async def test_voltage_3(mocker: MockerFixture) -> None:
    """Test the phase-to-neutral voltage with None values."""
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    mockgrid.add_batteries(2, no_meter=False)

    async with mockgrid:
        voltage = microgrid.voltage()
        voltage_recv = voltage.new_receiver()

        assert voltage._task is not None
        # Wait for voltage requests to be sent, one request per phase.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        for count in range(10):
            volt_delta = 1 if count % 2 == 0 else -1
            volt_phases: list[float | None] = [
                220.0 * volt_delta,
                219.8 * volt_delta,
                220.2 * volt_delta,
            ]

            await mockgrid.mock_resampler.send_meter_voltage(
                [volt_phases, [None, None, None], [None, 219.8, 220.2]]
            )

            val = await voltage_recv.receive()
            assert val is not None
            assert val.value_p1 and val.value_p2 and val.value_p3
            assert val.value_p1.as_volts() == volt_phases[0]
            assert val.value_p2.as_volts() == volt_phases[1]
            assert val.value_p3.as_volts() == volt_phases[2]
