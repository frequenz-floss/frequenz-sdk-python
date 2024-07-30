# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the DataSourcingActor."""

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timezone
from typing import TypeVar
from unittest import mock

import pytest
import pytest_mock
from frequenz.channels import Broadcast
from frequenz.client.microgrid import (
    BatteryComponentState,
    BatteryData,
    BatteryRelayState,
    Component,
    ComponentCategory,
    ComponentData,
    ComponentMetricId,
    EVChargerCableState,
    EVChargerComponentState,
    EVChargerData,
    InverterComponentState,
    InverterData,
    MeterData,
)

from frequenz.sdk._internal._channels import ChannelRegistry
from frequenz.sdk.actor import ComponentMetricRequest, DataSourcingActor
from frequenz.sdk.timeseries import Quantity, Sample

T = TypeVar("T", bound=ComponentData)


@pytest.fixture
def mock_connection_manager(mocker: pytest_mock.MockFixture) -> mock.Mock:
    """Fixture for getting a mock connection manager."""
    mock_client = mock.MagicMock(name="connection_manager.get().api_client")
    mock_client.components = mock.AsyncMock(
        name="components()",
        return_value=[
            Component(component_id=4, category=ComponentCategory.METER),
            Component(component_id=6, category=ComponentCategory.INVERTER),
            Component(component_id=9, category=ComponentCategory.BATTERY),
            Component(component_id=12, category=ComponentCategory.EV_CHARGER),
        ],
    )
    mock_client.meter_data = _new_meter_data_mock(4, starting_value=100.0)
    mock_client.inverter_data = _new_inverter_data_mock(6, starting_value=0.0)
    mock_client.battery_data = _new_battery_data_mock(9, starting_value=9.0)
    mock_client.ev_charger_data = _new_ev_charger_data_mock(12, starting_value=-13.0)
    mock_conn_manager = mock.MagicMock(name="connection_manager")
    mocker.patch(
        "frequenz.sdk.actor._data_sourcing.microgrid_api_source.connection_manager.get",
        return_value=mock_conn_manager,
    )
    mock_conn_manager.api_client = mock_client
    return mock_conn_manager


async def test_data_sourcing_actor(  # pylint: disable=too-many-locals
    mock_connection_manager: mock.Mock,  # pylint: disable=redefined-outer-name,unused-argument
) -> None:
    """Tests for the DataSourcingActor."""
    req_chan = Broadcast[ComponentMetricRequest](name="data_sourcing_requests")
    req_sender = req_chan.new_sender()

    registry = ChannelRegistry(name="test-registry")

    async with DataSourcingActor(req_chan.new_receiver(), registry):
        active_power_request_4 = ComponentMetricRequest(
            "test-namespace", 4, ComponentMetricId.ACTIVE_POWER, None
        )
        active_power_recv_4 = registry.get_or_create(
            Sample[Quantity], active_power_request_4.get_channel_name()
        ).new_receiver()
        await req_sender.send(active_power_request_4)

        reactive_power_request_4 = ComponentMetricRequest(
            "test-namespace", 4, ComponentMetricId.REACTIVE_POWER, None
        )
        reactive_power_recv_4 = registry.get_or_create(
            Sample[Quantity], reactive_power_request_4.get_channel_name()
        ).new_receiver()
        await req_sender.send(reactive_power_request_4)

        active_power_request_6 = ComponentMetricRequest(
            "test-namespace", 6, ComponentMetricId.ACTIVE_POWER, None
        )
        active_power_recv_6 = registry.get_or_create(
            Sample[Quantity], active_power_request_6.get_channel_name()
        ).new_receiver()
        await req_sender.send(active_power_request_6)

        soc_request_9 = ComponentMetricRequest(
            "test-namespace", 9, ComponentMetricId.SOC, None
        )
        soc_recv_9 = registry.get_or_create(
            Sample[Quantity], soc_request_9.get_channel_name()
        ).new_receiver()
        await req_sender.send(soc_request_9)

        soc2_request_9 = ComponentMetricRequest(
            "test-namespace", 9, ComponentMetricId.SOC, None
        )
        soc2_recv_9 = registry.get_or_create(
            Sample[Quantity], soc2_request_9.get_channel_name()
        ).new_receiver()
        await req_sender.send(soc2_request_9)

        active_power_request_12 = ComponentMetricRequest(
            "test-namespace", 12, ComponentMetricId.ACTIVE_POWER, None
        )
        active_power_recv_12 = registry.get_or_create(
            Sample[Quantity], active_power_request_12.get_channel_name()
        ).new_receiver()
        await req_sender.send(active_power_request_12)

        for i in range(3):
            sample = await active_power_recv_4.receive()
            assert sample.value is not None
            assert 100.0 + i == sample.value.base_value

            sample = await reactive_power_recv_4.receive()
            assert sample.value is not None
            assert 100.0 + i == sample.value.base_value

            sample = await active_power_recv_6.receive()
            assert sample.value is not None
            assert 0.0 + i == sample.value.base_value

            sample = await soc_recv_9.receive()
            assert sample.value is not None
            assert 9.0 + i == sample.value.base_value

            sample = await soc2_recv_9.receive()
            assert sample.value is not None
            assert 9.0 + i == sample.value.base_value

            sample = await active_power_recv_12.receive()
            assert sample.value is not None
            assert -13.0 + i == sample.value.base_value


def _new_meter_data(component_id: int, timestamp: datetime, value: float) -> MeterData:
    return MeterData(
        component_id=component_id,
        timestamp=timestamp,
        active_power=value,
        active_power_per_phase=(value, value, value),
        current_per_phase=(value, value, value),
        frequency=value,
        reactive_power=value,
        reactive_power_per_phase=(value, value, value),
        voltage_per_phase=(value, value, value),
    )


def _new_inverter_data(
    component_id: int, timestamp: datetime, value: float
) -> InverterData:
    return InverterData(
        component_id=component_id,
        timestamp=timestamp,
        active_power=value,
        reactive_power=value,
        frequency=value,
        reactive_power_per_phase=(value, value, value),
        active_power_per_phase=(value, value, value),
        current_per_phase=(value, value, value),
        voltage_per_phase=(value, value, value),
        active_power_exclusion_lower_bound=value,
        active_power_exclusion_upper_bound=value,
        active_power_inclusion_lower_bound=value,
        active_power_inclusion_upper_bound=value,
        component_state=InverterComponentState.UNSPECIFIED,
        errors=[],
    )


def _new_battery_data(
    component_id: int, timestamp: datetime, value: float
) -> BatteryData:
    return BatteryData(
        component_id=component_id,
        timestamp=timestamp,
        soc=value,
        temperature=value,
        component_state=BatteryComponentState.UNSPECIFIED,
        errors=[],
        soc_lower_bound=value,
        soc_upper_bound=value,
        capacity=value,
        power_exclusion_lower_bound=value,
        power_exclusion_upper_bound=value,
        power_inclusion_lower_bound=value,
        power_inclusion_upper_bound=value,
        relay_state=BatteryRelayState.UNSPECIFIED,
    )


def _new_ev_charger_data(
    component_id: int, timestamp: datetime, value: float
) -> EVChargerData:
    return EVChargerData(
        component_id=component_id,
        timestamp=timestamp,
        active_power=value,
        active_power_per_phase=(value, value, value),
        current_per_phase=(value, value, value),
        frequency=value,
        reactive_power=value,
        reactive_power_per_phase=(value, value, value),
        voltage_per_phase=(value, value, value),
        active_power_exclusion_lower_bound=value,
        active_power_exclusion_upper_bound=value,
        active_power_inclusion_lower_bound=value,
        active_power_inclusion_upper_bound=value,
        cable_state=EVChargerCableState.UNSPECIFIED,
        component_state=EVChargerComponentState.UNSPECIFIED,
    )


def _new_streamer_mock(
    name: str,
    constructor: Callable[[int, datetime, float], T],
    component_id: int,
    starting_value: float,
) -> mock.AsyncMock:
    """Get a mock streamer."""

    async def generate_data(starting_value: float) -> AsyncIterator[T]:
        value = starting_value
        while True:
            yield constructor(component_id, datetime.now(timezone.utc), value)
            await asyncio.sleep(0)  # Let other tasks run
            value += 1.0

    return mock.AsyncMock(name=name, return_value=generate_data(starting_value))


def _new_meter_data_mock(component_id: int, starting_value: float) -> mock.AsyncMock:
    """Get a mock streamer for meter data."""
    return _new_streamer_mock(
        f"meter_data_mock(id={component_id}, starting_value={starting_value})",
        _new_meter_data,
        component_id,
        starting_value,
    )


def _new_inverter_data_mock(component_id: int, starting_value: float) -> mock.AsyncMock:
    """Get a mock streamer for inverter data."""
    return _new_streamer_mock(
        f"inverter_data_mock(id={component_id}, starting_value={starting_value})",
        _new_inverter_data,
        component_id,
        starting_value,
    )


def _new_battery_data_mock(component_id: int, starting_value: float) -> mock.AsyncMock:
    """Get a mock streamer for battery data."""
    return _new_streamer_mock(
        f"battery_data_mock(id={component_id}, starting_value={starting_value})",
        _new_battery_data,
        component_id,
        starting_value,
    )


def _new_ev_charger_data_mock(
    component_id: int, starting_value: float
) -> mock.AsyncMock:
    """Get a mock streamer for EV charger data."""
    return _new_streamer_mock(
        f"ev_charger_data_mock(id={component_id}, starting_value={starting_value})",
        _new_ev_charger_data,
        component_id,
        starting_value,
    )
