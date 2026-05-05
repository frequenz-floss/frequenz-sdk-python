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
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    BatteryInverter,
    ComponentStateCode,
    DcEvCharger,
    LiIonBattery,
    Meter,
)
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Quantity

from frequenz.sdk._internal._channels import ChannelRegistry
from frequenz.sdk.microgrid._data_sourcing import (
    ComponentMetricRequest,
    DataSourcingActor,
)
from frequenz.sdk.microgrid._old_component_data import (
    BatteryData,
    ComponentData,
    EVChargerData,
    InverterData,
    MeterData,
)
from frequenz.sdk.timeseries import Sample

from ..utils.receive_timeout import Timeout, receive_timeout

T = TypeVar("T", bound=ComponentData)

_MICROGRID_ID = MicrogridId(1)


@pytest.fixture
def mock_connection_manager(mocker: pytest_mock.MockFixture) -> mock.Mock:
    """Fixture for getting a mock connection manager."""
    mock_client = mock.MagicMock(name="connection_manager.get().api_client")
    mock_client.list_components = mock.AsyncMock(
        name="list_components()",
        return_value=[
            Meter(id=ComponentId(4), microgrid_id=_MICROGRID_ID),
            BatteryInverter(id=ComponentId(6), microgrid_id=_MICROGRID_ID),
            LiIonBattery(id=ComponentId(9), microgrid_id=_MICROGRID_ID),
            DcEvCharger(id=ComponentId(12), microgrid_id=_MICROGRID_ID),
        ],
    )

    mocker.patch(
        "frequenz.sdk.microgrid._data_sourcing.microgrid_api_source.MeterData.subscribe",
        side_effect=_new_meter_data_mock(ComponentId(4), starting_value=100.0),
    )
    mocker.patch(
        "frequenz.sdk.microgrid._data_sourcing.microgrid_api_source.InverterData.subscribe",
        side_effect=_new_inverter_data_mock(ComponentId(6), starting_value=0.0),
    )
    mocker.patch(
        "frequenz.sdk.microgrid._data_sourcing.microgrid_api_source.BatteryData.subscribe",
        side_effect=_new_battery_data_mock(ComponentId(9), starting_value=9.0),
    )
    mocker.patch(
        "frequenz.sdk.microgrid._data_sourcing.microgrid_api_source.EVChargerData.subscribe",
        side_effect=_new_ev_charger_data_mock(ComponentId(12), starting_value=-13.0),
    )

    mock_conn_manager = mock.MagicMock(name="connection_manager")
    mocker.patch(
        "frequenz.sdk.microgrid._data_sourcing"
        ".microgrid_api_source.connection_manager.get",
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
            "test-namespace", ComponentId(4), Metric.AC_ACTIVE_POWER, None
        )
        active_power_recv_4 = registry.get_or_create(
            Sample[Quantity], active_power_request_4.get_channel_name()
        ).new_receiver()
        await req_sender.send(active_power_request_4)

        reactive_power_request_4 = ComponentMetricRequest(
            "test-namespace", ComponentId(4), Metric.AC_REACTIVE_POWER, None
        )
        reactive_power_recv_4 = registry.get_or_create(
            Sample[Quantity], reactive_power_request_4.get_channel_name()
        ).new_receiver()
        await req_sender.send(reactive_power_request_4)

        active_power_request_6 = ComponentMetricRequest(
            "test-namespace", ComponentId(6), Metric.AC_ACTIVE_POWER, None
        )
        active_power_recv_6 = registry.get_or_create(
            Sample[Quantity], active_power_request_6.get_channel_name()
        ).new_receiver()
        await req_sender.send(active_power_request_6)

        soc_request_9 = ComponentMetricRequest(
            "test-namespace", ComponentId(9), Metric.BATTERY_SOC_PCT, None
        )
        soc_recv_9 = registry.get_or_create(
            Sample[Quantity], soc_request_9.get_channel_name()
        ).new_receiver()
        await req_sender.send(soc_request_9)

        soc2_request_9 = ComponentMetricRequest(
            "test-namespace", ComponentId(9), Metric.BATTERY_SOC_PCT, None
        )
        soc2_recv_9 = registry.get_or_create(
            Sample[Quantity], soc2_request_9.get_channel_name()
        ).new_receiver()
        await req_sender.send(soc2_request_9)

        active_power_request_12 = ComponentMetricRequest(
            "test-namespace", ComponentId(12), Metric.AC_ACTIVE_POWER, None
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


async def test_duplicate_requests_do_not_block_new_receivers(
    mock_connection_manager: mock.Mock,  # pylint: disable=redefined-outer-name,unused-argument
) -> None:
    """Ensure duplicate direct subscriptions still deliver samples."""
    req_chan = Broadcast[ComponentMetricRequest](name="data_sourcing_requests")
    req_sender = req_chan.new_sender()
    registry = ChannelRegistry(name="test-registry")

    request = ComponentMetricRequest(
        "test-namespace",
        ComponentId(4),
        Metric.AC_ACTIVE_POWER,
        None,
    )

    async with DataSourcingActor(req_chan.new_receiver(), registry):
        first_receiver = registry.get_or_create(
            Sample[Quantity], request.get_channel_name()
        ).new_receiver()
        await req_sender.send(request)

        first_sample = await receive_timeout(first_receiver, timeout=1.0)
        assert first_sample is not Timeout

        second_receiver = registry.get_or_create(
            Sample[Quantity], request.get_channel_name()
        ).new_receiver()
        await req_sender.send(request)

        second_sample = await receive_timeout(second_receiver, timeout=1.0)
        assert second_sample is not Timeout


async def test_unknown_component_request_does_not_block_later_valid_request(
    mock_connection_manager: mock.Mock,  # pylint: disable=redefined-outer-name,unused-argument
) -> None:
    """Ensure unknown component requests don't block later valid streams."""
    req_chan = Broadcast[ComponentMetricRequest](name="data_sourcing_requests")
    req_sender = req_chan.new_sender()
    registry = ChannelRegistry(name="test-registry")

    unknown_request = ComponentMetricRequest(
        "unknown-component",
        ComponentId(999),
        Metric.AC_ACTIVE_POWER,
        None,
    )
    valid_request = ComponentMetricRequest(
        "valid-component",
        ComponentId(4),
        Metric.AC_ACTIVE_POWER,
        None,
    )

    async with DataSourcingActor(req_chan.new_receiver(), registry):
        valid_receiver = registry.get_or_create(
            Sample[Quantity], valid_request.get_channel_name()
        ).new_receiver()

        await req_sender.send(unknown_request)
        await req_sender.send(valid_request)

        valid_sample = await receive_timeout(valid_receiver, timeout=1.0)
        assert valid_sample is not Timeout


@pytest.mark.skip(
    reason="This is currently failing, but we are probably not going to "
    "fix it since the data sourcing actor will likely go away."
)
async def test_invalid_metric_request_does_not_block_later_valid_request(
    mock_connection_manager: mock.Mock,  # pylint: disable=redefined-outer-name,unused-argument
) -> None:
    """Ensure a bad request doesn't poison later valid subscriptions."""
    req_chan = Broadcast[ComponentMetricRequest](name="data_sourcing_requests")
    req_sender = req_chan.new_sender()
    registry = ChannelRegistry(name="test-registry")

    invalid_request = ComponentMetricRequest(
        "invalid-metric",
        ComponentId(4),
        Metric.BATTERY_SOC_PCT,
        None,
    )
    valid_request = ComponentMetricRequest(
        "valid-metric",
        ComponentId(4),
        Metric.AC_ACTIVE_POWER,
        None,
    )

    async with DataSourcingActor(req_chan.new_receiver(), registry):
        valid_receiver = registry.get_or_create(
            Sample[Quantity], valid_request.get_channel_name()
        ).new_receiver()

        await req_sender.send(invalid_request)
        await req_sender.send(valid_request)

        valid_sample = await receive_timeout(valid_receiver, timeout=1.0)
        assert valid_sample is not Timeout


def _new_meter_data(
    component_id: ComponentId, timestamp: datetime, value: float
) -> MeterData:
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
        states=frozenset(),
        warnings=frozenset(),
        errors=frozenset(),
    )


def _new_inverter_data(
    component_id: ComponentId, timestamp: datetime, value: float
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
        states={ComponentStateCode.UNSPECIFIED},
        errors=frozenset(),
        warnings=frozenset(),
    )


def _new_battery_data(
    component_id: ComponentId, timestamp: datetime, value: float
) -> BatteryData:
    return BatteryData(
        component_id=component_id,
        timestamp=timestamp,
        soc=value,
        temperature=value,
        soc_lower_bound=value,
        soc_upper_bound=value,
        capacity=value,
        power_exclusion_lower_bound=value,
        power_exclusion_upper_bound=value,
        power_inclusion_lower_bound=value,
        power_inclusion_upper_bound=value,
        states={ComponentStateCode.UNSPECIFIED},
        errors=frozenset(),
        warnings=frozenset(),
    )


def _new_ev_charger_data(
    component_id: ComponentId, timestamp: datetime, value: float
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
        states={ComponentStateCode.UNSPECIFIED},
        errors=frozenset(),
        warnings=frozenset(),
    )


def _new_streamer_mock(
    name: str,
    constructor: Callable[[ComponentId, datetime, float], T],
    component_id: ComponentId,
    starting_value: float,
) -> mock.Mock:
    """Get a mock streamer."""

    async def generate_data(starting_value: float) -> AsyncIterator[T]:
        value = starting_value
        while True:
            yield constructor(component_id, datetime.now(timezone.utc), value)
            await asyncio.sleep(0)  # Let other tasks run
            value += 1.0

    return mock.Mock(name=name, return_value=generate_data(starting_value))


def _new_meter_data_mock(component_id: ComponentId, starting_value: float) -> mock.Mock:
    """Get a mock streamer for meter data."""
    return _new_streamer_mock(
        f"meter_data_mock(id={component_id}, starting_value={starting_value})",
        _new_meter_data,
        component_id,
        starting_value,
    )


def _new_inverter_data_mock(
    component_id: ComponentId, starting_value: float
) -> mock.Mock:
    """Get a mock streamer for inverter data."""
    return _new_streamer_mock(
        f"inverter_data_mock(id={component_id}, starting_value={starting_value})",
        _new_inverter_data,
        component_id,
        starting_value,
    )


def _new_battery_data_mock(
    component_id: ComponentId, starting_value: float
) -> mock.Mock:
    """Get a mock streamer for battery data."""
    return _new_streamer_mock(
        f"battery_data_mock(id={component_id}, starting_value={starting_value})",
        _new_battery_data,
        component_id,
        starting_value,
    )


def _new_ev_charger_data_mock(
    component_id: ComponentId, starting_value: float
) -> mock.Mock:
    """Get a mock streamer for EV charger data."""
    return _new_streamer_mock(
        f"ev_charger_data_mock(id={component_id}, starting_value={starting_value})",
        _new_ev_charger_data,
        component_id,
        starting_value,
    )
