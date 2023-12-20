# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the microgrid component data."""

from datetime import datetime, timezone

import pytest
from frequenz.api.common import metrics_pb2
from frequenz.api.common.metrics import electrical_pb2
from frequenz.api.microgrid import inverter_pb2, microgrid_pb2
from google.protobuf import timestamp_pb2

from frequenz.sdk.microgrid.component import ComponentData, InverterData

# pylint: disable=no-member


def test_component_data_abstract_class() -> None:
    """Verify the base class ComponentData may not be instantiated."""
    with pytest.raises(TypeError):
        # pylint: disable=abstract-class-instantiated
        ComponentData(0, datetime.now(timezone.utc))  # type: ignore


def test_inverter_data() -> None:
    """Verify the constructor for the InverterData class."""
    seconds = 1234567890

    raw = microgrid_pb2.ComponentData(
        id=5,
        ts=timestamp_pb2.Timestamp(seconds=seconds),
        inverter=inverter_pb2.Inverter(
            state=inverter_pb2.State(
                component_state=inverter_pb2.COMPONENT_STATE_DISCHARGING
            ),
            errors=[inverter_pb2.Error(msg="error message")],
            data=inverter_pb2.Data(
                dc_battery=None,
                dc_solar=None,
                temperature=None,
                ac=electrical_pb2.AC(
                    frequency=metrics_pb2.Metric(value=50.1),
                    power_active=metrics_pb2.Metric(
                        value=100.2,
                        system_exclusion_bounds=metrics_pb2.Bounds(
                            lower=-501.0, upper=501.0
                        ),
                        system_inclusion_bounds=metrics_pb2.Bounds(
                            lower=-51_000.0, upper=51_000.0
                        ),
                    ),
                    phase_1=electrical_pb2.AC.ACPhase(
                        current=metrics_pb2.Metric(value=12.3),
                        voltage=metrics_pb2.Metric(value=229.8),
                    ),
                    phase_2=electrical_pb2.AC.ACPhase(
                        current=metrics_pb2.Metric(value=23.4),
                        voltage=metrics_pb2.Metric(value=230.0),
                    ),
                    phase_3=electrical_pb2.AC.ACPhase(
                        current=metrics_pb2.Metric(value=34.5),
                        voltage=metrics_pb2.Metric(value=230.2),
                    ),
                ),
            ),
        ),
    )

    inv_data = InverterData.from_proto(raw)
    assert inv_data.component_id == 5
    assert inv_data.timestamp == datetime.fromtimestamp(seconds, timezone.utc)
    assert (  # pylint: disable=protected-access
        inv_data._component_state == inverter_pb2.COMPONENT_STATE_DISCHARGING
    )
    assert inv_data._errors == [  # pylint: disable=protected-access
        inverter_pb2.Error(msg="error message")
    ]
    assert inv_data.frequency == pytest.approx(50.1)
    assert inv_data.active_power == pytest.approx(100.2)
    assert inv_data.current_per_phase == pytest.approx((12.3, 23.4, 34.5))
    assert inv_data.voltage_per_phase == pytest.approx((229.8, 230.0, 230.2))
    assert inv_data.active_power_inclusion_lower_bound == pytest.approx(-51_000.0)
    assert inv_data.active_power_inclusion_upper_bound == pytest.approx(51_000.0)
    assert inv_data.active_power_exclusion_lower_bound == pytest.approx(-501.0)
    assert inv_data.active_power_exclusion_upper_bound == pytest.approx(501.0)
