# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Utility functions to generate data for testing purposes."""

from typing import Any, Dict, Optional

from frequenz.api.microgrid import microgrid_pb2 as microgrid_pb
from frequenz.api.microgrid.battery_pb2 import Battery
from frequenz.api.microgrid.battery_pb2 import Data as PbBatteryData
from frequenz.api.microgrid.battery_pb2 import Properties as BatteryProperties
from frequenz.api.microgrid.common_pb2 import AC, DC, Bounds, Metric, MetricAggregation
from frequenz.api.microgrid.ev_charger_pb2 import Data as PbEVChargerData
from frequenz.api.microgrid.ev_charger_pb2 import EVCharger
from frequenz.api.microgrid.inverter_pb2 import Data as PbInverterData
from frequenz.api.microgrid.inverter_pb2 import Inverter
from frequenz.api.microgrid.meter_pb2 import Data as PbMeterData
from frequenz.api.microgrid.meter_pb2 import Meter
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module

from frequenz.sdk.microgrid import BatteryData, EVChargerData, InverterData, MeterData


def generate_battery_data(
    component_id: int,
    timestamp: Optional[Timestamp] = None,
    params: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[int, Dict[str, Any]]] = None,
) -> BatteryData:
    """Generate `BatteryData` for testing purposes.

    Args:
        component_id: Component ID.
        timestamp: Timestamp describing when data was collected
            from the component. If not provided, it will default to current time.
        params: Params to be returned by all battery
            components. If not provided, default params will be used.
        overrides: Params to be returned by
            battery components with specific IDs. If not provided, all battery
            components will return the same data, either `params` or default params.

    Returns:
        Generated `BatteryData`, possibly with `params` and `overrides` applied to
            modify the default data, if they were provided by the caller.
    """
    if timestamp is None:
        timestamp = Timestamp()

    timestamp.GetCurrentTime()

    _params: Dict[str, Any] = dict(
        properties=BatteryProperties(capacity=float(component_id % 100)),
        data=PbBatteryData(
            soc=MetricAggregation(avg=float(component_id % 100)),
            dc=DC(
                power=Metric(
                    value=100.0, system_bounds=Bounds(lower=-1000.0, upper=1000.0)
                )
            ),
        ),
    )

    if params is not None:
        _params.update(params)

    if overrides is not None and component_id in overrides:
        _params.update(overrides[component_id])

    return BatteryData.from_proto(
        microgrid_pb.ComponentData(
            ts=timestamp,
            id=component_id,
            battery=Battery(
                **_params,
            ),
        )
    )


def generate_inverter_data(
    component_id: int,
    timestamp: Optional[Timestamp] = None,
    params: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[int, Dict[str, Any]]] = None,
) -> InverterData:
    """Generate `InverterData` for testing purposes.

    Args:
        component_id: Component ID.
        timestamp: Timestamp describing when data was collected
            from the component. If not provided, it will default to current time.
        params: Params to be returned by all inverter
            components. If not provided, default params will be used.
        overrides: Params to be returned by
            inverter components with specific IDs. If not provided, all inverter
            components will return the same data, either `params` or default params.

    Returns:
        Generated `InverterData`, possibly with `params` and `overrides` applied to
            modify the default data, if they were provided by the caller.
    """
    if timestamp is None:
        timestamp = Timestamp()

    timestamp.GetCurrentTime()

    _params: Dict[str, Any] = dict(
        data=PbInverterData(
            ac=AC(
                power_active=Metric(
                    value=100.0, system_bounds=Bounds(lower=-1000.0, upper=1000.0)
                )
            )
        )
    )

    if params is not None:
        _params.update(params)

    if overrides is not None and component_id in overrides:
        _params.update(overrides[component_id])

    return InverterData.from_proto(
        microgrid_pb.ComponentData(
            ts=timestamp,
            id=component_id,
            inverter=Inverter(
                **_params,
            ),
        )
    )


def generate_meter_data(
    component_id: int,
    timestamp: Optional[Timestamp] = None,
    params: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[int, Dict[str, Any]]] = None,
) -> MeterData:
    """Generate `MeterData` for testing purposes.

    Args:
        component_id: Component ID.
        timestamp: Timestamp describing when data was collected
            from the component. If not provided, it will default to current time.
        params: Params to be returned by all meter
            components. If not provided, default params will be used.
        overrides: Params to be returned by
            meter components with specific IDs. If not provided, all meter
            components will return the same data, either `params` or default params.

    Returns:
        Generated `MeterData`, possibly with `params` and `overrides` applied to
            modify the default data, if they were provided by the caller.
    """
    if timestamp is None:
        timestamp = Timestamp()

    timestamp.GetCurrentTime()

    _params: Dict[str, Any] = dict(
        data=PbMeterData(
            ac=AC(power_active=Metric(value=100.0)),
        )
    )

    if params is not None:
        _params.update(params)

    if overrides is not None and component_id in overrides:
        _params.update(overrides[component_id])

    return MeterData.from_proto(
        microgrid_pb.ComponentData(
            ts=timestamp,
            id=component_id,
            meter=Meter(
                **_params,
            ),
        )
    )


def generate_ev_charger_data(
    component_id: int,
    timestamp: Optional[Timestamp] = None,
    params: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[int, Dict[str, Any]]] = None,
) -> EVChargerData:
    """Generate `EVChargerData` for testing purposes.

    Args:
        component_id: Component ID.
        timestamp: Timestamp describing when data was collected
            from the component. If not provided, it will default to current time.
        params: Params to be returned by all EV charger
            components. If not provided, default params will be used.
        overrides: Params to be returned by
            EV charger components with specific IDs. If not provided, all EV charger
            components will return the same data, either `params` or default params.

    Returns:
        Generated `EVChargerData`, possibly with `params` and `overrides` applied to
            modify the default data, if they were provided by the caller.
    """
    if timestamp is None:
        timestamp = Timestamp()

    timestamp.GetCurrentTime()

    _params: Dict[str, Any] = dict(
        data=PbEVChargerData(
            ac=AC(
                power_active=Metric(
                    value=100.0,
                    system_bounds=Bounds(lower=-1000.0, upper=1000.0),
                ),
            ),
            dc=DC(
                power=Metric(
                    value=100.0,
                    system_bounds=Bounds(lower=-1000.0, upper=1000.0),
                ),
            ),
        )
    )

    if params is not None:
        _params.update(params)

    if overrides is not None and component_id in overrides:
        _params.update(overrides[component_id])

    return EVChargerData.from_proto(
        microgrid_pb.ComponentData(
            ts=timestamp,
            id=component_id,
            ev_charger=EVCharger(
                **_params,
            ),
        )
    )
