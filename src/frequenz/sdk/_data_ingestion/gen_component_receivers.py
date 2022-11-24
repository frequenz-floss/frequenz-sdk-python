# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Functions for generating DataColletor with necessary fields from ComponentInfo."""

from typing import Any, Dict, List

from frequenz.channels import Receiver
from frequenz.channels.util import Merge

from ..microgrid import client
from ..microgrid.component import (
    BatteryData,
    ComponentCategory,
    EVChargerData,
    InverterData,
    MeterData,
)
from .component_info import ComponentInfo
from .constants import (
    METRIC_ACTIVE_POWER,
    METRIC_ACTIVE_POWER_LOWER_BOUND,
    METRIC_ACTIVE_POWER_UPPER_BOUND,
    METRIC_CAPACITY,
    METRIC_COMPONENT_ID,
    METRIC_EV_ACTIVE_POWER,
    METRIC_POWER_LOWER_BOUND,
    METRIC_POWER_UPPER_BOUND,
    METRIC_SOC,
    METRIC_TIMESTAMP,
)


def transform_ev_charger_data(
    data: EVChargerData,
) -> Dict[str, Any]:
    """Transform component reading from an EV Charger into a dict.

    Args:
        data: data from EV Charger component

    Returns:
        Metrics collected from an EV Charger.
    """
    result: Dict[str, Any] = {}
    result[METRIC_COMPONENT_ID] = data.component_id
    result[METRIC_TIMESTAMP] = data.timestamp
    result[METRIC_EV_ACTIVE_POWER] = data.active_power
    return result


def transform_inverter_data(
    data: InverterData,
) -> Dict[str, Any]:
    """Transform component reading from an Inverter into a dict.

    Args:
        data: reading from Inverter component

    Returns:
        Metrics collected from an Inverter.
    """
    result: Dict[str, Any] = {}
    result[METRIC_COMPONENT_ID] = data.component_id
    result[METRIC_TIMESTAMP] = data.timestamp
    result[METRIC_ACTIVE_POWER_UPPER_BOUND] = data.active_power_upper_bound
    result[METRIC_ACTIVE_POWER_LOWER_BOUND] = data.active_power_lower_bound
    # Active Power shouldn't be a single value, but bounds instead
    # Bounds(data.active_power_lower_bound, data.active_power_upper_bound)
    result[METRIC_ACTIVE_POWER] = data.active_power
    return result


def transform_meter_data(
    data: MeterData,
) -> Dict[str, Any]:
    """Transform component reading from a Meter into a dict.

    Args:
        data: reading from Meter component

    Returns:
        Metrics collected from a Meter.
    """
    result: Dict[str, Any] = {}
    result[METRIC_COMPONENT_ID] = data.component_id
    result[METRIC_TIMESTAMP] = data.timestamp
    result[METRIC_ACTIVE_POWER] = data.active_power
    return result


def transform_battery_data(
    data: BatteryData,
) -> Dict[str, Any]:
    """Transform component reading from a Battery into a dict.

    Args:
        data: reading from Battery component

    Returns:
        Metrics collected from a Battery.
    """
    result: Dict[str, Any] = {}
    result[METRIC_COMPONENT_ID] = data.component_id
    result[METRIC_TIMESTAMP] = data.timestamp
    result[METRIC_SOC] = data.soc
    result[METRIC_CAPACITY] = data.capacity
    result[METRIC_POWER_UPPER_BOUND] = data.power_upper_bound
    result[METRIC_POWER_LOWER_BOUND] = data.power_lower_bound
    return result


async def _create_meter_receiver(
    component_id: int,
    microgrid_client: client.MicrogridApiClient,
) -> Receiver[Dict[str, Any]]:
    """Create a receiver for transformed metrics from a meter component.

    Args:
        component_id: component ID
        microgrid_client: microgrid client

    Returns:
        Receiver for transformed metrics.
    """
    recv_meter_data: Receiver[MeterData] = await microgrid_client.meter_data(
        component_id
    )
    receiver = recv_meter_data.map(transform_meter_data)
    return receiver


async def _create_ev_charger_receiver(
    component_id: int,
    microgrid_client: client.MicrogridApiClient,
) -> Receiver[Dict[str, Any]]:
    """Create a receiver for transformed metrics from a EV charger component.

    Args:
        component_id: component ID
        microgrid_client: microgrid client

    Returns:
        Receiver for transformed metrics.
    """
    recv_ev_charger_data: Receiver[
        EVChargerData
    ] = await microgrid_client.ev_charger_data(component_id)
    receiver = recv_ev_charger_data.map(transform_ev_charger_data)
    return receiver


async def _create_inverter_receiver(
    component_id: int,
    microgrid_client: client.MicrogridApiClient,
) -> Receiver[Dict[str, Any]]:
    """Create a receiver for transformed metrics from a inverter component.

    Args:
        component_id: component ID
        microgrid_client: microgrid client

    Returns:
        Receiver for transformed metrics.
    """
    recv_inverter_data: Receiver[InverterData] = await microgrid_client.inverter_data(
        component_id
    )
    receiver = recv_inverter_data.map(transform_inverter_data)
    return receiver


async def _create_battery_receiver(
    component_id: int,
    microgrid_client: client.MicrogridApiClient,
) -> Receiver[Dict[str, Any]]:
    """Create a receiver for transformed metrics from a battery component.

    Args:
        component_id: component ID
        microgrid_client: microgrid client

    Returns:
        Receiver for transformed metrics.
    """
    recv_battery_data: Receiver[BatteryData] = await microgrid_client.battery_data(
        component_id
    )
    receiver = recv_battery_data.map(transform_battery_data)
    return receiver


async def gen_component_receivers(
    component_infos: List[ComponentInfo],
    microgrid_client: client.MicrogridApiClient,
) -> Dict[ComponentCategory, Receiver[Dict[str, Any]]]:
    """Generate receivers for all components grouped by component category.

    Group all components provided in `component_infos` by `ComponentCategory`
    and create a receiver for each group to collect data from the components
    belonging to that group.

    Args:
        component_infos: list of ComponentInfo for all the components from
            which to collect data from
        microgrid_client: microgrid client

    Returns:
        Receiver of the data per each component category.

    Raises:
        ValueError: if any of the receivers receives a metric from an unsupported
            type of component.
    """
    grouped_component_infos: Dict[ComponentCategory, List[ComponentInfo]] = {}
    for component_info in component_infos:
        if component_info.category not in grouped_component_infos:
            grouped_component_infos[component_info.category] = []
        grouped_component_infos[component_info.category].append(component_info)

    recv_components: Dict[ComponentCategory, Receiver[Dict[str, Any]]] = {}

    for component_category, infos in grouped_component_infos.items():
        receivers = []
        for component_info in infos:
            component_id = component_info.component_id
            if component_category == ComponentCategory.BATTERY:
                receiver = await _create_battery_receiver(
                    component_id, microgrid_client
                )
            elif component_category == ComponentCategory.INVERTER:
                receiver = await _create_inverter_receiver(
                    component_id, microgrid_client
                )
            elif component_category == ComponentCategory.EV_CHARGER:
                receiver = await _create_ev_charger_receiver(
                    component_id, microgrid_client
                )
            elif component_category in {
                ComponentCategory.METER,
                ComponentCategory.CHP,
                ComponentCategory.PV_ARRAY,
            }:
                receiver = await _create_meter_receiver(component_id, microgrid_client)
            else:
                raise ValueError(f"Unsupported type: {component_category}")

            receivers.append(receiver)

        recv_components[component_category] = Merge(*receivers)

    return recv_components
