"""
Tests for the `MicrogridData` actor

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Receiver, Select, Timer

from frequenz.sdk.data_ingestion.component_info import ComponentInfo
from frequenz.sdk.data_ingestion.gen_component_receivers import gen_component_receivers
from frequenz.sdk.microgrid.component import Component, ComponentCategory

from ..utils.factories import (
    battery_data_factory,
    component_factory,
    inverter_data_factory,
    meter_data_factory,
)


async def test_gen_component_receivers() -> None:
    """Test gen_component_receivers() functionalities."""

    component_infos = [
        ComponentInfo(4, ComponentCategory.METER),
        ComponentInfo(8, ComponentCategory.BATTERY),
        ComponentInfo(9, ComponentCategory.INVERTER),
    ]
    components = {
        Component(info.component_id, info.category) for info in component_infos
    }
    timeout = 1.0

    microgrid_client = MagicMock()
    microgrid_client.components = AsyncMock(side_effect=component_factory(components))
    microgrid_client.battery_data = AsyncMock(
        side_effect=battery_data_factory(timeout=timeout)
    )
    microgrid_client.inverter_data = AsyncMock(
        side_effect=inverter_data_factory(timeout=timeout)
    )
    microgrid_client.meter_data = AsyncMock(
        side_effect=meter_data_factory(timeout=timeout)
    )

    recv_components = await gen_component_receivers(component_infos, microgrid_client)
    channels: Dict[str, Receiver[Dict[str, Any]]] = {
        component_category.name: receiver
        for component_category, receiver in recv_components.items()
    }
    select = Select(timer=Timer(1.0), **channels)
    assert recv_components is not None
    returned_components = set()
    while await select.ready():
        if msg := getattr(select, ComponentCategory.BATTERY.name):
            returned_components.add(msg.inner["id"])
            assert "id" in list(msg.inner.keys())
            assert "timestamp" in list(msg.inner.keys())
            assert "soc" in list(msg.inner.keys())
            assert "capacity" in list(msg.inner.keys())
            assert "power_upper_bound" in list(msg.inner.keys())
            assert "power_lower_bound" in list(msg.inner.keys())
        elif msg := getattr(select, ComponentCategory.METER.name):
            returned_components.add(msg.inner["id"])
            assert "id" in list(msg.inner.keys())
            assert "timestamp" in list(msg.inner.keys())
            assert "active_power" in list(msg.inner.keys())
        elif msg := getattr(select, ComponentCategory.INVERTER.name):
            returned_components.add(msg.inner["id"])
            assert "id" in list(msg.inner.keys())
            assert "timestamp" in list(msg.inner.keys())
            assert "active_power" in list(msg.inner.keys())
            assert "active_power_upper_bound" in list(msg.inner.keys())
            assert "active_power_lower_bound" in list(msg.inner.keys())
        elif select.timer:
            break
    assert returned_components == {4, 8, 9}
