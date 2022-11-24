# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Test for the `MicrogridData` when meter is on the grid's side
"""

from typing import Any, Dict, List, Set

from frequenz.api.microgrid.common_pb2 import AC, Metric
from frequenz.channels import Broadcast

from frequenz.sdk._data_handling.time_series import TimeSeriesEntry
from frequenz.sdk._data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk._data_ingestion.microgrid_data import MicrogridData
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory

from .base_microgrid_data_test import BaseMicrogridDataTest


# pylint:disable=too-many-locals
class TestMicrogridDataGridLoad(BaseMicrogridDataTest):
    """Test scenario of MicrogridData when graph has a meter on the grid's side"""

    @property
    def components(self) -> Set[Component]:
        return {
            Component(1, ComponentCategory.GRID),
            Component(3, ComponentCategory.JUNCTION),
            Component(4, ComponentCategory.METER),
            Component(5, ComponentCategory.METER),
            Component(6, ComponentCategory.PV_ARRAY),
            Component(7, ComponentCategory.METER),
            Component(8, ComponentCategory.INVERTER),
            Component(9, ComponentCategory.BATTERY),
            Component(10, ComponentCategory.METER),
            Component(11, ComponentCategory.INVERTER),
            Component(12, ComponentCategory.BATTERY),
            Component(13, ComponentCategory.METER),
            Component(14, ComponentCategory.EV_CHARGER),
            Component(15, ComponentCategory.METER),
            Component(16, ComponentCategory.CHP),
        }

    @property
    def connections(self) -> Set[Connection]:
        return {
            Connection(1, 3),
            Connection(3, 4),
            Connection(3, 5),
            Connection(5, 6),
            Connection(3, 7),
            Connection(7, 8),
            Connection(8, 9),
            Connection(3, 10),
            Connection(10, 11),
            Connection(11, 12),
            Connection(3, 13),
            Connection(13, 14),
            Connection(3, 15),
            Connection(15, 16),
        }

    @property
    def ev_charger_data_params(self) -> Dict[str, Any]:
        """Get parameters used to instantiate EVChargerData sent by ev chargers.

        Returns:
            parameters for data that ev chargers will be sending

        """
        load_meter_power = 20.0

        ev_charger_data_params = dict(
            data=dict(ac=AC(power_active=Metric(value=load_meter_power)))
        )
        return ev_charger_data_params

    @property
    def meter_data_params(self) -> Dict[str, Any]:
        """Get parameters used to instantiate MeterData sent by meters.

        Returns:
            parameters for data that meters will be sending

        """
        load_meter_power = -444.0

        meter_data_params = dict(
            data=dict(ac=AC(power_active=Metric(value=load_meter_power)))
        )
        return meter_data_params

    @property
    def meter_data_overrides(self) -> Dict[int, Any]:
        """Get overrides used to send MeterData by arbitrary meters.

        Returns:
            overrides for data that arbitrary meters will be sending

        """
        ev_meter_power = 120.0

        meter_data_overrides = {
            13: dict(data=dict(ac=AC(power_active=Metric(value=ev_meter_power)))),
        }
        return meter_data_overrides

    # pylint: disable=too-many-locals,too-many-statements
    async def test_microgrid_data_with_ev_charger(
        self,
    ) -> None:
        """Test collecting data from microgrid and applying formulas

        Create a valid graph and make components send arbitrary data, which is
        then used to check if formulas returned expected results.

        In this scenario, meter is on the grid's side.
        """
        timeout = 1.0
        interval = 0.2

        self.set_up_battery_data(self.microgrid_client, interval, timeout)
        self.set_up_meter_data(self.microgrid_client, interval, timeout)
        self.set_up_inverter_data(self.microgrid_client, interval, timeout)
        self.set_up_ev_charger_data(self.microgrid_client, interval, timeout)

        channels: List[Broadcast[TimeSeriesEntry[Any]]] = [
            Broadcast[TimeSeriesEntry[Any]](metric) for metric in self.metrics
        ]

        formula_calculator = FormulaCalculator(
            component_graph=self.component_graph,
        )

        microgrid_actor = MicrogridData(
            self.microgrid_client,
            self.component_graph,
            {
                metric: channel.new_sender()
                for metric, channel in zip(self.metrics, channels)
            },
            formula_calculator=formula_calculator,
            wait_for_data_sec=0.4,
        )
        assert microgrid_actor is not None

        returned_data = await self._collect_microgrid_data(channels, self.metrics)
        num_messages_per_component = int(timeout / interval)

        self._check_active_power_bounds(returned_data, num_messages_per_component)
        self._check_inverter_active_power(returned_data, num_messages_per_component)
        self._check_battery_total_energy(returned_data, num_messages_per_component)
        self._check_client_load_meter_by_client_side(
            returned_data, num_messages_per_component
        )
        self._check_grid_load_meter_by_client_side(
            returned_data, num_messages_per_component
        )
        self._check_batteries_capacity(returned_data, num_messages_per_component)

        # pylint: disable=protected-access,no-member
        await microgrid_actor._stop()  # type: ignore
