# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""
Tests for the `MicrogridData` actor
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


class TestMicrogridData(BaseMicrogridDataTest):
    """Test scenario of MicrogridData when graph has a meter on the client's side"""

    @property
    def components(self) -> Set[Component]:
        """Get components in the graph

        Override this method to create a graph with different components.

        Returns:
            set of components in graph
        """
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
        }

    @property
    def connections(self) -> Set[Connection]:
        """Get connections between components in the graph

        Override this method to create a graph with different connections.

        Returns:
            set of connections between components in graph
        """
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
        }

    @property
    def meter_data_params(self) -> Dict[str, Any]:
        """Get parameters used to instantiate MeterData sent by meters.

        Returns:
            parameters for data that meters will be sending

        """
        meter_power = -150.0

        meter_data_params = dict(
            data=dict(ac=AC(power_active=Metric(value=meter_power)))
        )
        return meter_data_params

    @property
    def meter_data_overrides(self) -> Dict[int, Any]:
        """Get overrides used to send MeterData by arbitrary meters

        Returns:
            overrides for data that arbitrary meters will be sending

        """
        load_meter_power = 444.0

        pv_meter_power = -555.0

        meter_data_overrides = {
            4: dict(data=dict(ac=AC(power_active=Metric(value=load_meter_power)))),
            5: dict(data=dict(ac=AC(power_active=Metric(value=pv_meter_power)))),
        }
        return meter_data_overrides

    def _check_pv_meter(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        pv_meter_id = 5
        pv_meter_power = self.get_meter_param(
            f"{pv_meter_id}.data.ac.power_active.value"
        )
        assert pv_meter_power is not None
        msg = "Meter[ID=5] PV Prod expected to be -1 * MeterPowerSupply + MeterPowerConsumption"
        pv_prod_calculation_checks = [
            entry.value == pv_meter_power for entry in returned_data["pv_prod"]
        ]
        assert 0 < len(pv_prod_calculation_checks) <= num_messages_per_component
        assert all(pv_prod_calculation_checks) is True, msg

    def _check_load_meter(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        load_meter_id = 4
        load_meter_power = self.get_meter_param(
            f"{load_meter_id}.data.ac.power_active.value"
        )
        assert load_meter_power is not None
        msg = "Meter[ID=4] Client Load expected to be -1 * MeterPowerSupply + MeterPowerConsumption"
        client_load_calculation_checks = [
            entry.value == load_meter_power for entry in returned_data["client_load"]
        ]
        assert 0 < len(client_load_calculation_checks) <= num_messages_per_component
        assert all(client_load_calculation_checks) is True, msg

    # pylint: disable=too-many-locals,too-many-statements
    async def test_microgrid_data(
        self,
    ) -> None:
        """Test collecting data from microgrid and applying formulas

        Create a valid graph and make components send arbitrary data, which is
        then used to check if formulas returned expected results.

        In this scenario, meter is on the client's side.
        """
        timeout = 1.0
        interval = 0.2

        self.set_up_battery_data(self.microgrid_client, interval, timeout)
        self.set_up_meter_data(self.microgrid_client, interval, timeout)
        self.set_up_inverter_data(self.microgrid_client, interval, timeout)

        channels: List[Broadcast[TimeSeriesEntry[Any]]] = [
            Broadcast[TimeSeriesEntry[Any]](metric) for metric in self.metrics
        ]

        formula_calculator = FormulaCalculator(self.component_graph)

        microgrid_actor = MicrogridData(
            self.microgrid_client,
            self.component_graph,
            {
                metric: channel.new_sender()
                for metric, channel in zip(self.metrics, channels)
            },
            formula_calculator,
            wait_for_data_sec=0.4,
        )
        assert microgrid_actor is not None
        for metric in self.metrics:
            assert metric in microgrid_actor.formula_calculator.microgrid_formulas

        returned_data = await self._collect_microgrid_data(channels, self.metrics)

        num_messages_per_component = int(timeout / interval)
        self._check_load_meter(returned_data, num_messages_per_component)
        self._check_pv_meter(returned_data, num_messages_per_component)
        self._check_active_power_bounds(returned_data, num_messages_per_component)
        self._check_inverter_active_power(returned_data, num_messages_per_component)
        self._check_battery_total_energy(returned_data, num_messages_per_component)
        self._check_grid_load_meter_by_client_side(
            returned_data, num_messages_per_component
        )
        self._check_client_load_meter_by_client_side(
            returned_data, num_messages_per_component
        )
        self._check_batteries_capacity(returned_data, num_messages_per_component)
        # pylint: disable=protected-access,no-member
        await microgrid_actor._stop()  # type: ignore
