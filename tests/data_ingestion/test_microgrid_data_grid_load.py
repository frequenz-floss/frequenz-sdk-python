# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Test for the `MicrogridData` when meter is on the grid's side
"""

from typing import Any, Dict, List, Set

from frequenz.api.microgrid.common_pb2 import AC, Metric
from frequenz.channels import Broadcast

from frequenz.sdk.data_handling.time_series import TimeSeriesEntry
from frequenz.sdk.data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk.data_ingestion.microgrid_data import MicrogridData
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.microgrid.connection import Connection

from .base_microgrid_data_test import BaseMicrogridDataTest


class TestMicrogridDataGridLoad(BaseMicrogridDataTest):
    """Test scenario of MicrogridData when graph has a meter on the grid's side"""

    @property
    def components(self) -> Set[Component]:
        return {
            Component(1, ComponentCategory.GRID),
            Component(2, ComponentCategory.METER),
            Component(3, ComponentCategory.JUNCTION),
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
        return {
            Connection(1, 2),
            Connection(2, 3),
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
    def meter_data_overrides(self) -> Dict[int, Any]:
        """Get overrides used to send MeterData by arbitrary meters

        Returns:
            overrides for data that arbitrary meters will be sending

        """
        load_meter_power = -444.0

        pv_meter_power = -555.0

        meter_data_overrides = {
            2: dict(data=dict(ac=AC(power_active=Metric(value=load_meter_power)))),
            5: dict(data=dict(ac=AC(power_active=Metric(value=pv_meter_power)))),
        }
        return meter_data_overrides

    def _check_grid_load(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        load_meter_id = 2
        load_meter_active_power = self.get_meter_param(
            f"{load_meter_id}.data.ac.power_active"
        )
        assert load_meter_active_power is not None

        returned_metric = returned_data["grid_load"]
        assert 1 <= len(returned_metric) <= num_messages_per_component

        msg = """Meter[ID=2] Grid Load expected to be active_power of the meter
                 adjacent to the grid"""
        client_load_calculation_checks = [
            entry.value == load_meter_active_power.value
            for entry in returned_data["grid_load"]
        ]
        assert len(client_load_calculation_checks) > 0
        assert all(client_load_calculation_checks) is True, msg

    def _check_client_load(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        # pylint: disable=too-many-locals
        expected_client_load = 0

        components_num = 0

        inverters = filter(
            lambda x: x.category == ComponentCategory.INVERTER, self.components
        )
        for inverter_id in {inverter.component_id for inverter in inverters}:
            components_num += 1
            inverter_power_active = self.get_inverter_param(
                f"{inverter_id}.data.ac.power_active"
            )
            assert inverter_power_active is not None
            expected_client_load += inverter_power_active.value

        meters = filter(
            lambda x: x.category == ComponentCategory.METER, self.components
        )
        for meter_id in {meter.component_id for meter in meters}:
            successors = self.component_graph.successors(meter_id)
            successor_types = {component.category for component in successors}
            if ComponentCategory.INVERTER in successor_types:
                continue

            components_num += 1

            meter_active_power = self.get_meter_param(
                f"{meter_id}.data.ac.power_active"
            )
            assert meter_active_power is not None
            expected_client_load += meter_active_power.value

        returned_metric = returned_data["client_load"]
        assert 1 <= len(returned_metric) <= components_num * num_messages_per_component

        msg = """Client Load expected to be the sum of active_power from all inverters
                 and meters that are not connected to an inverter"""
        client_load_calculation_checks = [
            entry.value == expected_client_load for entry in returned_metric
        ]
        assert len(client_load_calculation_checks) > 0
        assert all(client_load_calculation_checks) is True, msg

    # pylint: disable=too-many-locals,too-many-statements
    async def test_microgrid_data_grid_load(
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

        returned_data = await self._collect_microgrid_data(channels, self.metrics)

        num_messages_per_component = int(timeout / interval)
        self._check_active_power_bounds(returned_data, num_messages_per_component)
        self._check_inverter_active_power(returned_data, num_messages_per_component)
        self._check_battery_total_energy(returned_data, num_messages_per_component)
        self._check_client_load(returned_data, num_messages_per_component)
        self._check_grid_load(returned_data, num_messages_per_component)
        self._check_batteries_capacity(returned_data, num_messages_per_component)

        # pylint: disable=protected-access,no-member
        await microgrid_actor._stop()  # type: ignore
