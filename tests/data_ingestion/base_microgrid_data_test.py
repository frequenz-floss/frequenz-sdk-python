# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Base test class for the `MicrogridData`."""

from typing import Any, Dict, List, Optional, Set
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from frequenz.api.microgrid.battery_pb2 import Data as PbBatteryData
from frequenz.api.microgrid.battery_pb2 import Properties
from frequenz.api.microgrid.common_pb2 import AC, DC, Bounds, Metric, MetricAggregation
from frequenz.api.microgrid.inverter_pb2 import Data as PbInverterData
from frequenz.channels import Broadcast
from frequenz.channels.util import Select, Timer

import frequenz.sdk.microgrid.graph as gr
from frequenz.sdk._data_handling.time_series import TimeSeriesEntry
from frequenz.sdk._data_ingestion.constants import (
    METRIC_BATTERIES_ACTIVE_POWER,
    METRIC_BATTERIES_ACTIVE_POWER_BOUNDS,
    METRIC_BATTERIES_CAPACITY,
    METRIC_BATTERIES_REMAINING_ENERGY,
    METRIC_CLIENT_LOAD,
    METRIC_EV_CHARGERS_CONSUMPTION,
    METRIC_GRID_LOAD,
    METRIC_PV_PROD,
)
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.microgrid.connection import Connection

from ..utils.factories import (
    battery_data_factory,
    component_factory,
    connection_factory,
    ev_charger_data_factory,
    inverter_data_factory,
    meter_data_factory,
)


# pylint:disable=too-many-public-methods
class BaseMicrogridDataTest(IsolatedAsyncioTestCase):
    """Base Test class for MicrogridData() functionalities."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        """Set up a base microgrid data test."""
        super().__init__(*args, **kwargs)
        self.component_graph: gr.ComponentGraph = self._create_component_graph(
            self.components, self.connections
        )
        self.metrics: List[str] = [
            METRIC_CLIENT_LOAD,
            METRIC_PV_PROD,
            METRIC_BATTERIES_ACTIVE_POWER_BOUNDS,
            METRIC_BATTERIES_REMAINING_ENERGY,
            METRIC_BATTERIES_ACTIVE_POWER,
            METRIC_EV_CHARGERS_CONSUMPTION,
            METRIC_GRID_LOAD,
            METRIC_BATTERIES_CAPACITY,
        ]
        self.microgrid_client = MagicMock()
        self.microgrid_client.components = AsyncMock(
            side_effect=component_factory(self.components)
        )
        self.microgrid_client.connections = AsyncMock(
            side_effect=connection_factory(self.connections)
        )

    @property
    def components(self) -> Set[Component]:
        """Get components in the graph.

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
        """Get connections between components in the graph.

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

    @staticmethod
    async def _collect_microgrid_data(
        channels: List[Broadcast[TimeSeriesEntry[Any]]], metrics: List[str]
    ) -> Dict[str, List[TimeSeriesEntry[Any]]]:
        select = Select(
            timer=Timer(1.1),
            **{
                metric: channel.new_receiver()
                for metric, channel in zip(metrics, channels)
            },
        )
        returned_data: Dict[str, List[TimeSeriesEntry[Any]]] = {
            metric: [] for metric in metrics
        }
        while await select.ready():

            for metric in metrics:
                if msg := getattr(select, metric):
                    returned_data[metric].append(msg.inner)

            if select.timer:
                break
        return returned_data

    def _create_component_graph(
        self, components: Set[Component], connections: Set[Connection]
    ) -> gr.ComponentGraph:
        component_graph = (
            gr._MicrogridComponentGraph(  # pylint: disable=protected-access
                components, connections
            )
        )
        return component_graph

    def _get_component_count(self, component_category: ComponentCategory) -> int:
        return len(
            list(filter(lambda x: x.category == component_category, self.components))
        )

    @property
    def number_of_batteries(self) -> int:
        """Get number of batteries in the graph.

        Returns:
            number of PV arrays

        """
        return self._get_component_count(ComponentCategory.BATTERY)

    @property
    def number_of_inverters(self) -> int:
        """Get number of inverters in the graph.

        Returns:
            number of PV arrays

        """
        return self._get_component_count(ComponentCategory.INVERTER)

    @property
    def number_of_meters(self) -> int:
        """Get number of meters in the graph.

        Returns:
            number of PV arrays

        """
        return self._get_component_count(ComponentCategory.METER)

    @property
    def number_of_pv_arrays(self) -> int:
        """Get number of PV arrays in the graph.

        Returns:
            number of PV arrays

        """
        return self._get_component_count(ComponentCategory.PV_ARRAY)

    @property
    def number_of_ev_chargers(self) -> int:
        """Get number of EV chargers in the graph.

        Returns:
            number of EV chargers

        """
        return self._get_component_count(ComponentCategory.EV_CHARGER)

    def _get_value(self, fields: List[str], params: Dict[str, Any]) -> Optional[Any]:
        """Extract arbitrary value from the data that a component is sending.

        This is a helper method to easily get a specific value for a given component.

        `params` is a dictionary in which values can be dictionaries, protobuf
        messages or primitive types.

        Example:

            ``` python
            battery_data_params = dict(
                soc=Metric(now=2.0),
                capacity=Metric(now=1.0),
                dc_connection=DC(
                    power_consumption=Metric(
                        now=0.0,
                        system_bounds=Bounds(
                            lower=0.0, upper=55.0
                        ),
                    ),
                    power_supply=Metric(
                        now=0.0,
                        system_bounds=Bounds(lower=0.0, upper=10.0),
                    ),
                ),
            )

            _get_value('dc_connection.power_consumption.system_bounds.upper') translates to

            params.get('dc_connection').power_consumption.system_bounds.upper -> 55.0
            ```

        Args:
            fields: list of fields used to perform the value lookup
            params: data that a component is sending

        Returns:
            specific value that a component is sending, e.g. battery capacity
        """
        value = params.get(fields[0])
        for field in fields[1:]:
            if isinstance(value, dict):
                value = value.get(field)
            else:
                value = getattr(value, field)
        return value

    # pylint:disable=too-many-branches
    def _get_params(
        self, component_category: ComponentCategory, component_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get data sent by either a single component or all components of a given type.

        When all components of a given type are sending the same data, `component_id`
        is not required.

        Otherwise, providing `component_id` will return data sent only by the component
        that ID.

        Args:
            component_id: ID of the component, whose data should be returned
            component_category: type of the component

        Returns:
            data sent by the component(s)
        """
        if component_category == ComponentCategory.BATTERY:
            if component_id is None or component_id not in self.battery_data_overrides:
                params = self.battery_data_params
            else:
                params = self.battery_data_overrides[component_id]
        elif component_category == ComponentCategory.INVERTER:
            if component_id is None or component_id not in self.inverter_data_overrides:
                params = self.inverter_data_params
            else:
                params = self.inverter_data_overrides[component_id]
        elif component_category == ComponentCategory.METER:
            if component_id is None or component_id not in self.meter_data_overrides:
                params = self.meter_data_params
            else:
                params = self.meter_data_overrides[component_id]
        elif component_category == ComponentCategory.EV_CHARGER:
            if (
                component_id is None
                or component_id not in self.ev_charger_data_overrides
            ):
                params = self.ev_charger_data_params
            else:
                params = self.ev_charger_data_overrides[component_id]
        else:
            raise ValueError(f"Invalid component category: {component_category}!")
        return params

    def _get_component_param(
        self, name: str, component_category: ComponentCategory
    ) -> Optional[Any]:
        """Extract arbitrary value from the data that a component is sending.

        Args:
            name: dot-separated list of attributes
            component_category: type of the component

        Returns:
            specific value that a component is sending, e.g. battery capacity

        Raises:
            LookupError if the first segment of the `name` is an int (component_id) and
                the component with that ID doesn't exist in the graph.
        """
        fields = name.split(".")
        component_id = None
        try:
            component_id = int(fields[0])
            fields = fields[1:]
        except ValueError:
            pass

        if (
            component_id is not None
            and Component(component_id, component_category) not in self.components
        ):
            raise LookupError(
                f"Component of type [{component_category}] with ID [{component_id}] not found."
            )

        params = self._get_params(component_category, component_id)
        value = self._get_value(fields, params)
        return value

    def get_ev_charger_param(self, name: str) -> Optional[Any]:
        """Extract arbitrary value from the data that an ev charger is sending.

        Args:
            name: dot-separated list of attributes

        Returns:
            specific value that an ever charger is sending, e.g. current power supply
        """
        return self._get_component_param(name, ComponentCategory.EV_CHARGER)

    def get_battery_param(self, name: str) -> Optional[Any]:
        """Extract arbitrary value from the data that a battery is sending.

        Args:
            name: dot-separated list of attributes

        Returns:
            specific value that a battery is sending, e.g. battery capacity
        """
        return self._get_component_param(name, ComponentCategory.BATTERY)

    def get_inverter_param(self, name: str) -> Optional[Any]:
        """Extract arbitrary value from the data that a inverter is sending.

        Args:
            name: dot-separated list of attributes

        Returns:
            specific value that a inverter is sending, e.g. current power supply
        """
        return self._get_component_param(name, ComponentCategory.INVERTER)

    def get_meter_param(self, name: str) -> Optional[Any]:
        """Extract arbitrary value from the data that a meter is sending.

        Args:
            name: dot-separated list of attributes

        Returns:
            specific value that a meter is sending, e.g. current power supply
        """
        return self._get_component_param(name, ComponentCategory.METER)

    @property
    def battery_data_params(self) -> Dict[str, Any]:
        """Get parameters used to instantiate BatteryData sent by batteries.

        Returns:
            parameters for data that batteries will be sending

        """
        power_upper_bound = 20.0
        soc = 10.0
        capacity = 50.0

        battery_data_params = dict(
            properties=Properties(capacity=capacity),
            data=PbBatteryData(
                soc=MetricAggregation(avg=soc),
                dc=DC(
                    power=Metric(
                        value=0.0,
                        system_bounds=Bounds(lower=-10.0, upper=power_upper_bound),
                    )
                ),
            ),
        )
        return battery_data_params

    @property
    def battery_data_overrides(self) -> Dict[int, Any]:
        """Get overrides used to send BatteryData by arbitrary batteries.

        Returns:
            overrides for data that arbitrary batteries will be sending

        """
        return {}

    @property
    def inverter_data_params(self) -> Dict[str, Any]:
        """Get parameters used to instantiate InverterData sent by inverters.

        Returns:
            parameters for data that inverters will be sending

        """
        inverter_power_lower_bound = -999.0
        inverter_power_now = 255.0

        inverter_data_params = dict(
            data=PbInverterData(
                ac=AC(
                    power_active=Metric(
                        value=inverter_power_now,
                        system_bounds=Bounds(
                            lower=inverter_power_lower_bound, upper=1000.0
                        ),
                    )
                )
            )
        )
        return inverter_data_params

    @property
    def inverter_data_overrides(self) -> Dict[int, Any]:
        """Get overrides used to send InverterData by arbitrary inverters.

        Returns:
            overrides for data that arbitrary inverters will be sending

        """
        return {}

    @property
    def meter_data_params(self) -> Dict[str, Any]:
        """Get parameters used to instantiate MeterData sent by meters.

        Returns:
            parameters for data that meters will be sending

        """
        return {}

    @property
    def meter_data_overrides(self) -> Dict[int, Any]:
        """Get overrides used to send MeterData by arbitrary meters.

        Returns:
            overrides for data that arbitrary meters will be sending

        """
        return {}

    @property
    def ev_charger_data_params(self) -> Dict[str, Any]:
        """Get parameters used to instantiate EVChargerData sent by ev chargers.

        Returns:
            parameters for data that ev chargers will be sending

        """
        return {}

    @property
    def ev_charger_data_overrides(self) -> Dict[int, Any]:
        """Get overrides used to send EVChargerData by arbitrary ev chargers.

        Returns:
            overrides for data that arbitrary ev chargers will be sending

        """
        return {}

    def set_up_battery_data(
        self, microgrid_client: MagicMock, interval: float, timeout: float
    ) -> None:
        """Set up batteries to send fake data.

        Args:
            microgrid_client: instance of a `MicrogridGrpcClient`
            interval: how often batteries should send data
            timeout: when should batteries stop sending data
        """
        microgrid_client.battery_data = AsyncMock(
            side_effect=battery_data_factory(
                interval=interval,
                timeout=timeout,
                params=self.battery_data_params,
                overrides=self.battery_data_overrides,
            )
        )

    def set_up_inverter_data(
        self, microgrid_client: MagicMock, interval: float, timeout: float
    ) -> None:
        """Set up inverters to send fake data.

        Args:
            microgrid_client: instance of a `MicrogridGrpcClient`
            interval: how often inverters should send data
            timeout: when should inverters stop sending data
        """
        microgrid_client.inverter_data = AsyncMock(
            side_effect=inverter_data_factory(
                interval=interval,
                timeout=timeout,
                params=self.inverter_data_params,
                overrides=self.inverter_data_overrides,
            )
        )

    def set_up_meter_data(
        self, microgrid_client: MagicMock, interval: float, timeout: float
    ) -> None:
        """Set up meters to send fake data.

        Args:
            microgrid_client: instance of a `MicrogridGrpcClient`
            interval: how often meters should send data
            timeout: when should meters stop sending data
        """
        microgrid_client.meter_data = AsyncMock(
            side_effect=meter_data_factory(
                interval=interval,
                timeout=timeout,
                params=self.meter_data_params,
                overrides=self.meter_data_overrides,
            )
        )

    def set_up_ev_charger_data(
        self, microgrid_client: MagicMock, interval: float, timeout: float
    ) -> None:
        """Set up EV chargers to send fake data.

        Args:
            microgrid_client: instance of a `MicrogridGrpcClient`
            interval: how often EV chargers should send data
            timeout: when should EV chargers stop sending data
        """
        microgrid_client.ev_charger_data = AsyncMock(
            side_effect=ev_charger_data_factory(
                interval=interval,
                timeout=timeout,
                params=self.ev_charger_data_params,
                overrides=self.ev_charger_data_overrides,
            )
        )

    def _check_battery_total_energy(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        soc = self.get_battery_param("data.soc.avg")
        assert soc is not None

        capacity = self.get_battery_param("properties.capacity")
        assert capacity is not None

        returned_metric = returned_data[METRIC_BATTERIES_REMAINING_ENERGY]
        components_num = self.number_of_batteries

        assert 1 <= len(returned_metric) <= components_num * num_messages_per_component

        energy = soc * capacity
        expected_total_energy = energy * components_num

        total_energy_calculation_checks = [
            entry.value == expected_total_energy for entry in returned_metric
        ]

        msg = "Battery total energy expected to be 2 * SoC * Capacity"
        assert len(total_energy_calculation_checks) > 0
        assert all(total_energy_calculation_checks) is True, msg

    def _check_batteries_capacity(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        msg = "Total battery capacity should be equal to sum of individual capacities"

        capacity = self.get_battery_param("properties.capacity")
        assert capacity is not None

        components_num = self.number_of_inverters
        returned_metric = returned_data[METRIC_BATTERIES_CAPACITY]
        assert 1 <= len(returned_metric) <= num_messages_per_component * components_num

        expected_batteries_capacity: float = capacity * components_num
        batteries_capacity_calculation_checks = [
            entry.value == expected_batteries_capacity for entry in returned_metric
        ]
        assert len(batteries_capacity_calculation_checks) > 0
        assert all(batteries_capacity_calculation_checks) is True, msg

    def _check_inverter_active_power(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        msg = "Inverter active power expected to be (ConsumptionNow - SupplyNow) * 2"

        active_power = self.get_inverter_param("data.ac.power_active.value")
        assert active_power is not None

        components_num = self.number_of_inverters
        returned_metric = returned_data[METRIC_BATTERIES_ACTIVE_POWER]
        assert 1 <= len(returned_metric) <= num_messages_per_component * components_num

        expected_total_active_power: float = active_power * components_num
        active_power_calculation_checks = [
            entry.value == expected_total_active_power for entry in returned_metric
        ]
        assert len(active_power_calculation_checks) > 0
        assert all(active_power_calculation_checks) is True, msg

    def _check_active_power_lower_bound(
        self, lower_bound_metrics: List[float], num_messages_per_component: int
    ) -> None:
        msg = "Inverter Max Supply Rate expected to be -1 * PowerSupplyUpperBound * 2"
        inverter_power_lower_bound = self.get_inverter_param(
            "data.ac.power_active.system_bounds.lower"
        )
        assert inverter_power_lower_bound is not None

        battery_power_lower_bound = self.get_battery_param(
            "data.dc.power.system_bounds.lower"
        )
        assert battery_power_lower_bound is not None
        assert battery_power_lower_bound <= 0
        assert inverter_power_lower_bound <= 0

        power_lower_bound: float = max(
            inverter_power_lower_bound,
            battery_power_lower_bound,
        )

        components_num = self.number_of_batteries + self.number_of_inverters

        assert (
            1 <= len(lower_bound_metrics) <= components_num * num_messages_per_component
        )

        max_supply_rate_checks = [
            value == power_lower_bound * self.number_of_batteries
            for value in lower_bound_metrics
        ]
        assert len(max_supply_rate_checks) > 0
        assert all(max_supply_rate_checks) is True, msg

    def _check_active_power_upper_bound(
        self, upper_bound_metrics: List[float], num_messages_per_component: int
    ) -> None:
        msg = (
            "Battery Max Consumption Rate expected to be PowerConsumptionUpperBound * 2"
        )
        battery_power_consumption_upper_bound = self.get_battery_param(
            "data.dc.power.system_bounds.upper"
        )
        assert battery_power_consumption_upper_bound is not None

        inverter_power_consumption_upper_bound = self.get_inverter_param(
            "data.ac.power_active.system_bounds.upper"
        )
        assert inverter_power_consumption_upper_bound

        power_consumption_upper_bound: float = min(
            battery_power_consumption_upper_bound,
            inverter_power_consumption_upper_bound,
        )

        components_num = self.number_of_batteries + self.number_of_inverters

        assert (
            1 <= len(upper_bound_metrics) <= components_num * num_messages_per_component
        )

        max_consumption_rate_calculation_checks = [
            value == power_consumption_upper_bound * self.number_of_batteries
            for value in upper_bound_metrics
        ]
        assert len(max_consumption_rate_calculation_checks) > 0
        assert all(max_consumption_rate_calculation_checks) is True, msg

    def _check_active_power_bounds(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        self._check_active_power_lower_bound(
            lower_bound_metrics=[
                metric.value[0]
                for metric in returned_data[METRIC_BATTERIES_ACTIVE_POWER_BOUNDS]
            ],
            num_messages_per_component=num_messages_per_component,
        )
        self._check_active_power_upper_bound(
            upper_bound_metrics=[
                metric.value[1]
                for metric in returned_data[METRIC_BATTERIES_ACTIVE_POWER_BOUNDS]
            ],
            num_messages_per_component=num_messages_per_component,
        )

    def _check_grid_load_meter_by_client_side(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        # pylint: disable=too-many-locals
        expected_grid_load = 0

        components_num = 0

        ev_chargers = filter(
            lambda x: x.category == ComponentCategory.EV_CHARGER, self.components
        )
        for ev_charger_id in {ev_charger.component_id for ev_charger in ev_chargers}:
            components_num += 1

            ev_charger_power_now = self.get_ev_charger_param(
                f"{ev_charger_id}.data.ac.power_active.value"
            )
            assert ev_charger_power_now is not None

            expected_grid_load += ev_charger_power_now

        inverters = filter(
            lambda x: x.category == ComponentCategory.INVERTER, self.components
        )
        for inverter_id in {inverter.component_id for inverter in inverters}:
            components_num += 1
            inverter_power_now = self.get_inverter_param(
                f"{inverter_id}.data.ac.power_active.value"
            )
            assert inverter_power_now is not None
            expected_grid_load += inverter_power_now

        meters = filter(
            lambda x: x.category == ComponentCategory.METER, self.components
        )
        for meter_id in {meter.component_id for meter in meters}:
            successors = self.component_graph.successors(meter_id)
            successor_types = {component.category for component in successors}
            if (
                ComponentCategory.INVERTER in successor_types
                or ComponentCategory.EV_CHARGER in successor_types
            ):
                continue

            components_num += 1
            meter_power_now = self.get_meter_param(
                f"{meter_id}.data.ac.power_active.value"
            )
            assert meter_power_now is not None
            expected_grid_load += meter_power_now

        returned_metric = returned_data[METRIC_GRID_LOAD]
        assert 1 <= len(returned_metric) <= components_num * num_messages_per_component

        msg = """Grid Load expected to be the sum of active_power from all inverters
                 and meters that are not connected to an inverter"""

        # For simplicity only check if total values are correct.
        # We check whether the first values are correct in other tests.
        client_load_calculation_checks = [
            entry.value == expected_grid_load for entry in returned_metric
        ]
        assert len(client_load_calculation_checks) > 0
        assert all(client_load_calculation_checks) is True, msg

    def _check_client_load_meter_by_client_side(
        self, returned_data: Dict[str, List[Any]], num_messages_per_component: int
    ) -> None:
        load_meter_id = 4
        load_meter_power = self.get_meter_param(
            f"{load_meter_id}.data.ac.power_active.value"
        )
        assert load_meter_power is not None

        returned_metric = returned_data[METRIC_CLIENT_LOAD]

        assert 1 <= len(returned_metric) <= num_messages_per_component

        msg = "Meter[ID=4] Client Load expected to be -1 * MeterPowerSupply + MeterPowerConsumption"

        client_load_calculation_checks = [
            entry.value == load_meter_power for entry in returned_metric
        ]
        assert len(client_load_calculation_checks) > 0
        assert all(client_load_calculation_checks) is True, msg
