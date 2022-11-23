# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Test for the `MicrogridData` when config manager fires an update about Config change

Copyright © 2022 Frequenz Energy-as-a-Service GmbH.  All rights reserved.
"""
import asyncio
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Set

from frequenz.channels import Broadcast

from frequenz.sdk.actor import ConfigManagingActor
from frequenz.sdk.config import Config
from frequenz.sdk.data_handling.time_series import TimeSeriesEntry
from frequenz.sdk.data_ingestion.constants import METRIC_BATTERIES_CAPACITY
from frequenz.sdk.data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk.data_ingestion.microgrid_data import MicrogridData
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.microgrid.connection import Connection
from tests.data_ingestion.base_microgrid_data_test import BaseMicrogridDataTest


# pylint:disable=too-many-locals
class TestMicrogridDataConfigFormulaUpdates(BaseMicrogridDataTest):
    """Test scenario of MicrogridData when config file updates formulas"""

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
            Component(14, ComponentCategory.INVERTER),
            Component(15, ComponentCategory.BATTERY),
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
            Connection(14, 15),
        }

    def _check_selected_batteries_capacity(
        self, returned_data: Dict[str, List[Any]], included_battery_ids: Set[int]
    ) -> None:
        initial_capacity = self.get_battery_param("properties.capacity")
        assert initial_capacity is not None
        initial_expected_batteries_capacity = (
            self.number_of_batteries * initial_capacity
        )

        final_expected_batteries_capacity = 0
        for battery_id in included_battery_ids:
            capacity = self.get_battery_param(f"{battery_id}.properties.capacity")
            assert capacity is not None
            final_expected_batteries_capacity += capacity

        returned_metric = returned_data[METRIC_BATTERIES_CAPACITY]

        # Before config update, the total battery capacity should be higher
        initial_entries = list(
            filter(
                lambda x: x.value == initial_expected_batteries_capacity,
                returned_metric,
            )
        )
        assert len(initial_entries) > 0

        # After config update, the total battery capacity should be lower and
        # timestamps of the entries should be increasing
        final_entries = list(
            filter(
                lambda x: x.value == final_expected_batteries_capacity, returned_metric
            )
        )
        assert len(final_entries) > 0
        for entry in final_entries:
            assert entry.timestamp >= initial_entries[-1].timestamp
            assert entry.value == final_expected_batteries_capacity

    def _write_to_file(self, path: str, content: str) -> None:
        with open(path, "wb") as file:
            file.write(content.encode("utf-8"))

    # pylint: disable=too-many-locals,too-many-statements,protected-access
    async def test_microgrid_data_config_updates(self) -> None:
        """Check if MicrogridData is working as expected after a config file update"""

        channels: List[Broadcast[TimeSeriesEntry[Any]]] = [
            Broadcast[TimeSeriesEntry[Any]](metric) for metric in self.metrics
        ]

        timeout = 1.0
        interval = 0.2

        self.set_up_battery_data(self.microgrid_client, 0.05, timeout)
        self.set_up_meter_data(self.microgrid_client, interval, timeout)
        self.set_up_inverter_data(self.microgrid_client, interval, timeout)

        config_update_channel: Broadcast[Config] = Broadcast(
            "microgrid_data_config_update_channel"
        )

        config_file_initial_content = """
        logging_lvl = 'DEBUG'
        """

        config_file_updated_content = """
        logging_lvl = 'ERROR'
        formula_batteries_capacity = '[9,12]'
        """

        with NamedTemporaryFile(delete=True, dir=".") as config_file:
            self._write_to_file(config_file.name, config_file_initial_content)

            _config_manager = ConfigManagingActor(
                conf_file=config_file.name, output=config_update_channel.new_sender()
            )
            formula_calculator = FormulaCalculator(self.component_graph)

            microgrid_actor = MicrogridData(
                microgrid_client=self.microgrid_client,
                component_graph=self.component_graph,
                outputs={
                    metric: channel.new_sender()
                    for metric, channel in zip(self.metrics, channels)
                },
                formula_calculator=formula_calculator,
                config_update_receiver=config_update_channel.new_receiver(),
                wait_for_data_sec=0.01,
                formula_update_interval_sec=0.05,
            )

            # Update the config file after 0.7 sec
            await asyncio.sleep(0.7)
            self._write_to_file(config_file.name, config_file_updated_content)

            returned_data = await self._collect_microgrid_data(channels, self.metrics)

            self._check_selected_batteries_capacity(returned_data, {9, 12})

            # pylint: disable=protected-access,no-member
            await microgrid_actor._stop()  # type: ignore
