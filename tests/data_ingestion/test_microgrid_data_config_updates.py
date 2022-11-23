# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Test for the `MicrogridData` when config manager fires an update about Config change

Copyright © 2022 Frequenz Energy-as-a-Service GmbH.  All rights reserved.
"""
import asyncio
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Set
from unittest.mock import AsyncMock

from frequenz.api.microgrid.common_pb2 import AC, Metric
from frequenz.channels import Broadcast

from frequenz.sdk.actor import ConfigManagingActor
from frequenz.sdk.config import Config
from frequenz.sdk.data_handling.time_series import TimeSeriesEntry
from frequenz.sdk.data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk.data_ingestion.microgrid_data import MicrogridData
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.microgrid.connection import Connection
from tests.data_ingestion.base_microgrid_data_test import BaseMicrogridDataTest


# pylint:disable=too-many-locals
class TestMicrogridDataConfigUpdates(BaseMicrogridDataTest):
    """Test scenario of MicrogridData when config file changes"""

    conf_content = """
    logging_lvl = 'DEBUG'
    var1 = "1"
    """

    @property
    def components(self) -> Set[Component]:
        return {
            Component(1, ComponentCategory.GRID),
            Component(3, ComponentCategory.JUNCTION),
            Component(4, ComponentCategory.METER),
            Component(8, ComponentCategory.INVERTER),
            Component(9, ComponentCategory.BATTERY),
            Component(5, ComponentCategory.METER),
            Component(6, ComponentCategory.PV_ARRAY),
            Component(10, ComponentCategory.METER),
            Component(11, ComponentCategory.EV_CHARGER),
        }

    @property
    def connections(self) -> Set[Connection]:
        return {
            Connection(1, 3),
            Connection(3, 4),
            Connection(3, 8),
            Connection(8, 9),
            Connection(3, 5),
            Connection(5, 6),
            Connection(3, 10),
            Connection(10, 11),
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

        self.set_up_battery_data(self.microgrid_client, interval, timeout)
        self.set_up_meter_data(self.microgrid_client, interval, timeout)
        self.set_up_inverter_data(self.microgrid_client, interval, timeout)
        self.set_up_ev_charger_data(self.microgrid_client, interval, timeout)

        config_update_channel: Broadcast[Config] = Broadcast(
            "microgrid_data_config_update_channel"
        )

        # Default temp directory needs to be changed, because otherwise it will depend
        # on platform settings and for some platforms (at least for Mac OS X), the
        # `watchfiles` library used for monitoring files, for some reason adds a prefix
        # "/private", which causes paths not to be equal, i.e. a change to
        # "/tmp/tmp7luc2x0f" is reported as a change to "/private/tmp/tmp7luc2x0f"
        with NamedTemporaryFile(delete=True, dir=".") as config_file:
            self._write_to_file(config_file.name, self.conf_content)

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
                wait_for_data_sec=0.4,
            )
            microgrid_actor._reinitialize = AsyncMock()  # type: ignore

            # Update the config file after 0.2 sec
            await asyncio.sleep(0.2)
            new_config_content = self.conf_content.replace("DEBUG", "ERROR")
            self._write_to_file(config_file.name, new_config_content)

            returned_data = await self._collect_microgrid_data(channels, self.metrics)

            expected_config = dict(
                logging_lvl="ERROR",
                var1="1",
            )
            actual_updated_config = (
                microgrid_actor._reinitialize.call_args_list[-1].args[0]._conf_store
            )
            assert microgrid_actor._reinitialize.called
            assert expected_config == actual_updated_config

            for metric in self.metrics:
                assert len(returned_data[metric]) > 0

            # pylint: disable=protected-access,no-member
            await microgrid_actor._stop()  # type: ignore
