# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Test for the `MicrogridData` when meter is on the grid's side
"""
from typing import Any, Dict, List, Set

from frequenz.api.microgrid.common_pb2 import AC, Metric
from frequenz.channels import Broadcast

from frequenz.sdk._data_handling.time_series import TimeSeriesEntry
from frequenz.sdk._data_ingestion.constants import METRIC_PV_PROD
from frequenz.sdk._data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk._data_ingestion.microgrid_data import MicrogridData
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory

from .data_ingestion.base_microgrid_data_test import BaseMicrogridDataTest


# pylint:disable=too-many-locals
class TestMicrogridDataWithCustomFormula(BaseMicrogridDataTest):
    """Test if MicrogridData computes formulas that are unnecessary"""

    @property
    def components(self) -> Set[Component]:
        return {
            Component(1, ComponentCategory.GRID),
            Component(3, ComponentCategory.JUNCTION),
            Component(4, ComponentCategory.METER),
            Component(8, ComponentCategory.INVERTER),
            Component(9, ComponentCategory.BATTERY),
        }

    @property
    def connections(self) -> Set[Connection]:
        return {
            Connection(1, 3),
            Connection(3, 4),
            Connection(3, 8),
            Connection(8, 9),
        }

    @property
    def meter_data_params(self) -> Dict[str, Any]:
        """Get parameters used to instantiate MeterData sent by meters.

        Returns:
            parameters for data that meters will be sending

        """
        load_meter_power = -200.0

        meter_data_params = dict(
            ac_connection=AC(power_active=Metric(value=load_meter_power))
        )
        return meter_data_params

    # pylint: disable=too-many-locals,too-many-statements
    async def test_microgrid_data_with_custom_metric(
        self,
    ) -> None:
        """Verify that MicrogridData doesn't compute unnecessary formulas"""

        channels: List[Broadcast[TimeSeriesEntry[Any]]] = [
            Broadcast[TimeSeriesEntry[Any]](metric) for metric in self.metrics
        ]

        timeout = 3.0
        interval = 0.2

        self.set_up_battery_data(self.microgrid_client, interval, timeout)
        self.set_up_meter_data(self.microgrid_client, interval, timeout)
        self.set_up_inverter_data(self.microgrid_client, interval, timeout)
        self.set_up_ev_charger_data(self.microgrid_client, interval, timeout)

        formula_calculator = FormulaCalculator(self.component_graph)

        microgrid_actor = MicrogridData(
            microgrid_client=self.microgrid_client,
            component_graph=self.component_graph,
            outputs={
                metric: channel.new_sender()
                for metric, channel in zip(self.metrics, channels)
            },
            formula_calculator=formula_calculator,
        )

        returned_data = await self._collect_microgrid_data(channels, self.metrics)

        assert len(returned_data[METRIC_PV_PROD]) == 0

        # pylint: disable=protected-access,no-member
        await microgrid_actor._stop()  # type: ignore
