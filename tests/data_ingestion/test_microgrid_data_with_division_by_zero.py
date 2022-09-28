"""
Test for the `MicrogridData` when one formula crashes

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
from typing import Any, Dict, List, Set

from frequenz.api.microgrid.common_pb2 import AC, Metric
from frequenz.channels import Broadcast

from frequenz.sdk.data_handling.time_series import (
    InverterField,
    MeterField,
    SymbolComponentCategory,
    SymbolMapping,
    TimeSeriesEntry,
    TimeSeriesFormula,
)
from frequenz.sdk.data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk.data_ingestion.microgrid_data import MicrogridData
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.microgrid.connection import Connection

from .base_microgrid_data_test import BaseMicrogridDataTest


# pylint:disable=too-many-locals
class TestMicrogridDataWithZeroDivision(BaseMicrogridDataTest):
    """Test scenario of MicrogridData with crashing formula"""

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

        In this test, send 0 on purpose to cause ZeroDivision error.

        Returns:
            parameters for data that meters will be sending

        """
        load_meter_power = 0.0

        meter_data_params = dict(
            data=dict(ac=AC(power_active=Metric(value=load_meter_power)))
        )
        return meter_data_params

    async def test_microgrid_data_with_division_by_zero(
        self,
    ) -> None:
        """Check if other formulas are correctly calculated when one formula crashes"""

        custom_metric_name = "custom_metric"
        metrics = self.metrics + [custom_metric_name]
        channels: List[Broadcast[TimeSeriesEntry[Any]]] = [
            Broadcast[TimeSeriesEntry[Any]](metric) for metric in metrics
        ]

        meters = [
            comp
            for comp in self.components
            if comp.category in {ComponentCategory.METER}
        ]

        inverters = [
            comp
            for comp in self.components
            if comp.category in {ComponentCategory.INVERTER}
        ]

        meter_symbols = [f"meter_{comp.component_id}_active_power" for comp in meters]
        inverter_symbols = [
            f"inverter_{comp.component_id}_active_power" for comp in inverters
        ]
        formula: TimeSeriesFormula[Any] = TimeSeriesFormula(
            " + ".join(inverter_symbols) + " / " + " + ".join(meter_symbols)
        )
        microgrid_formulas: Dict[str, TimeSeriesFormula[Any]] = {
            custom_metric_name: formula
        }

        symbol_mappings: List[SymbolMapping] = [
            SymbolMapping(
                SymbolComponentCategory.METER,
                meter.component_id,
                MeterField.ACTIVE_POWER,
            )
            for meter in meters
        ] + [
            SymbolMapping(
                SymbolComponentCategory.INVERTER,
                inverter.component_id,
                InverterField.ACTIVE_POWER,
            )
            for inverter in inverters
        ]

        timeout = 1.0
        interval = 0.2

        self.set_up_battery_data(self.microgrid_client, interval, timeout)
        self.set_up_meter_data(self.microgrid_client, interval, timeout)
        self.set_up_inverter_data(self.microgrid_client, interval, timeout)
        self.set_up_ev_charger_data(self.microgrid_client, interval, timeout)

        formula_calculator = FormulaCalculator(
            component_graph=self.component_graph,
            additional_formulas=microgrid_formulas,
            symbol_mappings=symbol_mappings,
        )

        microgrid_actor = MicrogridData(
            microgrid_client=self.microgrid_client,
            component_graph=self.component_graph,
            outputs={
                metric: channel.get_sender()
                for metric, channel in zip(metrics, channels)
            },
            formula_calculator=formula_calculator,
        )

        await self._collect_microgrid_data(channels, metrics)

        # pylint: disable=protected-access,no-member
        await microgrid_actor._stop()  # type: ignore
