# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Test for the `MicrogridData` when two formulas are overlapping
"""
from typing import Any, Dict, List, Set

import pytest
from frequenz.api.microgrid.common_pb2 import AC, Metric

from frequenz.sdk._data_handling.time_series import (
    MeterField,
    SymbolComponentCategory,
    SymbolMapping,
    TimeSeriesFormula,
)
from frequenz.sdk._data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.microgrid.connection import Connection

from .base_microgrid_data_test import BaseMicrogridDataTest


# pylint:disable=too-many-locals
class TestMicrogridDataWithOverlappingFormulas(BaseMicrogridDataTest):
    """Test scenario of MicrogridData with overlapping formulas"""

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
            data=dict(ac=AC(power_active=Metric(value=load_meter_power)))
        )
        return meter_data_params

    # pylint: disable=too-many-locals,too-many-statements
    async def test_microgrid_data_with_overlapping_formulas(
        self,
    ) -> None:
        """Check if MicroGrid raises an exception when formulas overlap"""

        overlapping_metric_name = "grid_load"
        meters = [
            comp
            for comp in self.components
            if comp.category in {ComponentCategory.METER}
        ]

        symbols = ["meter_power" + str(comp.component_id) for comp in meters]
        formula: TimeSeriesFormula[Any] = TimeSeriesFormula(" + ".join(symbols))
        microgrid_formulas: Dict[str, TimeSeriesFormula[Any]] = {
            overlapping_metric_name: formula
        }

        symbol_mappings: List[SymbolMapping] = [
            SymbolMapping(
                SymbolComponentCategory.METER,
                meter.component_id,
                MeterField.ACTIVE_POWER,
            )
            for meter in meters
        ]

        timeout = 3.0
        interval = 0.2

        self.set_up_battery_data(self.microgrid_client, interval, timeout)
        self.set_up_meter_data(self.microgrid_client, interval, timeout)
        self.set_up_inverter_data(self.microgrid_client, interval, timeout)
        self.set_up_ev_charger_data(self.microgrid_client, interval, timeout)

        with pytest.raises(KeyError) as excinfo:
            _ = FormulaCalculator(
                component_graph=self.component_graph,
                additional_formulas=microgrid_formulas,
                symbol_mappings=symbol_mappings,
            )
        actual_err_str = excinfo.value.args[0]
        expected_error = f"Formula {overlapping_metric_name} is already defined!"
        assert expected_error == actual_err_str
