"""
Test for the `MicrogridData` when meter is on the grid's side

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import re
from typing import Any, Dict, List, Set

import pytest

from frequenz.sdk.data_handling.time_series import TimeSeriesFormula
from frequenz.sdk.data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.microgrid.connection import Connection

from .base_microgrid_data_test import BaseMicrogridDataTest


# pylint:disable=too-many-locals
class TestMicrogridDataWithMissingSymbols(BaseMicrogridDataTest):
    """Test scenario of MicrogridData when formulas are missing some symbols"""

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
        }

    # pylint: disable=too-many-locals,too-many-statements
    async def test_microgrid_data_with_missing_symbols(
        self,
    ) -> None:
        """Check if MicroGrid raises an exception when symbols are missing"""
        fake_metric_name = "fake_metric"
        symbols = [
            "invalid_symbol" + str(comp.component_id) for comp in self.components
        ]
        formula_with_missing_symbol: TimeSeriesFormula[Any] = TimeSeriesFormula(
            " + ".join(symbols)
        )
        microgrid_formulas: Dict[str, TimeSeriesFormula[Any]] = {
            fake_metric_name: formula_with_missing_symbol
        }

        with pytest.raises(KeyError) as excinfo:
            _ = FormulaCalculator(
                component_graph=self.component_graph,
                additional_formulas=microgrid_formulas,
            )
        actual_err_str = excinfo.value.args[0]
        start = actual_err_str.find("[") + 1
        end = actual_err_str.find("]")
        actual_missing_symbols: List[str] = actual_err_str[start:end].split(",")
        cleaned_actual_missing_symbols = [
            sym.strip().replace("'", "") for sym in actual_missing_symbols
        ]
        assert sorted(cleaned_actual_missing_symbols) == sorted(symbols)
        pattern = r"Symbol\(s\) \[.+\] used in \`microgrid_formulas\` not defined\."
        assert re.match(pattern, actual_err_str) is not None
