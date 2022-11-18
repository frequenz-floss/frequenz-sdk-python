# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Test for the `TimeSeriesFormula`
"""
from typing import Any

import pytest

from frequenz.sdk.data_handling.time_series import TimeSeriesFormula


@pytest.mark.parametrize(
    "formula",
    [
        "",
        "meter_power1 + inverter_power1 + ",
    ],
)
async def test_microgrid_data_formula(formula: str) -> None:
    """Check if TimeSeriesFormula raises an error when formula syntax is invalid"""
    with pytest.raises(SyntaxError):
        _: TimeSeriesFormula[Any] = TimeSeriesFormula(formula)
