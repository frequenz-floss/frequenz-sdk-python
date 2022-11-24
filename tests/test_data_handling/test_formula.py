# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.sdk._data_handling.formula module."""
from datetime import datetime, timedelta, timezone
from typing import Dict

import pandas as pd
import pytest
import sympy

from frequenz.sdk._data_handling import TimeSeriesEntry, formula
from frequenz.sdk._data_handling import time_series as ts

# pylint: disable=invalid-name


def test_Formula() -> None:
    """Test the formula evaluator."""
    # formulas are simple enough to initialize
    # from a string representation

    f1 = formula.Formula("x + y")
    assert f1.symbols == set(["x", "y"])
    assert f1(x=1, y=2) == 3
    assert f1(x=49.5, y=-7.5) == 42.0

    f2 = formula.Formula("3 * x * y + z / (1j)")
    assert f2.symbols == set(["x", "y", "z"])
    assert f2(x=4, y=5, z=6j) == 66
    assert f2(x=0.5, y=0.25, z=2.0) == 0.375 - 2.0j

    f3 = formula.Formula("(a - b) / c")
    assert f3.symbols == set(["a", "b", "c"])
    assert f3(a=2, b=2, c=3) == 0
    assert f3(a=1, b=2, c=4) == -0.25
    assert f3(a=1, b=0, c=float("inf")) == 0
    with pytest.raises(ZeroDivisionError):
        f3(a=2, b=1, c=0)

    f4 = formula.Formula("z ** 2")
    assert f4.symbols == set(["z"])
    assert f4(z=1) == 1
    assert f4(z=2) == 4
    assert f4(z=(3 + 3j)) == 18j
    assert f4(z=(4 - 4j)) == -32j

    f5 = formula.Formula("onsite_power + pv_power + battery_power")
    assert f5.symbols == set(["onsite_power", "pv_power", "battery_power"])
    assert f5(onsite_power=3 + 1j, pv_power=-2, battery_power=-1 - 1j) == 0

    # we can also initialize a formula directly
    # with a `sympy.Expr`

    alpha, beta = sympy.symbols("alpha beta")
    expr = alpha**beta
    f6 = formula.Formula(expr)
    assert f6.symbols == set(["alpha", "beta"])
    assert f6(alpha=2.0, beta=3.0) == 8.0
    assert f6(alpha=-1.5, beta=5.0) == -7.59375

    # but attempting to initialize it with anything other
    # than a string or a `sympy.Expr` means a `ValueError`
    # gets raised

    with pytest.raises(ValueError):
        _ferr = formula.Formula(lambda x: x**2 + x + 1)  # type: ignore

    # formulas don't just work with numerical values, we can
    # also combine e.g. columns of a dataframe ...
    df = pd.DataFrame(
        index=pd.DatetimeIndex(
            [
                pd.Timestamp("2022-04-12T20:34:00Z"),
                pd.Timestamp("2022-04-12T20:34:01Z"),
                pd.Timestamp("2022-04-12T20:34:02Z"),
            ]
        ),
        data={
            "s1": [1.0, 2.0, 3.0],
            "s2": [4.0, 5.0, 6.0],
            "s3": [7.0, 8.0, 9.0],
        },
    )

    f7 = formula.Formula("s1 + s2 - s3")
    assert f7.symbols == set(["s1", "s2", "s3"])

    expected = pd.Series(index=df.index, data=[-2.0, -1.0, 0.0])
    # We need to convert the keys as strings because they are some weird pandas
    # type
    symbols = {str(k): v for k, v in df.to_dict(orient="series").items()}
    assert (f7(**symbols) == expected).all()


def test_formula_with_broken_meter() -> None:
    """Test a microgrid formula with a broken meter."""
    cache = ts.LatestEntryCache[str, float]()

    ts1 = datetime.fromisoformat("2021-03-11T10:49:00+00:00")
    outdated_ts = ts1 - timedelta(minutes=5)
    timedelta_tolerance = timedelta(minutes=3)

    symbols = [
        "meter_1_active_power",
        "meter_2_active_power",
        "meter_3_active_power",
        "ev_charger_4_active_power",
        "inverter_5_active_power",
    ]

    entries = [
        ts.TimeSeriesEntry(ts1, 100.0),
        ts.TimeSeriesEntry(ts1, 100.0),
        ts.TimeSeriesEntry(outdated_ts, 100.0),
        ts.TimeSeriesEntry(ts1, 100.0),
        ts.TimeSeriesEntry(ts1, 100.0),
    ]

    symbol_mappings = [
        ts.SymbolMapping(
            ts.SymbolComponentCategory.METER, 1, ts.MeterField.ACTIVE_POWER
        ),
        ts.SymbolMapping(
            ts.SymbolComponentCategory.METER, 2, ts.MeterField.ACTIVE_POWER
        ),
        ts.SymbolMapping(
            ts.SymbolComponentCategory.METER, 3, ts.MeterField.ACTIVE_POWER
        ),
        ts.SymbolMapping(
            ts.SymbolComponentCategory.EV_CHARGER,
            4,
            ts.EVChargerField.ACTIVE_POWER,
        ),
        ts.SymbolMapping(
            ts.SymbolComponentCategory.INVERTER, 5, ts.InverterField.ACTIVE_POWER
        ),
    ]

    symbol_to_symbol_mapping: Dict[str, ts.SymbolMapping] = dict(
        zip(symbols, symbol_mappings)
    )

    for symbol, entry in zip(symbols, entries):
        cache.update(key=symbol, entry=entry)

    formula_name = "client_load"

    client_load_formula = ts.TimeSeriesFormula[float](" + ".join(symbols))

    result = client_load_formula.evaluate(
        cache=cache,
        formula_name=formula_name,
        symbol_to_symbol_mapping=symbol_to_symbol_mapping,
        timedelta_tolerance=timedelta_tolerance,
    )
    assert result is not None
    assert result.status == ts.TimeSeriesEntry.Status.UNKNOWN


def test_formula_with_broken_battery() -> None:
    """Test a microgrid formula with a broken battery."""
    cache = ts.LatestEntryCache[str, float]()

    ts1 = datetime.fromisoformat("2021-03-11T10:49:00+00:00")
    outdated_ts = ts1 - timedelta(minutes=5)
    timedelta_tolerance = timedelta(minutes=3)

    symbols = [
        "battery_1_soc",
        "battery_2_soc",
        "battery_3_soc",
        "battery_1_capacity",
        "battery_2_capacity",
        "battery_3_capacity",
    ]

    entries = [
        ts.TimeSeriesEntry(ts1, 1.0),
        ts.TimeSeriesEntry(ts1, 1.0),
        ts.TimeSeriesEntry(outdated_ts, 1.0),
        ts.TimeSeriesEntry(ts1, 5.0),
        ts.TimeSeriesEntry(ts1, 5.0),
        ts.TimeSeriesEntry(outdated_ts, 5.0),
    ]

    symbol_mappings = [
        ts.SymbolMapping(ts.SymbolComponentCategory.BATTERY, 1, ts.BatteryField.SOC),
        ts.SymbolMapping(ts.SymbolComponentCategory.BATTERY, 2, ts.BatteryField.SOC),
        ts.SymbolMapping(ts.SymbolComponentCategory.BATTERY, 3, ts.BatteryField.SOC),
        ts.SymbolMapping(
            ts.SymbolComponentCategory.BATTERY, 1, ts.BatteryField.CAPACITY
        ),
        ts.SymbolMapping(
            ts.SymbolComponentCategory.BATTERY, 2, ts.BatteryField.CAPACITY
        ),
        ts.SymbolMapping(
            ts.SymbolComponentCategory.BATTERY, 3, ts.BatteryField.CAPACITY
        ),
    ]

    symbol_to_symbol_mapping: Dict[str, ts.SymbolMapping] = dict(
        zip(symbols, symbol_mappings)
    )

    for symbol, entry in zip(symbols, entries):
        cache.update(key=symbol, entry=entry)

    formula_name = "batteries_remaining_energy"

    batteries_remaining_energy_formula = ts.TimeSeriesFormula[float](
        " + ".join(
            [
                " * ".join([symbols[i], symbols[i + len(symbols) // 2]])
                for i in range(len(symbols) // 2)
            ]
        )
    )

    result = batteries_remaining_energy_formula.evaluate(
        cache=cache,
        formula_name=formula_name,
        symbol_to_symbol_mapping=symbol_to_symbol_mapping,
        timedelta_tolerance=timedelta_tolerance,
        default_entry=TimeSeriesEntry[float](
            timestamp=datetime.now(timezone.utc), value=0.0
        ),
    )

    assert result is not None
    assert result.status == ts.TimeSeriesEntry.Status.VALID
    assert result.value == 1.0 * 5.0 + 1.0 * 5.0 + 0.0 * 0.0
