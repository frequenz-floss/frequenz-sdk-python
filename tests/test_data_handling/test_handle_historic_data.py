"""Tests for the handle_historic_data package.

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from pytest_mock import MockerFixture  # pylint: disable=import-error

from frequenz.sdk.data_handling.formula import Formula
from frequenz.sdk.data_handling.gen_historic_data_features import get_active_power
from frequenz.sdk.data_ingestion.load_historic_data import (
    ComponentInfo,
    FeatureGenerator,
    LoadHistoricData,
    LoadHistoricDataSettings,
)


def test_handle_historic_data_import() -> None:
    """Check that HandleHistData can be imported successfully."""

    from frequenz.sdk.data_handling.handle_historic_data import (  # pylint: disable=import-outside-toplevel
        HandleHistData,
    )

    assert HandleHistData is not None


def mock_load_hd_read(
    self: LoadHistoricData,  # pylint: disable=unused-argument
    load_hd_settings: LoadHistoricDataSettings,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """Mock historic data loading function."""
    timestamps = pd.date_range(
        start_time,
        end_time,
        freq=str(int(load_hd_settings.data_sampling_rate)) + "S",
    )
    data = {"timestamp": timestamps}
    for feature_generator in load_hd_settings.feature_generators:
        feature = feature_generator.feature
        assert isinstance(feature, str)
        data.update({feature: np.full(len(timestamps), len(feature))})

    df_hist = pd.DataFrame(data)
    df_hist["timestamp"] = pd.to_datetime(df_hist.timestamp)
    return df_hist


def test_load_compute_formula(mocker: MockerFixture) -> None:
    """Test load_compute_formula() function in HandleHistData() class."""

    from frequenz.sdk.data_handling.handle_historic_data import (  # pylint: disable=import-outside-toplevel
        HandleHistData,
        HandleHistDataSettings,
        SymbolMapping,
    )

    messstellen_id = 1
    feature_generator = FeatureGenerator(
        [
            "ac_connection.total_power_active.power_supply.now",
            "ac_connection.total_power_active.power_consumption.now",
        ],
        get_active_power,
        "power",
    )
    data_sampling_rate = 1.0
    hd_loaders = [
        LoadHistoricDataSettings(
            ComponentInfo(1, "Meter", "grid"), [feature_generator], data_sampling_rate
        ),
        LoadHistoricDataSettings(
            ComponentInfo(3, "Meter", "pv"), [feature_generator], data_sampling_rate
        ),
        LoadHistoricDataSettings(
            ComponentInfo(4, "Inverter", None), [feature_generator], data_sampling_rate
        ),
    ]
    hd_formulas = {"client_load": Formula("-grid + inv + pv")}
    symbol_mappings = [
        SymbolMapping("grid", 1, "power"),
        SymbolMapping("pv", 3, "power"),
        SymbolMapping("inv", 4, "power"),
    ]
    hd_handler_settings = HandleHistDataSettings(
        hd_loaders, hd_formulas, symbol_mappings
    )
    end_time = datetime.now(timezone.utc)
    mocker.patch.object(LoadHistoricData, "read", mock_load_hd_read)
    hd_handler = HandleHistData(messstellen_id, hd_handler_settings)
    df_hdh = hd_handler.compute(end_time - timedelta(days=2), end_time)
    assert "client_load" in df_hdh.columns
    assert (df_hdh["client_load"] == 5).all()
