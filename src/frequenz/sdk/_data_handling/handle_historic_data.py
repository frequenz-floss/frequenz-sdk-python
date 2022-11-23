# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Module for handling loaded historic data from different components.

Object for computing aggregate quantities from multiple components as data are loaded.
"""

import datetime as dt
from functools import reduce
from typing import Any, Dict, List, NamedTuple

import numpy as np
import pandas as pd

from .._data_ingestion.load_historic_data import (
    LoadHistoricData,
    LoadHistoricDataSettings,
)
from .formula import Formula


class SymbolMapping(NamedTuple):
    """Mappings between fomula symbols and loaded historic data."""

    # name of the symbol for which the mapping is defined for
    symbol: str
    # component id
    component_id: int
    # name of column of the loaded historic dataframe corresponding to the symbol
    field: str


class HandleHistDataSettings(NamedTuple):
    """Settings for handling multi-component historic data."""

    # loaders of historic data, specifying which component and which columns of
    # the parquet files to load
    hd_loaders: List[LoadHistoricDataSettings]
    # formulas with which to calculate the aggregated quantities with
    hd_formulas: Dict[str, Formula]
    # mappings between fomula symbols and loaded historic data
    symbol_mappings: List[SymbolMapping]


class HandleHistData:
    """Object for computing aggregate quantities from historic data of multiple components.

    Aggregation of multiple components are done through the Formula() implementation.
    """

    def __init__(
        self,
        messstellen_id: int,
        hd_handler_settings: HandleHistDataSettings,
        ignore_faulty_files: bool = True,
    ) -> None:
        """Initialise `HandleHistData` instance.

        Args:
            messstellen_id: id of site
            hd_handler_settings: settings for handling multi- component historic
                data.
            ignore_faulty_files: mode for handling faulty files.
                When True: individual faulty file will be ignored. The reader will print
                out an error message but continue to read other files within the
                specified time range.
                When False: once the reader encounters a faulty file, the whole
                reader will be stopped and an error will be raised.

        Raises:
            ValueError: when multiple loaders are specified for one component
            ValueError: when a symbol is defined multiple times in
                hd_handler_settings.symbol_mappings
            KeyError: when any symbol used in the formulas are not defined in
                hd_handler_settings.symbol_mappings
        """
        self.hd_loaders = {
            hdl.component_info.component_id: hdl
            for hdl in hd_handler_settings.hd_loaders
        }
        if len(self.hd_loaders.keys()) < len(hd_handler_settings.hd_loaders):
            raise ValueError(
                "Use only one LoadHistoricDataSettings object for each component."
            )
        self.hd_formulas = hd_handler_settings.hd_formulas
        symbols = [hdf.symbols for hdf in self.hd_formulas.values()]
        symbols = list({sym for syms in symbols for sym in syms})
        shdm_symbols = [shdm.symbol for shdm in hd_handler_settings.symbol_mappings]
        if len(set(shdm_symbols)) < len(shdm_symbols):
            raise ValueError("Each symbol should have an unique definition.")
        sym_undefined = [sym for sym in symbols if sym not in shdm_symbols]
        if len(sym_undefined) > 0:
            raise KeyError(f"Symbol(s) {sym_undefined} used in formula(s) not defined.")
        self.symbol_mappings = hd_handler_settings.symbol_mappings
        self.load_historic_data = LoadHistoricData(
            messstellen_id, ignore_faulty_files=ignore_faulty_files
        )

    def load_compute_formula(
        self,
        start_time: dt.datetime,
        end_time: dt.datetime,
        data_sampling_rate: str = "1S",
    ) -> pd.DataFrame:
        """Load necessary data to compute specified quantities in `hd_formulas`.

        Args:
            start_time: starting time of time period over which to load historic data.
            end_time: ending time of time period over which to load historic data.
            data_sampling_rate: the rate at which to resample the data; following pandas
                resampling convention.

        Returns:
            Resultant dataframe with columns timestamp and individual columns for
                each of the specified `Formula()`s between start_time and end_time.
        """
        dfs_hist0 = []
        for hdl in self.hd_loaders:
            df_hist = self.load_historic_data.read(
                self.hd_loaders[hdl], start_time, end_time
            )
            if len(df_hist) > 0:
                dfs_hist0.append(
                    df_hist.rename(
                        columns={
                            col: col + "_" + str(hdl)
                            for col in df_hist.columns
                            if col != "timestamp"
                        }
                    )
                )
        res = {}
        if len(dfs_hist0) > 0:
            df_hist0 = (
                reduce(
                    lambda df1, df2: pd.merge(
                        df1,
                        df2,
                        on="timestamp",
                        how="outer",
                    ),
                    dfs_hist0,
                )
                .drop_duplicates(subset=["timestamp"])
                .set_index("timestamp")
                .resample(data_sampling_rate)
                .mean()
                .reset_index()
            )
            df_hist0 = df_hist0.rename(
                columns={
                    shdm.field + "_" + str(shdm.component_id): shdm.symbol
                    for shdm in self.symbol_mappings
                }
            )
            res["timestamp"] = df_hist0.timestamp
            for hdf in self.hd_formulas:
                if all(s in df_hist0.columns for s in self.hd_formulas[hdf].symbols):
                    args = {
                        s: np.array(df_hist0[s]) for s in self.hd_formulas[hdf].symbols
                    }
                    res[hdf] = self.hd_formulas[hdf](**args)
        if len(res.keys()) > 1:
            df_res = pd.DataFrame(res)
        else:
            df_res = pd.DataFrame()
        return df_res

    def compute(
        self,
        start_time: dt.datetime,
        end_time: dt.datetime,
        data_sampling_rate: str = "1S",
        read_freq: dt.timedelta = dt.timedelta(days=1),
    ) -> pd.DataFrame:
        """Load and compute the specified quantities in the formulas.

        Args:
            start_time: starting time from which to load historic data.
            end_time: ending time up till which to load historic data.
            data_sampling_rate: the rate at which to resample the data; following pandas
                resampling convention.
            read_freq: load individual component data per this specified rate before
                computing the fomula.

        Returns:
            Result data frame with columns timestamp and individual columns for each
                of the specified quantities from the hd_formulas
        """
        res: Dict[str, Any] = {"timestamp": []}
        for hdf in self.hd_formulas:
            res[hdf] = []
        df_res = pd.DataFrame(res)
        start_time0 = start_time
        end_time0 = min(start_time0 + read_freq, end_time)

        while start_time0 < end_time:
            df_res0 = self.load_compute_formula(
                start_time0, end_time0, data_sampling_rate=data_sampling_rate
            )
            df_res = df_res.append(df_res0)
            start_time0 = end_time0
            end_time0 = min(start_time0 + read_freq, end_time)
        df_res["timestamp"] = pd.to_datetime(df_res.timestamp, utc=True)
        return df_res
