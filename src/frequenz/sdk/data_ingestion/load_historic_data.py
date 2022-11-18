# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
LoadHistoricData is a tool for loading historic parquet files.

This object can be called from within other `IOperation`s or outside of the
pipeline.  Please be aware that the loading of parquet files is not
asynchronous, running this will block other IOperation tasks unless done in a
different thread.
"""

import glob
import itertools
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, List, Optional

import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

logger = logging.Logger(__name__)

# directory path to all component data of a particular site
HISTDATA_DIR = "/data"
# format of timestamp in the parquet file name
FILE_TIMEFORMAT = "%Y-%m-%dT%H:%M:%S"
# prefix in filename before timestamp identifier
FILE_TIMEPREFIX = "/"
# suffix in filename after timestamp identifier
FILE_TIMESUFFIX = "-"


@dataclass
class ComponentInfo:
    """Object containing component information."""

    # component id
    component_id: int
    # category of component; "Meter"/"Inverter"/"Battery"
    category: str
    # type of meter; "pv"/"market"/"grid"/None (when component_category != "Meter")
    meter_type: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate fields once an object has been created.

        Raises:
            ValueError: on validation failure.
        """
        if (
            self.category
            in [
                "Meter",
                "Battery",
                "Inverter",
            ]
        ) is False:
            raise ValueError(
                "category must be one of following: `Meter`, `Battery` or `Inverter`"
            )
        if self.category == "Meter":
            if (
                self.meter_type
                in [
                    "pv",
                    "market",
                    "grid",
                ]
            ) is False:
                raise ValueError(
                    "meter type must be one of following: `pv`, `market`, `grid`"
                )


@dataclass
class FeatureGenerator:
    """Object containing the info for generating features from historic data."""

    # columns to read for generating features, can be single column or multiple
    # columns (e.g. if user wants to generate a feature by adding two columns
    # together) a key within a column could also be specified, e.g. soc.now
    read_cols: List[str]
    # user-defined functions to apply to columns (wrapped through pandas .apply()
    # function) for generating features
    # when set to None, there should be only one column specified in read_cols
    # it will be returned as it is
    apply_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None
    # column name for the generated feature in the return dataframe
    # when set to None, there should be only one column specified in read_cols
    # it will be set to be the same as the existing column / column.key name
    feature: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate fields once an object has been created.

        Raises:
            ValueError: on validation failure.
        """
        if self.apply_func is None:
            if len(self.read_cols) != 1:
                raise ValueError(
                    "length of read_cols must be 1 when apply_func is None"
                )
        if self.feature is None:
            if len(self.read_cols) != 1:
                raise ValueError("length of read_cols must be 1 when feature is None")
            self.feature = self.read_cols[0]


@dataclass
class LoadHistoricDataSettings:
    """Object containing instruction on which historic data to read."""

    # specify which component to read
    component_info: ComponentInfo
    # specify the list of features to generate
    feature_generators: List[FeatureGenerator]
    # rate at which data should be sampled in seconds
    data_sampling_rate: float


def gen_date_dirs(data_dir: str, dates: pd.DatetimeIndex) -> List[str]:
    """Generate the historical data directory paths for the given dates.

    Args:
        data_dir: directory of all the historic data of the particular component
        dates: the dates over which historic data should be read

    Returns:
        A list of directories of the specified dates.
    """
    date_dirs = [
        os.path.join(
            data_dir,
            "year=" + str(date.year),
            "month=" + ("0" + str(date.month))[-2:],
            "day=" + ("0" + str(date.day))[-2:],
        )
        for date in dates
    ]
    return date_dirs


def crop_df_list_by_time(
    df_list: List[pd.DataFrame],
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """Concat and crop read data by the specified start and end time.

    Cropping is implemented in case the specified start and end times lie within
    individual data files

    Args:
        df_list: list of read dataframes per file
        start_time: will read from this timestamp onwards
        end_time: will read up to this timestamp

    Returns:
        All dataframes in the list combined to one and cropped to fit within the
            specified start and end times.
    """
    df0 = pd.concat(df_list).reset_index(drop=True)
    df0["ts"] = pd.to_datetime(df0["ts"]).tz_localize(timezone.utc)
    df0 = df0.loc[((df0["ts"] >= start_time) & (df0["ts"] <= end_time))].reset_index(
        drop=True
    )
    return df0


def gen_features(
    df_read: pd.DataFrame, feature_generators: List[FeatureGenerator]
) -> pd.DataFrame:
    """Generate the specified features.

    This is done by applying the input feature_generator.apply_func on
    feature_generator.read_cols

    Args:
        df_read: read historical data containing all the read_cols specified in
            its original form
        feature_generators: list of FeatureGenerator objects specifying the
            features to generate

    Returns:
        A dataframe containing the generated features.
    """
    features: List[Any] = []
    for feature_generator in feature_generators:
        if feature_generator.apply_func is None:
            df_read[feature_generator.feature] = df_read[feature_generator.read_cols[0]]
        elif len(feature_generator.read_cols) > 1:
            df_read[feature_generator.feature] = df_read[
                feature_generator.read_cols
            ].apply(feature_generator.apply_func, axis=1)
        else:
            df_read[feature_generator.feature] = df_read[
                feature_generator.read_cols[0]
            ].apply(feature_generator.apply_func)
        features.append(feature_generator.feature)
    features = list(set(["ts"] + features))
    return df_read[features]


class LoadHistoricData:
    """Load historical data from parquet files.

    Can be used from within `IOperation`s or separately.  Not asynchronous, so
    will block other `IOperation`s and actors, unless run in a separate thread.
    """

    def __init__(
        self,
        microgrid_id: int,
        ignore_faulty_files: bool = True,
    ) -> None:
        """Create a `LoadHistoricalData` instance.

        Args:
            microgrid_id: mid of site
            ignore_faulty_files: mode for handling faulty files
                when True: individual faulty file will be ignored. The reader will
                print out an error message but continue to read other files within
                the specified time range.
                when False: once the reader encounters a faulty file, the whole
                reader will be stopped and an error will be raised.
        """
        self.histdata_dir = os.path.join(
            HISTDATA_DIR, "messstellen_id=" + str(microgrid_id)
        )
        self.file_time_format = FILE_TIMEFORMAT
        self.file_time_prefix = FILE_TIMEPREFIX
        self.file_time_suffix = FILE_TIMESUFFIX + str(microgrid_id) + ".parquet"
        self.ignore_faulty_files = ignore_faulty_files

    def get_file_timestamps(self, filenames: List[str]) -> pd.Series:
        """Get the timestamps for a list of historic parquet data files.

        Args:
            filenames: list of historic parquet data files

        Returns:
            Timestamps of each of the files.
        """
        timestamps = pd.to_datetime(
            [
                file.split(self.file_time_suffix)[0].split(self.file_time_prefix)[-1]
                for file in filenames
            ],
            format=self.file_time_format,
        ).tz_localize(timezone.utc)
        return timestamps

    def gen_datafile_list(
        self,
        data_dir: str,
        dates: pd.DatetimeIndex,
        start_time: datetime,
        end_time: datetime,
    ) -> List[str]:
        """Generate the list of historic parquet files to read.

        Args:
            data_dir: directory of all the historic data of the particular component
            dates: the dates over which histori data should be read
            start_time: will read from this timestamp onwards
            end_time: will read up to this timestamp

        Returns:
            The list of files that will be read within the specified timerange.
        """
        data: List[str] = []
        date_dirs = gen_date_dirs(data_dir, dates)
        for date, date_dir in zip(dates, date_dirs):
            date_files = sorted(
                glob.glob(
                    os.path.join(
                        date_dir,
                        "*.parquet",
                    )
                )
            )
            if (date == dates[0]) | (date == dates[-1]):
                timestamps = self.get_file_timestamps(date_files)
                date_files = [
                    date_files[i]
                    for i in range(len(date_files))
                    if ((timestamps[i] >= start_time) & (timestamps[i] <= end_time))
                ]
            data = data + date_files
        return data

    def load_parquet_file(self, file: str, read_cols: List[str]) -> pd.DataFrame:
        """Load one parquet file.

        Args:
            file: path of the file to read
            read_cols: list of columns to read

        Returns:
            Return dataframe of the read file with the specified columns

        Raises:
            RuntimeError: if file is not found or cannot be read
        """
        try:
            data = pq.ParquetDataset(file).read(columns=read_cols)
            df_read = data.to_pandas()
        except Exception as exception:  # pylint: disable=broad-except
            logger.exception(
                "failed loading data from file: %s, due to: %s", file, exception
            )
            df_read = pd.DataFrame()
            if self.ignore_faulty_files is False:
                raise RuntimeError from exception
        return df_read

    def load_parquet_files(
        self, files: List[str], read_cols: List[str], resampling_str: str = "1S"
    ) -> List[pd.DataFrame]:
        """Load multiple parquet files.

        Args:
            files: list of paths of the files to read
            read_cols: list of columns to read
            resampling_str: rule to use for resampling.

        Returns:
            List of return dataframes per file.
        """
        df_list: List[pd.DataFrame] = []
        for file in tqdm(files):
            df_read = self.load_parquet_file(file, read_cols)
            if len(df_read) > 0:
                df_list.append(
                    df_read.set_index("ts")
                    .resample(resampling_str)
                    .first()
                    .ffill()
                    .reset_index()
                )
        return df_list

    def read(
        self,
        load_hd_settings: LoadHistoricDataSettings,
        start_time: datetime,
        end_time: datetime,
    ) -> pd.DataFrame:
        """Read historical data.

        Args:
            load_hd_settings: instruction on which historic data to read.
            start_time: will read from this timestamp onwards.
            end_time: will read up to this timestamp.

        Returns:
            Dataframe containing historical data with column `timestamp` specifying
                the timestamp and the features generated with names
                `FeatureGenerator.feature` in feature_generators.
        """
        data_dir = os.path.join(
            self.histdata_dir,
            "category=" + load_hd_settings.component_info.category,
            "component_id=" + str(load_hd_settings.component_info.component_id),
        )
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        logger.info(
            "reading historic data from component id=%s within the time interval: %s to %s",
            load_hd_settings.component_info.component_id,
            start_time,
            end_time,
        )
        dates = pd.date_range(start_time.date(), end_time.date())
        data_files = self.gen_datafile_list(data_dir, dates, start_time, end_time)
        read_cols = list(
            set(
                itertools.chain(
                    *[
                        feature_generator.read_cols
                        for feature_generator in load_hd_settings.feature_generators
                    ]
                )
            )
        )
        read_cols = list(set(["ts"] + read_cols))
        df_list = self.load_parquet_files(
            data_files,
            read_cols,
            resampling_str=str(load_hd_settings.data_sampling_rate) + "S",
        )
        if len(df_list) == 0:
            return pd.DataFrame()
        df0 = crop_df_list_by_time(df_list, start_time, end_time)
        df_features = gen_features(df0, load_hd_settings.feature_generators).rename(
            columns={"ts": "timestamp"}
        )
        return df_features
