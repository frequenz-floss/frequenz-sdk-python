# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Auxiliary functions for generating features when loading historic data."""

import datetime as dt

import pandas as pd


def get_day_sec(timestamps: dt.datetime) -> float:
    """Extract the second within the day from timestamps.

    This is an example user-defined feature generating function.

    Args:
        timestamps: the timestamp to extract the current day's seconds from.

    Returns:
        Number of seconds since midnight in the given timestamp.
    """
    pd_ts: pd.Timestamp = pd.to_datetime(timestamps)
    day_sec: float = (
        pd_ts.hour * 3600 + pd_ts.minute * 60 + pd_ts.second + pd_ts.microsecond / 1e6
    )
    return day_sec


def get_active_power(
    ac_connection: pd.DataFrame, passive_sign_convention: bool = True
) -> pd.Series:
    """Extract active power from ac_connection column dictionary.

    This is an example user-defined feature generating function.

    Args:
        ac_connection: the ac_connection column data read from the parquet
            files.
        passive_sign_convention: if `True` (the default), the active power value
            will follow the passive sign convention, where a positive value
            means net consumption, and a negative value means net supply of
            power; If `False`, the power value will follow the active sign
            convention, where a positive value corresponds to net supply, and
            negative to net consumption.

    Returns:
        The calculated active power.
    """
    power_sign = 2 * int(passive_sign_convention) - 1
    active_power = power_sign * (
        ac_connection["ac_connection.total_power_active.power_consumption.now"]
        - ac_connection["ac_connection.total_power_active.power_supply.now"]
    )
    return active_power
