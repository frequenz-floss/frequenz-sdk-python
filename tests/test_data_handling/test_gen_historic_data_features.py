# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.sdk._data_handling.gen_historic_data_features module."""

import pandas as pd

import frequenz.sdk._data_handling.gen_historic_data_features as hd


def test_get_active_power() -> None:
    """Test get active power."""
    input1 = pd.DataFrame(
        index=pd.DatetimeIndex(
            [
                pd.Timestamp("2022-04-29T11:11:11Z"),
                pd.Timestamp("2022-04-29T11:11:12Z"),
                pd.Timestamp("2022-04-29T11:11:13Z"),
            ]
        ),
        data={
            "ac_connection.total_power_active.power_consumption.now": [
                0.0,
                1.0,
                2.0,
            ],
            "ac_connection.total_power_active.power_supply.now": [
                0.5,
                0.25,
                0.0,
            ],
        },
    )

    passive1 = hd.get_active_power(input1)
    expected_passive1 = pd.Series(
        index=input1.index,
        data=[-0.5, 0.75, 2.0],
    )
    assert (passive1 == expected_passive1).all()

    passive1a = hd.get_active_power(input1, passive_sign_convention=True)
    assert (passive1a == expected_passive1).all()

    active1 = hd.get_active_power(input1, passive_sign_convention=False)
    expected_active1 = pd.Series(
        index=input1.index,
        data=[0.5, -0.75, -2.0],
    )
    assert (active1 == expected_active1).all()
