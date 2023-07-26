# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Methods for processing battery-inverter data."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Bound:
    """Lower and upper bound values."""

    lower: float
    """Lower bound."""

    upper: float
    """Upper bound."""


@dataclass
class PowerMetrics:
    """Power bounds metrics."""

    # compare = False tells the dataclass to not use name for comparison methods
    timestamp: datetime = field(compare=False)
    """Timestamp of the metrics."""

    supply_bound: Bound
    """Supply power bounds.

    Upper bound is always 0 and will be supported later.
    Lower bound is negative number calculated with with the formula:
    ```python
    working_pairs: Set[BatteryData, InverterData] # working batteries from the battery
        pool and adjacent inverters

    supply_bound.lower = sum(
        max(
            battery.power_lower_bound, inverter.active_power_lower_bound)
            for each working battery in battery pool
            )
        )
    ```
    """

    consume_bound: Bound
    """Consume power bounds.

    Lower bound is always 0 and will be supported later.
    Upper bound is positive number calculated with with the formula:
    ```python
    working_pairs: Set[BatteryData, InverterData] # working batteries from the battery
        pool and adjacent inverters

    consume_bound.upper = sum(
        min(
            battery.power_upper_bound, inverter.active_power_upper_bound)
            for each working battery in battery pool
            )
        )
    ```
    """
