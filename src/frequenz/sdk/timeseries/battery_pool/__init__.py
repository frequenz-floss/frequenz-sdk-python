# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Manage a pool of batteries."""

from ...actor._power_managing import _Report
from ._battery_pool import BatteryPool
from ._result_types import BatteryPoolReport

__all__ = [
    "BatteryPool",
    "BatteryPoolReport",
    "_Report",
]
