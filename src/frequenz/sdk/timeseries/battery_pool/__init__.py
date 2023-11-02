# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Manage a pool of batteries."""

from ...actor._power_managing import Report
from ._battery_pool import BatteryPool

__all__ = [
    "BatteryPool",
    "Report",
]
