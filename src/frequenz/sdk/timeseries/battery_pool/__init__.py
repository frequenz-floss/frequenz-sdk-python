# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Manage a pool of batteries."""

from ._battery_pool import BatteryPool
from .messages import BatteryPoolReport

__all__ = [
    "BatteryPool",
    "BatteryPoolReport",
]
