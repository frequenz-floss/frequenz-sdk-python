# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""This module provides feature to set active_power between many batteries.

Distributing active_power is very important to keep the microgrid ready
for the active_power requirements.
This module provides PowerDistributingActor that knows how to distribute active_power.
It also provides all the secondary features that should be used to communicate with
PowerDistributingActor and send requests for charging or discharging active_power.
"""

from ._battery_pool_status import BatteryStatus
from .power_distributing import PowerDistributingActor
from .request import Request
from .result import Error, OutOfBound, PartialFailure, Result, Success

__all__ = [
    "PowerDistributingActor",
    "Request",
    "Result",
    "Error",
    "Success",
    "OutOfBound",
    "PartialFailure",
    "BatteryStatus",
]
