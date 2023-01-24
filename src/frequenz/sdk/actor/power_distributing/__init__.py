# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""This module provides feature to set power between many batteries.

Distributing power is very important to keep the microgrid ready
for the power requirements.
This module provides PowerDistributingActor that knows how to distribute power.
It also provides all the secondary features that should be used to communicate with
PowerDistributingActor and send requests for charging or discharging power.
"""

from .power_distributing import PowerDistributingActor
from .request import Request
from .result import Error, Ignored, OutOfBound, PartialFailure, Result, Success

__all__ = [
    "PowerDistributingActor",
    "Request",
    "Result",
    "Error",
    "Success",
    "Ignored",
    "OutOfBound",
    "PartialFailure",
]
