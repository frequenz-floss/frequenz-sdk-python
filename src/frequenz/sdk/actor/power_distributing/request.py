# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Definition of the user request."""

from dataclasses import dataclass
from typing import Set


@dataclass
class Request:
    """Request from the user."""

    # How much power to set
    power: int
    # In which batteries the power should be set
    batteries: Set[int]
    # Timeout for the server to respond on the requests.
    request_timeout_sec: float = 5.0
    # If True and requested power value is above upper bound, then the power will be
    # decreased to match the bounds. Only the decreased power will be set.
    # If False and the requested power is above upper bound, then request won't
    # be processed. result.OutOfBound message will be send back to the user.
    adjust_power: bool = True
