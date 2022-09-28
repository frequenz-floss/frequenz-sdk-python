"""All helpers used to distribute power.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, NamedTuple, Optional, Set

from frequenz.channels import BidirectionalHandle

from ..microgrid.component_data import BatteryData, InverterData


class InvBatPair(NamedTuple):
    """InvBatPair with inverter and adjacent battery data."""

    battery: BatteryData
    inverter: InverterData


@dataclass
class Request:
    """Request from the user."""

    # How much power to set
    power: int
    # In which batteries the power should be set
    batteries: Set[int]
    # Timeout for the server to respond on the request.
    request_timeout_sec: int = 5
    # If True and requested power value is out of bound, then
    # PowerDistributor will decrease the power to match the bounds and
    # distribute only decreased power.
    # If False and the requested power is out of bound, then
    # PowerDistributor will not process this request and send result with status
    # Result.Status.OUT_OF_BOUND.
    adjust_power: bool = True


@dataclass
class Result:
    """Result on distribution request."""

    class Status(Enum):
        """Status of the result."""

        FAILED = 0  # If any request for any battery didn't succeed for any reason.
        SUCCESS = 1  # If all requests for all batteries succeed.
        IGNORED = 2  # If request was dispossessed by newer request with the same set
        # of batteries.
        ERROR = 3  # If any error happened. In this case error_message describes error.
        OUT_OF_BOUND = 4  # When Request.adjust_power=False and the requested power was
        # out of the bounds for specified batteries.

    status: Status  # Status of the request.

    failed_power: float  # How much power failed.

    above_upper_bound: float  # How much power was not used because it was beyond the
    # limits.

    error_message: Optional[
        str
    ] = None  # error_message filled only when status is ERROR


@dataclass
class User:
    """User definitions. Only for internal use."""

    # User id
    user_id: str
    # Channel for the communication
    channel: BidirectionalHandle[Result, Request]


class BrokenComponents:
    """Store components marked as broken."""

    def __init__(self, timeout_sec: float) -> None:
        """Create object instance.

        Args:
            timeout_sec: How long the component should be marked as broken.
        """
        self._broken: Dict[int, datetime] = {}
        self._timeout_sec = timeout_sec

    def mark_as_broken(self, component_id: int) -> None:
        """Mark component as broken.

        After marking component as broken it would be considered as broken for
        self._timeout_sec.

        Args:
            component_id: component id
        """
        self._broken[component_id] = datetime.now()

    def update_retry(self, timeout_sec: float) -> None:
        """Change how long the component should be marked as broken.

        Args:
            timeout_sec: New retry time after sec.
        """
        self._timeout_sec = timeout_sec

    def is_broken(self, component_id: int) -> bool:
        """Check if component is marked as broken.

        Args:
            component_id: component id

        Returns:
            True if component is broken, False otherwise.
        """
        if component_id in self._broken:
            last_broken = self._broken[component_id]
            if (datetime.now() - last_broken).total_seconds() < self._timeout_sec:
                return True

            del self._broken[component_id]
        return False
