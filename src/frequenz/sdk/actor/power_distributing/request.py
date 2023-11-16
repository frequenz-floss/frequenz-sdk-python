# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Definition of the user request."""


import dataclasses
from collections import abc
from datetime import timedelta

from ...timeseries._quantities import Power


@dataclasses.dataclass
class Request:
    """Request to set power to the `PowerDistributingActor`."""

    power: Power
    """The requested power."""

    component_ids: abc.Set[int]
    """The component ids of the components to be used for this request."""

    request_timeout: timedelta = timedelta(seconds=5.0)
    """The maximum amount of time to wait for the request to be fulfilled."""

    adjust_power: bool = True
    """Whether to adjust the power to match the bounds.

    If `True`, the power will be adjusted (lowered) to match the bounds, so
    only the reduced power will be set.

    If `False` and the power is outside the available bounds, the request will
    fail and be replied to with an `OutOfBound` result.
    """
