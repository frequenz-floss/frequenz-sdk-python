# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Definition of the user request."""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass
class Request:
    """Request to set power to the `PowerDistributingActor`."""

    power: float
    """The requested power in watts."""

    batteries: set[int]
    """The component ids of the batteries to be used for this request."""

    request_timeout_sec: float = 5.0
    """The maximum amount of time to wait for the request to be fulfilled."""

    adjust_power: bool = True
    """Whether to adjust the power to match the bounds.

    If `True`, the power will be adjusted (lowered) to match the bounds, so
    only the reduced power will be set.

    If `False` and the power is outside the batteries' bounds, the request will
    fail and be replied to with an `OutOfBound` result.
    """

    include_broken: bool = False
    """Whether to use all batteries included in the batteries set regardless the status.

    if `True`, the remaining power after distributing between working batteries
    will be distributed equally between broken batteries. Also if all batteries
    in the batteries set are broken then the power is distributed equally between
    broken batteries.

    if `False`, the power will be only distributed between the working batteries.
    """
