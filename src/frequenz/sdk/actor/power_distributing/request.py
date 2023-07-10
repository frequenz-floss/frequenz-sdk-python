# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Definition of the user request."""

from __future__ import annotations

import dataclasses
from collections import abc


@dataclasses.dataclass
class Request:
    """Request to set active_power to the `PowerDistributingActor`."""

    namespace: str
    """The namespace of the request.

    This will be used to identify the channel for sending the response into, in the
    channel registry.
    """

    active_power: float
    """The requested active_power in watts."""

    batteries: abc.Set[int]
    """The component ids of the batteries to be used for this request."""

    request_timeout_sec: float = 5.0
    """The maximum amount of time to wait for the request to be fulfilled."""

    adjust_active_power: bool = True
    """Whether to adjust the active_power to match the bounds.

    If `True`, the active_power will be adjusted (lowered) to match the bounds, so
    only the reduced active_power will be set.

    If `False` and the active_power is outside the batteries' bounds, the request will
    fail and be replied to with an `OutOfBound` result.
    """

    include_broken_batteries: bool = False
    """Whether to use all batteries included in the batteries set regardless the status.

    If set to `True`, the active_power distribution algorithm will consider all batteries,
    including the broken ones, when distributing active_power.  In such cases, any remaining
    active_power after distributing among the available batteries will be distributed equally
    among the unavailable (broken) batteries.  If all batteries in the set are
    unavailable, the active_power will be equally distributed among all the unavailable
    batteries in the request.

    If set to `False`, the active_power distribution will only take into account the available
    batteries, excluding any broken ones.
    """
