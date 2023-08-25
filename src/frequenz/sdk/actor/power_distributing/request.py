# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Definition of the user request."""

from __future__ import annotations

import dataclasses
from collections import abc
from datetime import timedelta

from ...timeseries._quantities import Power


@dataclasses.dataclass
class Request:
    """Request to set power to the `PowerDistributingActor`."""

    namespace: str
    """The namespace of the request.

    This will be used to identify the channel for sending the response into, in the
    channel registry.
    """

    power: Power
    """The requested power."""

    batteries: abc.Set[int]
    """The component ids of the batteries to be used for this request."""

    request_timeout: timedelta = timedelta(seconds=5.0)
    """The maximum amount of time to wait for the request to be fulfilled."""

    adjust_power: bool = True
    """Whether to adjust the power to match the bounds.

    If `True`, the power will be adjusted (lowered) to match the bounds, so
    only the reduced power will be set.

    If `False` and the power is outside the batteries' bounds, the request will
    fail and be replied to with an `OutOfBound` result.
    """

    include_broken_batteries: bool = False
    """Whether to use all batteries included in the batteries set regardless the status.

    If set to `True`, the power distribution algorithm will consider all batteries,
    including the broken ones, when distributing power.  In such cases, any remaining
    power after distributing among the available batteries will be distributed equally
    among the unavailable (broken) batteries.  If all batteries in the set are
    unavailable, the power will be equally distributed among all the unavailable
    batteries in the request.

    If set to `False`, the power distribution will only take into account the available
    batteries, excluding any broken ones.
    """
