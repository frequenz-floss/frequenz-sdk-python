# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Results from PowerDistributingActor."""

from __future__ import annotations

import dataclasses

from .request import Request


@dataclasses.dataclass
class _BaseResultMixin:
    """Base mixin class for reporting active_power distribution results."""

    request: Request
    """The user's request to which this message responds."""


# When moving to Python 3.10+ we should replace this with an union type:
# Result = Success | PartialFailure | Error | OutOfBound | Ignored
# For now it can't be done because before 3.10 isinstance(result, Success)
# doesn't work, so it is hard to figure out what type of result you got in
# a forward compatible way.
# When moving we should use the _BaseResultMixin as a base class for all
# results.
@dataclasses.dataclass
class Result(_BaseResultMixin):
    """Power distribution result."""


@dataclasses.dataclass
class _BaseSuccessMixin:
    """Result returned when setting the active_power succeed for all batteries."""

    succeeded_active_power: float
    """The part of the requested active_power that was successfully set."""

    succeeded_batteries: set[int]
    """The subset of batteries for which active_power was set successfully."""

    excess_active_power: float
    """The part of the requested active_power that could not be fulfilled.

    This happens when the requested active_power is outside the available active_power bounds.
    """


# We need to put the _BaseSuccessMixin before Result in the inheritance list to
# make sure that the Result attributes appear before the _BaseSuccessMixin,
# otherwise the request attribute will be last in the dataclass constructor
# because of how MRO works.


@dataclasses.dataclass
class Success(_BaseSuccessMixin, Result):  # Order matters here. See above.
    """Result returned when setting the active_power succeeded for all batteries."""


@dataclasses.dataclass
class PartialFailure(_BaseSuccessMixin, Result):
    """Result returned when any battery failed to perform the request."""

    failed_active_power: float
    """The part of the requested active_power that failed to be set."""

    failed_batteries: set[int]
    """The subset of batteries for which the request failed."""


@dataclasses.dataclass
class Error(Result):
    """Result returned when an error occurred and active_power was not set at all."""

    msg: str
    """The error message explaining why error happened."""


@dataclasses.dataclass
class OutOfBound(Result):
    """Result returned when the active_power was not set because it was out of bounds.

    This result happens when the originating request was done with
    `adjust_active_power = False` and the requested active_power is not within the batteries bounds.
    """

    bound: float
    """The total active_power bound for the requested batteries.

    If the requested active_power negative, then this value is the lower bound.
    Otherwise it is upper bound.
    """
