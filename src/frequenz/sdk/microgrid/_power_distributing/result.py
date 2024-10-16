# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Results from PowerDistributingActor."""


import dataclasses
from collections import abc

from frequenz.quantities import Power

from .request import Request


@dataclasses.dataclass
class _BaseResultMixin:
    """Base mixin class for reporting power distribution results."""

    request: Request
    """The user's request to which this message responds."""


@dataclasses.dataclass
class _BaseSuccessMixin:
    """Result returned when setting the power succeed for all components."""

    succeeded_power: Power
    """The part of the requested power that was successfully set."""

    succeeded_components: abc.Set[int]
    """The subset of components for which power was set successfully."""

    excess_power: Power
    """The part of the requested power that could not be fulfilled.

    This happens when the requested power is outside the available power bounds.
    """


# We need to put the _BaseSuccessMixin before _BaseResultMixin in the
# inheritance list to make sure that the _BaseResultMixin attributes appear
# before the _BaseSuccessMixin, otherwise the request attribute will be last
# in the dataclass constructor because of how MRO works.


@dataclasses.dataclass
class Success(_BaseSuccessMixin, _BaseResultMixin):  # Order matters here. See above.
    """Result returned when setting the power was successful for all components."""


@dataclasses.dataclass
class PartialFailure(_BaseSuccessMixin, _BaseResultMixin):
    """Result returned when some of the components had an error setting the power."""

    failed_power: Power
    """The part of the requested power that failed to be set."""

    failed_components: abc.Set[int]
    """The subset of batteries for which the request failed."""


@dataclasses.dataclass
class Error(_BaseResultMixin):
    """Result returned when an error occurred and power was not set at all."""

    msg: str
    """The error message explaining why error happened."""


@dataclasses.dataclass
class PowerBounds:
    """Inclusion and exclusion power bounds for the requested components."""

    inclusion_lower: float
    """The lower value of the inclusion power bounds for the requested components."""

    exclusion_lower: float
    """The lower value of the exclusion power bounds for the requested components."""

    exclusion_upper: float
    """The upper value of the exclusion power bounds for the requested components."""

    inclusion_upper: float
    """The upper value of the inclusion power bounds for the requested components."""


@dataclasses.dataclass
class OutOfBounds(_BaseResultMixin):
    """Result returned when the power was not set because it was out of bounds.

    This result happens when the originating request was done with
    `adjust_power = False` and the requested power is not within the available bounds.
    """

    bounds: PowerBounds
    """The power bounds for the requested components.

    If the requested power negative, then this value is the lower bound.
    Otherwise it is upper bound.
    """


Result = Success | PartialFailure | Error | OutOfBounds
"""Power distribution result.

Example: Handling power distribution results

    ```python
    from typing import assert_never

    from frequenz.sdk.actor.power_distributing import (
        Error,
        OutOfBounds,
        PartialFailure,
        Result,
        Success,
    )
    from frequenz.sdk.actor.power_distributing.request import Request
    from frequenz.sdk.actor.power_distributing.result import PowerBounds
    from frequenz.quantities import Power

    def handle_power_request_result(result: Result) -> None:
        match result:
            case Success() as success:
                print(f"Power request was successful: {success}")
            case PartialFailure() as partial_failure:
                print(f"Power request was partially successful: {partial_failure}")
            case OutOfBounds() as out_of_bounds:
                print(f"Power request was out of bounds: {out_of_bounds}")
            case Error() as error:
                print(f"Power request failed: {error}")
            case _ as unreachable:
                assert_never(unreachable)

    request = Request(
        namespace="TestChannel",
        power=Power.from_watts(123.4),
        component_ids={8, 18},
    )

    results: list[Result] = [
        Success(
            request,
            succeeded_power=Power.from_watts(123.4),
            succeeded_components={8, 18},
            excess_power=Power.zero(),
        ),
        PartialFailure(
            request,
            succeeded_power=Power.from_watts(103.4),
            succeeded_components={8},
            excess_power=Power.zero(),
            failed_components={18},
            failed_power=Power.from_watts(20.0),
        ),
        OutOfBounds(request, bounds=PowerBounds(0, 0, 0, 800)),
        Error(request, msg="The components are not available"),
    ]

    for r in results:
        handle_power_request_result(r)
    ```
"""
