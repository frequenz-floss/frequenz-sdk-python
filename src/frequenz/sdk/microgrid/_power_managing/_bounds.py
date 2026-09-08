# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utilities for checking and clamping bounds and power values to exclusion bounds."""

import enum
from typing import assert_never

from frequenz.quantities import Power

from ...timeseries import Bounds


# This used to be a tuple[bool, bool], but mypy can't check that a match over a tuple
# covers every combination (see python/mypy#12364), so callers could leave a case out
# without anyone noticing. It also reads better, as (True, False) gives no hint about
# which of the two bounds it refers to.
@enum.unique
class ExclusionOverlap(enum.Enum):
    """Which bounds of a pair fall inside an exclusion zone."""

    NONE = enum.auto()
    """Neither bound is inside the exclusion zone."""

    LOWER = enum.auto()
    """Only the lower bound is inside the exclusion zone."""

    UPPER = enum.auto()
    """Only the upper bound is inside the exclusion zone."""

    BOTH = enum.auto()
    """Both bounds are inside the exclusion zone."""


def check_exclusion_bounds_overlap(
    lower_bound: Power,
    upper_bound: Power,
    exclusion_bounds: Bounds[Power] | None,
) -> ExclusionOverlap:
    """Check if the given bounds overlap with the given exclusion bounds.

    Example:

        ```
                       lower                        upper
                          .----- exclusion zone -----.
        -----|✓✓✓✓✓✓✓✓✓✓✓✓|xxxxxxxxxxxxxxx|----------|----
             `-- usable --'-- exclusion --´
             |                 overlap    |
             |                            |
           lower                        upper
           bound                        bound
                              (inside the exclusion zone)
        ```

        Resulting in `ExclusionOverlap.UPPER` because only the upper bound is inside
        the exclusion zone.

    Args:
        lower_bound: The lower bound to check.
        upper_bound: The upper bound to check.
        exclusion_bounds: The exclusion bounds to check against.

    Returns:
        Which of the given bounds are inside the exclusion bounds.
    """
    if exclusion_bounds is None:
        return ExclusionOverlap.NONE

    bounded_lower = exclusion_bounds.lower < lower_bound < exclusion_bounds.upper
    bounded_upper = exclusion_bounds.lower < upper_bound < exclusion_bounds.upper

    if bounded_lower and bounded_upper:
        return ExclusionOverlap.BOTH
    if bounded_lower:
        return ExclusionOverlap.LOWER
    if bounded_upper:
        return ExclusionOverlap.UPPER
    return ExclusionOverlap.NONE


def adjust_exclusion_bounds(
    lower_bound: Power,
    upper_bound: Power,
    exclusion_bounds: Bounds[Power] | None,
) -> tuple[Power, Power]:
    """Adjust the given bounds to exclude the given exclusion bounds.

    Args:
        lower_bound: The lower bound to adjust.
        upper_bound: The upper bound to adjust.
        exclusion_bounds: The exclusion bounds to adjust to.

    Returns:
        The adjusted lower and upper bounds.
    """
    if exclusion_bounds is None:
        return lower_bound, upper_bound

    # If the given bounds are within the exclusion bounds, there's no room to adjust,
    # so return zero.
    #
    # And if the given bounds overlap with the exclusion bounds on one side, then clamp
    # the given bounds on that side.
    match check_exclusion_bounds_overlap(lower_bound, upper_bound, exclusion_bounds):
        case ExclusionOverlap.BOTH:
            return Power.zero(), Power.zero()
        case ExclusionOverlap.UPPER:
            return lower_bound, exclusion_bounds.lower
        case ExclusionOverlap.LOWER:
            return exclusion_bounds.upper, upper_bound
        case ExclusionOverlap.NONE:
            return lower_bound, upper_bound
        case unexpected:
            assert_never(unexpected)


# Just 20 lines of code in this function, but unfortunately 8 of those are return
# statements, and that's too many for pylint.
def clamp_to_bounds(  # pylint: disable=too-many-return-statements
    value: Power,
    lower_bound: Power,
    upper_bound: Power,
    exclusion_bounds: Bounds[Power] | None,
) -> tuple[Power | None, Power | None]:
    """Clamp the given value to the given bounds.

    When the given value can falls within the exclusion zone, and can be clamped to
    both sides, both options will be returned.

    When the given value falls outside the usable bounds and can be clamped only to
    one side, only that option will be returned.

    Args:
        value: The value to clamp.
        lower_bound: The lower bound to clamp to.
        upper_bound: The upper bound to clamp to.
        exclusion_bounds: The exclusion bounds to clamp outside of.

    Returns:
        The clamped value.
    """
    # If the given bounds are within the exclusion bounds, return zero.
    #
    # And if the given bounds overlap with the exclusion bounds on one side, and the
    # given power is in that overlap region, clamp it to the exclusion bounds on that
    # side.
    if exclusion_bounds is not None:
        match check_exclusion_bounds_overlap(
            lower_bound, upper_bound, exclusion_bounds
        ):
            case ExclusionOverlap.BOTH:
                return None, None
            case ExclusionOverlap.LOWER:
                if value < exclusion_bounds.upper:
                    return None, exclusion_bounds.upper
            case ExclusionOverlap.UPPER:
                if value > exclusion_bounds.lower:
                    return exclusion_bounds.lower, None
            case ExclusionOverlap.NONE:
                # The bounds don't overlap the exclusion zone, so the value only needs
                # the generic clamping done below.
                pass
            case unexpected:
                assert_never(unexpected)

    # If the given value is outside the given bounds, clamp it to the closest bound.
    if value < lower_bound:
        return lower_bound, None
    if value > upper_bound:
        return None, upper_bound

    # If the given value is within the exclusion bounds and the exclusion bounds are
    # within the given bounds, clamp the given value to the closest exclusion bound.
    if exclusion_bounds is not None and not value.isclose(Power.zero()):
        if exclusion_bounds.lower < value < exclusion_bounds.upper:
            return exclusion_bounds.lower, exclusion_bounds.upper

    return value, value
