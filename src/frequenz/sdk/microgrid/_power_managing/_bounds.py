# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utilities for checking and clamping bounds and power values to exclusion bounds."""

from frequenz.quantities import Power

from ...timeseries import Bounds


def check_exclusion_bounds_overlap(
    lower_bound: Power,
    upper_bound: Power,
    exclusion_bounds: Bounds[Power] | None,
) -> tuple[bool, bool]:
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

        Resulting in `(False, True)` because only the upper bound is inside the
        exclusion zone.

    Args:
        lower_bound: The lower bound to check.
        upper_bound: The upper bound to check.
        exclusion_bounds: The exclusion bounds to check against.

    Returns:
        A tuple containing a boolean indicating if the lower bound is bounded by the
            exclusion bounds, and a boolean indicating if the upper bound is bounded by
            the exclusion bounds.
    """
    if exclusion_bounds is None:
        return False, False

    bounded_lower = False
    bounded_upper = False

    if exclusion_bounds.lower < lower_bound < exclusion_bounds.upper:
        bounded_lower = True
    if exclusion_bounds.lower < upper_bound < exclusion_bounds.upper:
        bounded_upper = True

    return bounded_lower, bounded_upper


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
        case (True, True):
            return Power.zero(), Power.zero()
        case (False, True):
            return lower_bound, exclusion_bounds.lower
        case (True, False):
            return exclusion_bounds.upper, upper_bound
    return lower_bound, upper_bound


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
            case (True, True):
                return None, None
            case (True, False):
                if value < exclusion_bounds.upper:
                    return None, exclusion_bounds.upper
            case (False, True):
                if value > exclusion_bounds.lower:
                    return exclusion_bounds.lower, None

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
