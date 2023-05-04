# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Internal math tools."""

import math


def is_close_to_zero(value: float, abs_tol: float = 1e-9) -> bool:
    """Check if a floating point value is close to zero.

    A value of 1e-9 is a commonly used absolute tolerance to balance precision
    and robustness for floating-point numbers comparisons close to zero. Note
    that this is also the default value for the relative tolerance.
    For more technical details, see https://peps.python.org/pep-0485/#behavior-near-zero

    Args:
        value: the floating point value to compare to.
        abs_tol: the minimum absolute tolerance. Defaults to 1e-9.

    Returns:
        whether the floating point value is close to zero.
    """
    zero: float = 0.0
    return math.isclose(a=value, b=zero, abs_tol=abs_tol)
