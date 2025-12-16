# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Deprecated Formula Engine."""


from typing_extensions import deprecated

from .._base_types import QuantityT
from ..formulas._formula import Formula
from ..formulas._formula_3_phase import Formula3Phase


@deprecated(
    "The FormulaEngine class is deprecated and will be removed in a future release. "
    + "Please use the Formula class instead."
)
class FormulaEngine(Formula[QuantityT]):
    """Deprecated Formula Engine class.

    This class is deprecated and will be removed in a future release.
    Please use the `Formula` and `Formula3Phase` classes directly.
    """


@deprecated(
    "The FormulaEngine3Phase class is deprecated and will be removed in a future release. "
    + "Please use the Formula3Phase class instead."
)
class FormulaEngine3Phase(Formula3Phase[QuantityT]):
    """Deprecated FormulaEngine3Phase class.

    This class is deprecated and will be removed in a future release.
    Please use the `Formula3Phase` class directly.
    """


__all__ = ["FormulaEngine", "FormulaEngine3Phase"]
