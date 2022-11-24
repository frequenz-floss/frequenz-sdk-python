# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Helper class for creating custom algebraic formulas from strings or SymPy expressions."""

from typing import Any, Collection, Union

import sympy


class Formula:
    """Simple wrapper class for `sympy` algebraic expressions.

    This allows the user to define arbitrary algebraic formulas from string expressions
    such as "x + y" or "3 * z1 + 0.9 * z2 + 5.0 / z3".  For example, if an application
    needs a custom formula per deployment (such as how to calculate the grid power from
    several different meters), this can be used to create that formula from app config.
    """

    def __init__(self, formula: Union[str, sympy.Expr]) -> None:
        """Initialize the class.

        Args:
            formula: formula describing how to combine values from different
                time series (may be provided either as a string or as a
                ready-made SymPy expression)

        Raises:
            ValueError: if `formula` is not an instance of any of the following types
                `str`, `sympy.Expr`, `sympy.Tuple`

            TypeError: if formula symbols are not instances of `sympy.Symbol` or if
                symbol names are not of type `str`
        """
        if isinstance(formula, str):
            formula = sympy.parse_expr(formula)

        if not isinstance(formula, (sympy.Expr, sympy.Tuple)):
            raise ValueError(
                f"formula must be provided as str, sympy.Expr or sympy.Tuple, not"
                f" {type(formula)}"
            )

        def symbol_name(symbol: sympy.Basic) -> str:
            if not isinstance(symbol, sympy.Symbol):
                raise TypeError(
                    f"Symbol must be of type sympy.Symbol instead of {type(symbol)}"
                )
            if not isinstance(symbol.name, str):
                raise TypeError(
                    f"Symbol name must be a string instead of {type(symbol.name)}"
                )
            return symbol.name

        self._symbols = set(map(symbol_name, formula.free_symbols))
        self._evaluate_formula = sympy.lambdify(list(formula.free_symbols), formula)

    @property
    def symbols(self) -> Collection[str]:
        """Get the names of all the variables used in the formula.

        Returns:
            All the unique symbol names used in the formula.
        """
        return self._symbols

    def __call__(self, **kwargs: Any) -> Any:
        """Directly evaluate the formula with symbol values provided as `kwargs`.

        Args:
            **kwargs: key-value pairs corresponding to the names and values of
                symbols in the formula (e.g. if the formula is `x + y` then
                values for `x` and `y` must be provided)

        Returns:
            Result of the formula applied to the provided values.
        """
        return self._evaluate_formula(**kwargs)
