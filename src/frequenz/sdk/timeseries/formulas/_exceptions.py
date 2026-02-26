# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Exception classes for formulas parser and evaluator."""


class FormulaError(Exception):
    """Base class for exceptions in this module."""


class FormulaSyntaxError(FormulaError):
    """Exception raised for syntax errors in the formula."""

    def __init__(
        self, *, formula: str, span: tuple[int, int] | None, message: str
    ) -> None:
        """Initialize this instance."""
        self._formula: str = formula
        self._span: tuple[int, int] | None = span
        self._message: str = message

    def __str__(self) -> str:
        """Return a string representation of the error.

        The span (if present) will be used to highlight the error location.
        For long formulas, the beginning and/or end will be truncated as needed.
        """
        if self._span is None:
            return self._message

        formula_line = self._formula
        span_padding = " " * self._span[0]
        span_highlight = "^" * (self._span[1] - self._span[0])
        error_line = f"{span_padding}{span_highlight} {self._message}"

        char_limit: int = 80 - 11  # Account for "  Formula: " prefix in final output

        if len(error_line) > char_limit:
            # Remove characters from the left to fit error line within char limit
            num_chars_to_truncate = len(error_line) - char_limit
            error_line = error_line[num_chars_to_truncate:]
            # Shift formula line by the same amount and insert leading ellipsis
            formula_line = f"... {formula_line[num_chars_to_truncate+4:]}"

        if len(formula_line) > char_limit:
            # Truncate the formula line, insert ellipsis
            formula_line = f"{formula_line[:char_limit-4]} ..."

        return (
            "Formula syntax error:\n"
            f"  Formula: {formula_line}\n"
            f"           {error_line}"
        )
