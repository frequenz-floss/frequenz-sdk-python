# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the Formula implementation."""

from unittest.mock import Mock

import pytest
from frequenz.quantities import Quantity

from frequenz.sdk.timeseries.formulas._exceptions import FormulaSyntaxError
from frequenz.sdk.timeseries.formulas._parser import parse


@pytest.mark.parametrize(
    ("formula_str", "parsed_formula_str"),
    [
        ("#1", "[f](#1)"),
        ("-(1+#1)", "[f](0.0 - (1.0 + #1))"),
        ("1*(2+3)", "[f](1.0 * (2.0 + 3.0))"),
    ],
)
async def test_parser_validation(
    formula_str: str,
    parsed_formula_str: str,
) -> None:
    """Test formula parser validation."""
    try:
        formula = parse(
            name="f",
            formula=formula_str,
            create_method=Quantity,
            telemetry_fetcher=Mock(),
        )
        assert str(formula) == parsed_formula_str
    except FormulaSyntaxError:
        assert False, "Parser should not raise an error for this formula"


@pytest.mark.parametrize(
    ("formula_str", "expected_error_line"),
    [
        (
            "1++",
            "  ^ Expected expression",
        ),
        (
            "1**",
            "  ^ Expected expression",
        ),
        (
            "--1",
            " ^ Expected expression",
        ),
        (
            "(",
            " ^ Expected expression",
        ),
        (
            "(1",
            "^ Unmatched parenthesis",
        ),
        (
            "max",
            "   ^ Expected '(' after function name",
        ),
        (
            "max()",
            "    ^ Expected argument",
        ),
        (
            "max(1(",
            "     ^ Expected ',' or ')'",
        ),
        (
            "max(1",
            "   ^ Unmatched parenthesis",
        ),
        (
            "foo",
            "^^^ Unknown function name",
        ),
        (
            "foo(1)",
            "^^^ Unknown function name",
        ),
        (
            "max(1,,2)",
            "      ^ Expected argument",
        ),
        (
            "1 2",
            "  ^ Unexpected token",
        ),
        (
            "1, 2",
            " ^ Unexpected token",
        ),
        (
            "max(1, 2,)",
            "         ^ Expected argument",
        ),
        (
            "max(1, 2))",
            "         ^ Unexpected token",
        ),
        (
            "max(1, 2),",
            "         ^ Unexpected token",
        ),
    ],
)
async def test_parser_validation_errors(
    formula_str: str, expected_error_line: str
) -> None:
    """Test formula parser validation."""
    with pytest.raises(FormulaSyntaxError) as error:
        _ = parse(
            name="f",
            formula=formula_str,
            create_method=Quantity,
            telemetry_fetcher=Mock(),
        )

    assert str(error.value) == (
        "Formula syntax error:\n"
        f"  Formula: {formula_str}\n"
        f"           {expected_error_line}"
    )


@pytest.mark.parametrize(
    ("formula_str", "expected_error"),
    [
        # Long formula with error near start -> Ellipsize end
        (
            "max(coalesce(#1001, %1002, 0), coalesce(#1003, #1004, 0), coalesce(#1005, #1006, 0), coalesce(#1007, #1008, 0))",  # noqa: E501
            "Formula syntax error:\n"
            "  Formula: max(coalesce(#1001, %1002, 0), coalesce(#1003, #1004, 0), coalesc ...\n"
            "                               ^ Unexpected character",
        ),
        # Long formula with error near the end -> Ellipsize start
        (
            "max(coalesce(#1001, #1002, 0), coalesce(#1003, #1004, 0), coalesce(#1005, #1006, 0), coalesce(#10.07, #1008, 0))",  # noqa: E501
            "Formula syntax error:\n"
            "  Formula: ... 0), coalesce(#1005, #1006, 0), coalesce(#10.07, #1008, 0))\n"
            "                                                          ^ Unexpected character",
        ),
        # Very long formula with error in the middle -> Ellipsize both sides
        (
            "max(coalesce(#1001, #1002, 0), coalesce(#1003, #1004, 0), coalesce(#1005, #1006, 0), coalesce(#1007, #1008, 0)) :) "  # noqa: E501
            "min(coalesce(#2001, #2002, 0), coalesce(#2003, #2004, 0), coalesce(#2005, #2006, 0), coalesce(#2007, #2008, 0))",  # noqa: E501
            "Formula syntax error:\n"
            "  Formula: ... 005, #1006, 0), coalesce(#1007, #1008, 0)) :) min(coalesce(#2 ...\n"
            "                                                          ^ Unexpected character",
        ),
    ],
)
async def test_parser_validation_errors_in_long_formulas(
    formula_str: str, expected_error: str
) -> None:
    """Test formula parser validation for long formulas."""
    with pytest.raises(FormulaSyntaxError) as error:
        _ = parse(
            name="f",
            formula=formula_str,
            create_method=Quantity,
            telemetry_fetcher=Mock(),
        )

    assert str(error.value) == expected_error
    assert all(len(line) <= 80 for line in str(error.value).splitlines())


async def test_empty_formula() -> None:
    """Test formula parser validation."""
    with pytest.raises(FormulaSyntaxError) as error:
        _ = parse(
            name="f",
            formula="",
            create_method=Quantity,
            telemetry_fetcher=Mock(),
        )

    assert str(error.value) == "Empty formula"
