# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the formula lexer."""
import pytest

from frequenz.sdk.timeseries.formulas import _token
from frequenz.sdk.timeseries.formulas._exceptions import FormulaSyntaxError
from frequenz.sdk.timeseries.formulas._lexer import Lexer


def test_lexer() -> None:
    """Test the Lexer reading different kinds of tokens."""
    formula = "#123 + coalesce(#1 / 10.0, 0.0)"
    lexer = Lexer(formula)
    component = next(lexer)
    assert isinstance(component, _token.Component)
    assert component.id == "123"
    assert component.value == "#123"
    assert component.span == (0, 4)

    plus_op = next(lexer)
    assert isinstance(plus_op, _token.Plus)
    assert plus_op.value == "+"
    assert plus_op.span == (5, 6)

    symbol = next(lexer)
    assert isinstance(symbol, _token.Symbol)
    assert symbol.value == "coalesce"
    assert symbol.span == (7, 15)

    open_paren = next(lexer)
    assert isinstance(open_paren, _token.OpenParen)
    assert open_paren.value == "("
    assert open_paren.span == (15, 16)

    component = next(lexer)
    assert isinstance(component, _token.Component)
    assert component.id == "1"
    assert component.value == "#1"
    assert component.span == (16, 18)

    div_op = next(lexer)
    assert isinstance(div_op, _token.Div)
    assert div_op.value == "/"
    assert div_op.span == (19, 20)

    number = next(lexer)
    assert isinstance(number, _token.Number)
    assert number.value == "10.0"
    assert number.span == (21, 25)

    comma = next(lexer)
    assert isinstance(comma, _token.Comma)
    assert comma.value == ","
    assert comma.span == (25, 26)

    number = next(lexer)
    assert isinstance(number, _token.Number)
    assert number.value == "0.0"
    assert number.span == (27, 30)

    close_paren = next(lexer)
    assert isinstance(close_paren, _token.CloseParen)
    assert close_paren.value == ")"
    assert close_paren.span == (30, 31)

    with pytest.raises(StopIteration):
        next(lexer)


@pytest.mark.parametrize("whitespace", ["", " ", "\n", "\t"])
def test_lexer_formula_is_only_whitespace(whitespace: str) -> None:
    """Test handling of whitespace-only formula in the Lexer."""
    lexer = Lexer(whitespace)
    tokens = list(lexer)
    assert len(tokens) == 0


def test_lexer_invalid_component_id() -> None:
    """Test handling of invalid component IDs in the Lexer."""
    lexer = Lexer("#")
    with pytest.raises(FormulaSyntaxError) as ex_info:
        next(lexer)
    error_lines = str(ex_info.value).splitlines()
    assert error_lines == [
        "Formula syntax error:",
        "  Formula: #",
        "            ^ Expected integer after '#'",
    ]

    lexer = Lexer("#abc")
    with pytest.raises(FormulaSyntaxError) as ex_info:
        next(lexer)
    error_lines = str(ex_info.value).splitlines()
    assert error_lines == [
        "Formula syntax error:",
        "  Formula: #abc",
        "            ^ Expected integer after '#'",
    ]

    lexer = Lexer("#-1")
    with pytest.raises(FormulaSyntaxError) as ex_info:
        next(lexer)
    error_lines = str(ex_info.value).splitlines()
    assert error_lines == [
        "Formula syntax error:",
        "  Formula: #-1",
        "            ^ Expected integer after '#'",
    ]


def test_lexer_unexpected_character() -> None:
    """Test handling of unexpected characters in the Lexer."""
    lexer = Lexer("123 $ 456")
    with pytest.raises(FormulaSyntaxError) as ex_info:
        list(lexer)
    error_lines = str(ex_info.value).splitlines()
    assert error_lines == [
        "Formula syntax error:",
        "  Formula: 123 $ 456",
        "               ^ Unexpected character",
    ]
