# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the formula lexer."""

from frequenz.sdk.timeseries.formulas import _token
from frequenz.sdk.timeseries.formulas._lexer import Lexer


def test_lexer() -> None:
    """Test the Lexer reading integer tokens."""
    formula = "#123 + coalesce(#1 / 10.0, 0.0)"
    lexer = Lexer(formula)
    component = next(lexer)
    assert isinstance(component, _token.Component)
    assert component.id == "123"
    assert component.span == (1, 4)

    plus_op = next(lexer)
    assert isinstance(plus_op, _token.Plus)
    assert plus_op.value == "+"
    assert plus_op.span == (6, 6)

    number = next(lexer)
    assert isinstance(number, _token.Symbol)
    assert number.value == "coalesce"
    assert number.span == (8, 15)

    open_paren = next(lexer)
    assert isinstance(open_paren, _token.OpenParen)
    assert open_paren.value == "("
    assert open_paren.span == (16, 16)

    component = next(lexer)
    assert isinstance(component, _token.Component)
    assert component.id == "1"
    assert component.span == (17, 18)

    div_op = next(lexer)
    assert isinstance(div_op, _token.Div)
    assert div_op.value == "/"
    assert div_op.span == (20, 20)

    number = next(lexer)
    assert isinstance(number, _token.Number)
    assert number.value == "10.0"
    assert number.span == (22, 25)

    comma = next(lexer)
    assert isinstance(comma, _token.Comma)
    assert comma.value == ","
    assert comma.span == (26, 26)

    number = next(lexer)
    assert isinstance(number, _token.Number)
    assert number.value == "0.0"
    assert number.span == (28, 30)

    close_paren = next(lexer)
    assert isinstance(close_paren, _token.CloseParen)
    assert close_paren.value == ")"
    assert close_paren.span == (31, 31)

    try:
        _ = next(lexer)
        assert False, "Expected StopIteration"
    except StopIteration:
        pass
