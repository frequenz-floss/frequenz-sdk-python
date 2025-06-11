# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the FormulaFormatter."""


from frequenz.channels import Broadcast
from frequenz.quantities import Quantity

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.formula_engine._formula_engine import FormulaBuilder
from frequenz.sdk.timeseries.formula_engine._formula_formatter import format_formula
from frequenz.sdk.timeseries.formula_engine._formula_steps import (
    Clipper,
    ConstantValue,
    FormulaStep,
    Maximizer,
    Minimizer,
)
from frequenz.sdk.timeseries.formula_engine._tokenizer import Tokenizer, TokenType


def build_formula(formula: str) -> list[FormulaStep]:
    """Parse the formula and returns the steps.

    Args:
        formula: The formula in infix notation.

    Returns:
        The formula in postfix notation steps.
    """
    channels: dict[str, Broadcast[Sample[Quantity]]] = {}
    builder = FormulaBuilder("test_formula", Quantity)
    nones_are_zeros = True

    for token in Tokenizer(formula):
        if token.type == TokenType.COMPONENT_METRIC:
            if token.value not in channels:
                channels[token.value] = Broadcast(name=token.value)
            builder.push_metric(
                name=f"#{token.value}",
                data_stream=channels[token.value].new_receiver(),
                nones_are_zeros=nones_are_zeros,
            )
        elif token.type == TokenType.OPER:
            builder.push_oper(token.value)
    steps, _ = builder.finalize()
    return steps


def reconstruct(formula: str) -> str:
    """Parse the formula and reconstructs it from the steps.

    Args:
        formula: The formula in infix notation.

    Returns:
        The reconstructed formula in infix notation.
    """
    steps = build_formula(formula)
    reconstructed = format_formula(steps)
    if formula != reconstructed:
        print(f"Formula: input {formula} != output {reconstructed}")
    return reconstructed


class TestFormulaFormatter:
    """Tests for the FormulaFormatter."""

    async def test_basic_precedence(self) -> None:
        """Test that the formula is wrapped in parentheses for operators of different precedence."""
        assert reconstruct("#2 + #3 * #4") == "#2 + #3 * #4"

    def test_all_same_precedence(self) -> None:
        """Test that the formula is not wrapped in parentheses for operators of same precedence."""
        assert reconstruct("#2 + #3 + #4") == "#2 + #3 + #4"

    def test_lhs_precedence(self) -> None:
        """Test that the left-hand side of a binary operation is wrapped in parentheses."""
        assert reconstruct("(#2 - #3) - #4") == "#2 - #3 - #4"
        assert reconstruct("#2 - #3 - #4") == "#2 - #3 - #4"
        assert reconstruct("(#2 - #3) * #4") == "(#2 - #3) * #4"

    def test_rhs_precedence(self) -> None:
        """Test that the right-hand side of a binary operation is wrapped in parentheses if needed."""
        assert reconstruct("#2 + #3") == "#2 + #3"
        assert reconstruct("#2 - #3") == "#2 - #3"
        assert reconstruct("#2 + #3 + #4") == "#2 + #3 + #4"
        assert reconstruct("#2 - #3 - #4") == "#2 - #3 - #4"
        assert reconstruct("#2 - #3 * #4") == "#2 - #3 * #4"
        assert reconstruct("#2 - (#3 * #4)") == "#2 - #3 * #4"
        assert reconstruct("#2 - (#3 - #4)") == "#2 - (#3 - #4)"
        assert reconstruct("#2 - (#3 + #4)") == "#2 - (#3 + #4)"

    def test_rhs_parenthesis(self) -> None:
        """Test that the right-hand side of a binary operation is wrapped in parentheses."""
        assert reconstruct("#2 / (#3 - #4)") == "#2 / (#3 - #4)"

    def test_functions(self) -> None:
        """Test that the functions are formatted correctly."""
        # For simplicity, we only test with constant values.
        # fmt: off
        # flake8: noqa: E501
        assert format_formula([ConstantValue(2), ConstantValue(3), Minimizer()]) == "min(2, 3)"
        assert format_formula([ConstantValue(2), ConstantValue(3), Maximizer()]) == "max(2, 3)"
        assert format_formula([ConstantValue(3.5), Clipper(0.0, 1.0)]) == "clip(0.0, 3.5, 1.0)"
        # flake8: enable
        # fmt: on

    async def test_higher_order_formula(self) -> None:
        """Test that higher-order formulas (formulas combining other formulas) are formatted correctly."""
        # Create two base formulas
        builder1 = FormulaBuilder("test_formula1", Quantity)
        builder2 = FormulaBuilder("test_formula2", Quantity)

        # Push metrics directly to the builders
        channel1 = Broadcast[Sample[Quantity]](name="channel1")
        channel2 = Broadcast[Sample[Quantity]](name="channel2")
        builder1.push_metric("#1", channel1.new_receiver(), nones_are_zeros=True)
        builder1.push_oper("+")
        builder1.push_metric("#2", channel2.new_receiver(), nones_are_zeros=True)

        channel3 = Broadcast[Sample[Quantity]](name="channel3")
        channel4 = Broadcast[Sample[Quantity]](name="channel4")
        builder2.push_metric("#3", channel3.new_receiver(), nones_are_zeros=True)
        builder2.push_oper("+")
        builder2.push_metric("#4", channel4.new_receiver(), nones_are_zeros=True)

        # Build individual formula engines first
        engine1 = builder1.build()
        engine2 = builder2.build()

        # Combine them into a higher-order formula
        composed_formula = (engine1 - engine2).build("higher_order_formula")

        # Check the string representation
        assert (
            str(composed_formula)
            == "[test_formula1](#1 + #2) - [test_formula2](#3 + #4)"
        )
