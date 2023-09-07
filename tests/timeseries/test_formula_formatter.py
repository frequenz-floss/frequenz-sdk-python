# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the FormulaFormatter."""


import logging

from frequenz.channels import Broadcast
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._formula_engine._formula_engine import FormulaBuilder
from frequenz.sdk.timeseries._formula_engine._formula_formatter import FormulaFormatter
from frequenz.sdk.timeseries._formula_engine._formula_steps import (
    Averager,
    Clipper,
    ConstantValue,
    FormulaStep,
    Maximizer,
    Minimizer,
)
from frequenz.sdk.timeseries._formula_engine._tokenizer import Tokenizer, TokenType
from frequenz.sdk.timeseries._quantities import Percentage, Quantity
from tests.timeseries.mock_microgrid import MockMicrogrid

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
                channels[token.value] = Broadcast(token.value)
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
    reconstructed = FormulaFormatter.format(steps)
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
        assert FormulaFormatter.format([ConstantValue(2), ConstantValue(3), Minimizer()]) == "min(2, 3)"
        assert FormulaFormatter.format([ConstantValue(2), ConstantValue(3), Maximizer()]) == "max(2, 3)"
        assert FormulaFormatter.format([ConstantValue(3.5), Clipper(0.0, 1.0)]) == "clip(0.0, 3.5, 1.0)"
        # flake8: enable
        # fmt: on

    def test_function_avg(self) -> None:
        """Test that the avg function is formatted correctly."""
        # This tests the special case of the avg function with no arguments.
        assert FormulaFormatter.format([Averager[Percentage]([])]) == "avg()"

    async def test_higher_order_formula(self, mocker: MockerFixture) -> None:
        """Test that the formula is formatted correctly for a higher-order formula."""
        mockgrid = MockMicrogrid(grid_meter=False)
        mockgrid.add_batteries(3)
        mockgrid.add_ev_chargers(1)
        mockgrid.add_solar_inverters(2)
        await mockgrid.start(mocker)

        logical_meter = microgrid.logical_meter()
        assert str(logical_meter.grid_power) == "#36 + #7 + #47 + #17 + #57 + #27"

        composed_formula = (logical_meter.grid_power - logical_meter.pv_power).build(
            "grid_minus_pv"
        )
        assert (
            str(composed_formula)
            == "[grid-power](#36 + #7 + #47 + #17 + #57 + #27) - [pv-power](#57 + #47)"
        )

        await mockgrid.cleanup()