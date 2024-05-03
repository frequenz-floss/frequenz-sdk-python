# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the FormulaFormatter."""


from contextlib import AsyncExitStack

from frequenz.channels import Broadcast
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._quantities import Quantity
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

    async def test_higher_order_formula(self, mocker: MockerFixture) -> None:
        """Test that the formula is formatted correctly for a higher-order formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_batteries(3)
        mockgrid.add_ev_chargers(1)
        mockgrid.add_solar_inverters(2)

        async with mockgrid, AsyncExitStack() as stack:
            logical_meter = microgrid.logical_meter()
            stack.push_async_callback(logical_meter.stop)

            pv_pool = microgrid.pv_pool(priority=5)
            stack.push_async_callback(pv_pool.stop)

            grid = microgrid.grid()
            stack.push_async_callback(grid.stop)

            assert str(grid.power) == "#36 + #7 + #47 + #17 + #57 + #27"

            composed_formula = (grid.power - pv_pool.power).build("grid_minus_pv")
            assert (
                str(composed_formula)
                == "[grid-power](#36 + #7 + #47 + #17 + #57 + #27) - [pv-power](#48 + #58)"
            )
