# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Formatter for the formula."""

from __future__ import annotations

import enum

from ._formula_steps import (
    Adder,
    Clipper,
    ConstantValue,
    Divider,
    FormulaStep,
    Maximizer,
    MetricFetcher,
    Minimizer,
    Multiplier,
    OpenParen,
    Subtractor,
)


class OperatorPrecedence(enum.Enum):
    """The precedence of an operator."""

    ADDITION = 1
    SUBTRACTION = 1
    MULTIPLICATION = 2
    DIVISION = 2
    PRIMARY = 9

    def __lt__(self, other: OperatorPrecedence) -> bool:
        """Test the precedence of this operator is less than the precedence of the other operator.

        Args:
            other: The other operator (on the right-hand side).

        Returns:
            Whether the precedence of this operator is less than the other operator.
        """
        return self.value < other.value

    def __le__(self, other: OperatorPrecedence) -> bool:
        """Test the precedence of this operator is less than or equal to the other operator.

        Args:
            other: The other operator (on the right-hand side).

        Returns:
            Whether the precedence of this operator is less than or equal to the other operator.
        """
        return self.value <= other.value


class Operator(enum.Enum):
    """The precedence of an operator."""

    ADDITION = "+"
    SUBTRACTION = "-"
    MULTIPLICATION = "*"
    DIVISION = "/"

    @property
    def precedence(self) -> OperatorPrecedence:
        """Return the precedence of this operator.

        Returns:
            The precedence of this operator.
        """
        match self:
            case Operator.SUBTRACTION:
                return OperatorPrecedence.SUBTRACTION
            case Operator.ADDITION:
                return OperatorPrecedence.ADDITION
            case Operator.DIVISION:
                return OperatorPrecedence.DIVISION
            case Operator.MULTIPLICATION:
                return OperatorPrecedence.MULTIPLICATION

    def __str__(self) -> str:
        """Return the string representation of the operator precedence.

        Returns:
            The string representation of the operator precedence.
        """
        return str(self.value)


class StackItem:
    """Stack item for the formula formatter."""

    def __init__(self, value: str, precedence: OperatorPrecedence, num_steps: int):
        """Initialize the StackItem.

        Args:
            value: The value of the stack item.
            precedence: The precedence of the stack item.
            num_steps: The number of steps of the stack item.
        """
        self.value = value
        self.precedence = precedence
        self.num_steps = num_steps

    def __str__(self) -> str:
        """Return the string representation of the stack item.

        This is used for debugging purposes.

        Returns:
            str: The string representation of the stack item.
        """
        return f'("{self.value}", {self.precedence}, {self.num_steps})'

    def as_left_value(self, outer_precedence: OperatorPrecedence) -> str:
        """Return the value of the stack item with parentheses if necessary.

        Args:
            outer_precedence: The precedence of the outer stack item.

        Returns:
            str: The value of the stack item with parentheses if necessary.
        """
        return f"({self.value})" if self.precedence < outer_precedence else self.value

    def as_right_value(self, outer_precedence: OperatorPrecedence) -> str:
        """Return the value of the stack item with parentheses if necessary.

        Args:
            outer_precedence: The precedence of the outer stack item.

        Returns:
            str: The value of the stack item with parentheses if necessary.
        """
        if self.num_steps > 1:
            return (
                f"({self.value})" if self.precedence <= outer_precedence else self.value
            )
        return f"({self.value})" if self.precedence < outer_precedence else self.value

    @staticmethod
    def create_binary(lhs: StackItem, operator: Operator, rhs: StackItem) -> StackItem:
        """Create a binary stack item.

        Args:
            lhs: The left-hand side of the binary operation.
            operator: The operator of the binary operation.
            rhs: The right-hand side of the binary operation.

        Returns:
            StackItem: The binary stack item.
        """
        pred = OperatorPrecedence(operator.precedence)
        return StackItem(
            f"{lhs.as_left_value(pred)} {operator} {rhs.as_right_value(pred)}",
            pred,
            lhs.num_steps + 1 + rhs.num_steps,
        )

    @staticmethod
    def create_primary(value: float) -> StackItem:
        """Create a stack item for literal values or function calls (primary expressions).

        Args:
            value: The value of the literal.

        Returns:
            StackItem: The literal stack item.
        """
        return StackItem(str(value), OperatorPrecedence.PRIMARY, 1)


class FormulaFormatter:
    """Formats a formula into a human readable string in infix-notation."""

    def __init__(self) -> None:
        """Initialize the FormulaFormatter."""
        self._stack = list[StackItem]()

    def format(self, postfix_expr: list[FormulaStep]) -> str:
        """Format the postfix expression to infix notation.

        Args:
            postfix_expr: The steps of the formula in postfix notation order.

        Returns:
            str: The formula in infix notation.
        """
        for step in postfix_expr:
            match step:
                case ConstantValue():
                    self._stack.append(StackItem.create_primary(step.value))
                case Adder():
                    self._format_binary(Operator.ADDITION)
                case Subtractor():
                    self._format_binary(Operator.SUBTRACTION)
                case Multiplier():
                    self._format_binary(Operator.MULTIPLICATION)
                case Divider():
                    self._format_binary(Operator.DIVISION)
                case Clipper():
                    the_value = self._stack.pop()
                    min_value = step.min_value if step.min_value is not None else "-inf"
                    max_value = step.max_value if step.max_value is not None else "inf"
                    value = f"clip({min_value}, {the_value.value}, {max_value})"
                    self._stack.append(StackItem(value, OperatorPrecedence.PRIMARY, 1))
                case Maximizer():
                    left, right = self._pop_two_from_stack()
                    value = f"max({left.value}, {right.value})"
                    self._stack.append(StackItem(value, OperatorPrecedence.PRIMARY, 1))
                case Minimizer():
                    left, right = self._pop_two_from_stack()
                    value = f"min({left.value}, {right.value})"
                    self._stack.append(StackItem(value, OperatorPrecedence.PRIMARY, 1))
                case MetricFetcher():
                    metric_fetcher = step
                    value = metric_fetcher._name  # pylint: disable=protected-access
                    if engine_reference := getattr(
                        metric_fetcher.stream, "_engine_reference", None
                    ):
                        value = f"[{value}]({str(engine_reference)})"
                    self._stack.append(StackItem(value, OperatorPrecedence.PRIMARY, 1))
                case OpenParen():
                    pass  # We gently ignore this one.

        assert (
            len(self._stack) == 1
        ), f"The formula {postfix_expr} is not valid. Evaluation stack left-over: {self._stack}"
        return self._stack[0].value

    def _format_binary(self, operator: Operator) -> None:
        """Format a binary operation.

        Pops the arguments of the binary expression from the stack
        and pushes the string representation of the binary operation to the stack.

        Args:
            operator: The operator of the binary operation.
        """
        left, right = self._pop_two_from_stack()
        self._stack.append(StackItem.create_binary(left, operator, right))

    def _pop_two_from_stack(self) -> tuple[StackItem, StackItem]:
        """Pop two items from the stack.

        Returns:
            The two items popped from the stack.
        """
        right = self._stack.pop()
        left = self._stack.pop()
        return left, right


def format_formula(postfix_expr: list[FormulaStep]) -> str:
    """Return the formula as a string in infix notation.

    Args:
        postfix_expr: The steps of the formula in postfix notation order.

    Returns:
        str: The formula in infix notation.
    """
    formatter = FormulaFormatter()
    return formatter.format(postfix_expr)
