# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Formula AST nodes and evaluation logic."""

from __future__ import annotations

import abc
import logging
import math
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Generic

from typing_extensions import override

from ..._internal._math import is_close_to_zero
from .._base_types import QuantityT, Sample
from ._functions import Function

_logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class Node(abc.ABC):
    """An abstract syntax tree node representing a formula expression."""

    span: tuple[int, int] | None = None

    @abc.abstractmethod
    def evaluate(self) -> float | None:
        """Evaluate the expression and return its numerical value."""

    @abc.abstractmethod
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the node."""

    @override
    def __str__(self) -> str:
        """Return the string representation of the node."""
        return self.format()


@dataclass(kw_only=True)
class TelemetryStream(Node, Generic[QuantityT]):
    """A AST node that retrieves values from a component's telemetry stream."""

    source: str
    stream: AsyncIterator[Sample[QuantityT]]
    _latest_sample: Sample[QuantityT] | None = None

    @property
    def latest_sample(self) -> Sample[QuantityT] | None:
        """Return the latest fetched sample for this component."""
        return self._latest_sample

    @override
    def evaluate(self) -> float | None:
        """Return the base value of the latest sample for this component."""
        if self._latest_sample is None:
            raise ValueError("Next value has not been fetched yet.")
        if self._latest_sample.value is None:
            return None
        return self._latest_sample.value.base_value

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the telemetry stream node."""
        return f"{self.source}"

    async def fetch_next(self) -> None:
        """Fetch the next value for this component and store it internally."""
        self._latest_sample = await anext(self.stream)


@dataclass(kw_only=True)
class FunCall(Node):
    """A function call in the formula."""

    function: Function
    args: list[Node]

    @override
    def evaluate(self) -> float | None:
        """Evaluate the function call with its arguments."""
        return self.function(arg.evaluate() for arg in self.args)

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the function call node."""
        args_str = ", ".join(str(arg) for arg in self.args)
        return f"{self.function.name}({args_str})"


@dataclass(kw_only=True)
class Constant(Node):
    """A constant numerical value in the formula."""

    value: float

    @override
    def evaluate(self) -> float | None:
        """Return the constant value."""
        return self.value

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the constant node."""
        return str(self.value)


@dataclass(kw_only=True)
class Add(Node):
    """Addition operation node."""

    left: Node
    right: Node

    @override
    def evaluate(self) -> float | None:
        """Evaluate the addition of the left and right nodes."""
        left = self.left.evaluate()
        right = self.right.evaluate()
        if left is None or right is None:
            return None
        return left + right

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the addition node."""
        expr = f"{self.left} + {self.right}"
        if wrap:
            expr = f"({expr})"
        return expr


@dataclass(kw_only=True)
class Sub(Node):
    """Subtraction operation node."""

    left: Node
    right: Node

    @override
    def evaluate(self) -> float | None:
        """Evaluate the subtraction of the right node from the left node."""
        left = self.left.evaluate()
        right = self.right.evaluate()
        if left is None or right is None:
            return None
        return left - right

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the subtraction node."""
        expr = f"{self.left} - {self.right.format(True)}"
        if wrap:
            expr = f"({expr})"
        return expr


@dataclass(kw_only=True)
class Mul(Node):
    """Multiplication operation node."""

    left: Node
    right: Node

    @override
    def evaluate(self) -> float | None:
        """Evaluate the multiplication of the left and right nodes."""
        left = self.left.evaluate()
        right = self.right.evaluate()
        if left is None or right is None:
            return None
        return left * right

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the multiplication node."""
        return f"{self.left.format(True)} * {self.right.format(True)}"


@dataclass(kw_only=True)
class Div(Node):
    """Division operation node."""

    left: Node
    right: Node

    @override
    def evaluate(self) -> float | None:
        """Evaluate the division of the left node by the right node."""
        left = self.left.evaluate()
        right = self.right.evaluate()
        if left is None or right is None:
            return None
        if is_close_to_zero(right):
            return math.nan
        return left / right

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the division node."""
        return f"{self.left.format(True)} / {self.right.format(True)}"
