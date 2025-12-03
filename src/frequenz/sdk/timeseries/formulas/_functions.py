# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Function implementations for evaluating formulas."""

from __future__ import annotations

import abc
from dataclasses import dataclass

from typing_extensions import override

from ._base_ast_node import AstNode


@dataclass
class Function(abc.ABC):
    """A function that can be called in a formula expression."""

    params: list[AstNode]

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Return the name of the function."""

    @abc.abstractmethod
    def __call__(self) -> float | None:
        """Call the function with the given arguments."""

    def format(self) -> str:
        """Return a string representation of the function."""
        params_str = ", ".join(str(param) for param in self.params)
        return f"{self.name}({params_str})"

    @classmethod
    def from_string(cls, name: str, params: list[AstNode]) -> Function:
        """Create a function instance from its name."""
        match name.upper():
            case "COALESCE":
                return Coalesce(params)
            case "MAX":
                return Max(params)
            case "MIN":
                return Min(params)
            case _:
                raise ValueError(f"Unknown function name: {name}")


class Coalesce(Function):
    """A function that returns the first non-None argument."""

    @property
    @override
    def name(self) -> str:
        """Return the name of the function."""
        return "COALESCE"

    @override
    def __call__(self) -> float | None:
        """Return the first non-None argument."""
        for param in self.params:
            arg = param.evaluate()
            if arg is not None:
                return arg
        return None


class Max(Function):
    """A function that returns the maximum of the arguments."""

    @property
    @override
    def name(self) -> str:
        """Return the name of the function."""
        return "MAX"

    @override
    def __call__(self) -> float | None:
        """Return the maximum of the arguments."""
        max_value: float | None = None
        for param in self.params:
            arg = param.evaluate()
            if arg is None:
                return None
            if max_value is None or arg > max_value:
                max_value = arg
        return max_value


class Min(Function):
    """A function that returns the minimum of the arguments."""

    @property
    @override
    def name(self) -> str:
        """Return the name of the function."""
        return "MIN"

    @override
    def __call__(self) -> float | None:
        """Return the minimum of the arguments."""
        min_value: float | None = None
        for param in self.params:
            arg = param.evaluate()
            if arg is None:
                return None
            if min_value is None or arg < min_value:
                min_value = arg
        return min_value
