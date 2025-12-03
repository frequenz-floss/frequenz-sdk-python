# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Formula AST node base class."""

import abc
from dataclasses import dataclass

from typing_extensions import override


@dataclass(kw_only=True)
class AstNode(abc.ABC):
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
