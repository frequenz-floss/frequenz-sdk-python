# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Formula tokens."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Token:
    """Base class for all tokens."""

    span: tuple[int, int]
    """The span (start, end) of the token in the input string."""

    value: str
    """The string value of the token."""


@dataclass
class Component(Token):
    """An electrical component token."""

    id: str
    """The unique ID of the component."""


@dataclass
class Plus(Token):
    """A plus operator token."""


@dataclass
class Minus(Token):
    """A minus operator token."""


@dataclass
class Mul(Token):
    """A multiplication operator token."""


@dataclass
class Div(Token):
    """A division operator token."""


@dataclass
class Number(Token):
    """A number token."""


@dataclass
class Symbol(Token):
    """A symbol token."""


@dataclass
class OpenParen(Token):
    """An open parenthesis token."""


@dataclass
class CloseParen(Token):
    """A close parenthesis token."""


@dataclass
class Comma(Token):
    """A comma token."""
