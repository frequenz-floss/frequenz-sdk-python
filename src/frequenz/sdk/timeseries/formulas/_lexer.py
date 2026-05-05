# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""A lexer for formula strings."""

from __future__ import annotations

from collections.abc import Iterator

from more_itertools import peekable
from typing_extensions import override

from . import _token
from ._exceptions import FormulaSyntaxError


class Lexer(Iterator[_token.Token]):
    """A lexer for formula strings."""

    def __init__(self, formula: str):
        """Initialize this instance.

        Args:
            formula: The formula string to lex.
        """
        self._formula: str = formula
        self._iter: peekable[tuple[int, str]] = peekable(enumerate(iter(formula)))

    def _peek_char(self) -> tuple[int, str] | None:
        """Return the next position and character, or None if _iter is at its end."""
        try:
            return self._iter.peek()
        except StopIteration:
            return None

    def _read_integer(self) -> str:
        num_str = ""
        peek = self._peek_char()
        while peek is not None and peek[1].isdigit():
            _, char = next(self._iter)
            num_str += char
            peek = self._peek_char()
        return num_str

    def _read_number(self) -> str:
        num_str = ""
        peek = self._peek_char()
        while peek is not None and (peek[1].isdigit() or peek[1] == "."):
            _, char = next(self._iter)
            num_str += char
            peek = self._peek_char()
        return num_str

    def _read_symbol(self) -> str:
        word_str = ""
        peek = self._peek_char()
        while peek is not None and peek[1].isalnum():
            _, char = next(self._iter)
            word_str += char
            peek = self._peek_char()
        return word_str

    @override
    def __iter__(self) -> Lexer:
        """Return the iterator itself."""
        return self

    @override
    def __next__(self) -> _token.Token:  # pylint: disable=too-many-branches
        """Return the next token from the formula string."""
        peek = self._peek_char()
        while peek is not None and peek[1].isspace():
            _ = next(self._iter)
            peek = self._peek_char()

        if peek is None:
            raise StopIteration

        pos, char = peek
        if char == "#":
            _ = next(self._iter)  # consume '#'
            comp_id = self._read_integer()
            if not comp_id:
                raise FormulaSyntaxError(
                    formula=self._formula,
                    span=(pos + 1, pos + 2),
                    message="Expected integer after '#'",
                )
            end_pos = pos + len(comp_id) + 1  # account for '#'
            return _token.Component(
                span=(pos, end_pos),
                id=comp_id,
                value=self._formula[pos:end_pos],
            )

        if char == "+":
            _, char = next(self._iter)  # consume operator
            return _token.Plus(span=(pos, pos + 1), value=char)

        if char == "-":
            _, char = next(self._iter)
            return _token.Minus(span=(pos, pos + 1), value=char)

        if char == "*":
            _, char = next(self._iter)
            return _token.Mul(span=(pos, pos + 1), value=char)

        if char == "/":
            _, char = next(self._iter)
            return _token.Div(span=(pos, pos + 1), value=char)

        if char == "(":
            _, char = next(self._iter)
            return _token.OpenParen(span=(pos, pos + 1), value=char)

        if char == ")":
            _, char = next(self._iter)
            return _token.CloseParen(span=(pos, pos + 1), value=char)

        if char == ",":
            _, char = next(self._iter)
            return _token.Comma(span=(pos, pos + 1), value=char)

        if char.isdigit():
            num = self._read_number()
            end_pos = pos + len(num)
            return _token.Number(span=(pos, end_pos), value=num)

        if char.isalpha():
            symbol = self._read_symbol()
            end_pos = pos + len(symbol)
            return _token.Symbol(span=(pos, end_pos), value=symbol)

        raise FormulaSyntaxError(
            formula=self._formula,
            span=(pos, pos + 1),
            message="Unexpected character",
        )
