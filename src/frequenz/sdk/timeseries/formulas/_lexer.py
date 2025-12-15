# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""A lexer for formula strings."""


from __future__ import annotations

from collections.abc import Iterator

from typing_extensions import override

from . import _token
from ._peekable import Peekable


class Lexer(Iterator[_token.Token]):
    """A lexer for formula strings."""

    def __init__(self, formula: str):
        """Initialize this instance.

        Args:
            formula: The formula string to lex.
        """
        self._formula: str = formula
        self._iter: Peekable[tuple[int, str]] = Peekable(enumerate(iter(formula)))

    def _read_integer(self) -> str:
        num_str = ""
        peek = self._iter.peek()
        while peek is not None and peek[1].isdigit():
            _, char = next(self._iter)
            num_str += char
            peek = self._iter.peek()
        return num_str

    def _read_number(self) -> str:
        num_str = ""
        peek = self._iter.peek()
        while peek is not None and (peek[1].isdigit() or peek[1] == "."):
            _, char = next(self._iter)
            num_str += char
            peek = self._iter.peek()
        return num_str

    def _read_symbol(self) -> str:
        word_str = ""
        peek = self._iter.peek()
        while peek is not None and peek[1].isalnum():
            _, char = next(self._iter)
            word_str += char
            peek = self._iter.peek()
        return word_str

    @override
    def __iter__(self) -> Lexer:
        """Return the iterator itself."""
        return self

    @override
    def __next__(self) -> _token.Token:  # pylint: disable=too-many-branches
        """Return the next token from the formula string."""
        peek = self._iter.peek()
        while peek is not None and peek[1].isspace():
            _ = next(self._iter)
            peek = self._iter.peek()

        if peek is None:
            raise StopIteration

        pos, char = peek
        if char == "#":
            _ = next(self._iter)  # consume '#'
            comp_id = self._read_integer()
            if not comp_id:
                raise ValueError(f"Expected integer after '#' at position {pos}")
            end_pos = pos + len(comp_id)
            return _token.Component(
                span=(
                    pos + 1,
                    end_pos + 1,  # account for '#'
                ),
                id=comp_id,
                value=self._formula[pos:end_pos],
            )

        if char == "+":
            _, char = next(self._iter)  # consume operator
            return _token.Plus(span=(pos + 1, pos + 1), value=char)

        if char == "-":
            _, char = next(self._iter)
            return _token.Minus(span=(pos + 1, pos + 1), value=char)

        if char == "*":
            _, char = next(self._iter)
            return _token.Mul(span=(pos + 1, pos + 1), value=char)

        if char == "/":
            _, char = next(self._iter)
            return _token.Div(span=(pos + 1, pos + 1), value=char)

        if char == "(":
            _, char = next(self._iter)
            return _token.OpenParen(span=(pos + 1, pos + 1), value=char)

        if char == ")":
            _, char = next(self._iter)
            return _token.CloseParen(span=(pos + 1, pos + 1), value=char)

        if char == ",":
            _, char = next(self._iter)
            return _token.Comma(span=(pos + 1, pos + 1), value=char)

        if char.isdigit():
            num = self._read_number()
            end_pos = pos + len(num)
            return _token.Number(span=(pos + 1, end_pos), value=num)

        if char.isalpha():
            symbol = self._read_symbol()
            end_pos = pos + len(symbol)
            return _token.Symbol(span=(pos + 1, end_pos), value=symbol)

        raise ValueError(f"Unexpected character '{char}' at position {pos}")
