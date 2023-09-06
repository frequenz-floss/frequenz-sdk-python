# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A tokenizer for data pipeline formulas."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class StringIter:
    """An iterator for reading characters from a string."""

    def __init__(self, raw: str) -> None:
        """Create a `StringIter` instance.

        Args:
            raw: The raw string to create the iterator out of.
        """
        self._raw = raw
        self._max = len(raw)
        self._pos = 0

    @property
    def pos(self) -> int:
        """Return the position of the iterator in the raw string.

        Returns:
            The position of the iterator.
        """
        return self._pos

    @property
    def raw(self) -> str:
        """Return the raw string the iterator is created with.

        Returns:
            The base string of the iterator.
        """
        return self._raw

    def __iter__(self) -> StringIter:
        """Return an iterator to this class.

        Returns:
            self.
        """
        return self

    def __next__(self) -> str:
        """Return the next character in the raw string, and move forward.

        Returns:
            The next character.

        Raises:
            StopIteration: when there are no more characters in the string.
        """
        if self._pos < self._max:
            char = self._raw[self._pos]
            self._pos += 1
            return char
        raise StopIteration()

    def peek(self) -> str | None:
        """Return the next character in the raw string, without consuming it.

        Returns:
            The next character.
        """
        if self._pos < self._max:
            return self._raw[self._pos]
        return None


class TokenType(Enum):
    """Represents the types of tokens the Tokenizer can return."""

    COMPONENT_METRIC = 0
    """A component metric ID."""

    CONSTANT = 1
    """A constant value."""

    OPER = 2
    """An operator."""


@dataclass
class Token:
    """Represents a Token returned by the Tokenizer."""

    type: TokenType
    """The type of the token."""

    value: str
    """The value associated to the token."""


class Tokenizer:
    """A Tokenizer for breaking down a string formula into individual tokens.

    Every instance is an iterator that allows us to iterate over the individual tokens
    in the given formula.

    Formulas can have Component IDs that are preceeded by a pound symbol("#"), and these
    operators: +, -, *, /, (, ).

    For example, the input string: "#20 + #5" would produce three tokens:
     - COMPONENT_METRIC: 20
     - OPER: +
     - COMPONENT_METRIC: 5
    """

    def __init__(self, formula: str) -> None:
        """Create a `Tokenizer` instance.

        Args:
            formula: The string formula to tokenize.
        """
        self._formula = StringIter(formula)

    def _read_unsigned_int(self) -> str:
        """Read an unsigned int from the current position in the input string.

        Returns:
            A string containing the read unsigned int value.

        Raises:
            ValueError: when there is no unsigned int at the current position.
        """
        first_char = True
        result = ""

        while char := self._formula.peek():
            if not char.isdigit():
                if first_char:
                    raise ValueError(
                        f"Expected an integer. got '{char}', "
                        f"at pos {self._formula.pos} in formula {self._formula.raw}"
                    )
                break
            first_char = False
            result += char
            next(self._formula)
        return result

    def __iter__(self) -> Tokenizer:
        """Return an iterator to this class.

        Returns:
            self.
        """
        return self

    def __next__(self) -> Token:
        """Return the next token in the input string.

        Returns:
            The next token.

        Raises:
            ValueError: when there are unknown tokens in the input string.
            StopIteration: when there are no more tokens in the input string.
        """
        for char in self._formula:
            if char in (" ", "\n", "\r", "\t"):
                continue
            if char in ("+", "-", "*", "/", "(", ")"):
                return Token(TokenType.OPER, char)
            if char == "#":
                return Token(TokenType.COMPONENT_METRIC, self._read_unsigned_int())
            raise ValueError(
                f"Unable to parse character '{char}' at pos: {self._formula.pos}"
                f" in formula: {self._formula.raw}"
            )
        raise StopIteration()
