# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Helper class to compare two sequences without caring about the underlying type."""


from collections.abc import Sequence
from typing import Any


# Disabling the lint because it reads easier in tests like this
class a_sequence:  # pylint: disable=invalid-name
    """Helper class to compare two sequences without caring about the underlying type.

    Examples:
        >>> from collections import deque
        >>> (1, 2) == a_sequence(1, 2)
        >>> True
        >>> deque([1, 2]) == a_sequence(1, 2)
        >>> True
        >>> deque([2, 1]) == a_sequence(1, 2)
        >>> False
        >>> 1 == a_sequence(1, 2)
        >>> False
    """

    def __init__(self, *sequence: Any) -> None:
        """Create an instance."""
        self.sequence: Sequence[Any] = sequence

    def __eq__(self, other: Any) -> bool:
        """# noqa D105 (Missing docstring in magic method)."""
        if not isinstance(other, Sequence):
            return False
        return list(self.sequence) == list(other)

    def __str__(self) -> str:
        """# noqa D105 (Missing docstring in magic method)."""
        return f"{self.__class__.__name__}({', '.join(str(s) for s in self.sequence)})"

    def __repr__(self) -> str:
        """# noqa D105 (Missing docstring in magic method)."""
        return f"{self.__class__.__name__}({', '.join(repr(s) for s in self.sequence)})"
