"""Defines the connections between microgrid components.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from typing import NamedTuple


class Connection(NamedTuple):
    """Metadata for a connection between microgrid components."""

    start: int
    end: int

    def is_valid(self) -> bool:
        """Check if this instance contains valid data.

        Returns:
            `True` if `start >= 0`, `end > 0`, and `start != end`, `False`
            otherwise.
        """
        return self.start >= 0 and self.end > 0 and self.start != self.end
