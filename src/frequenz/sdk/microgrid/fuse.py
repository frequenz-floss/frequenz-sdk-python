# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Fuse data class."""

from dataclasses import dataclass

from ..timeseries import Current


@dataclass(frozen=True)
class Fuse:
    """Fuse data class."""

    max_current: Current
    """Rated current of the fuse."""
