# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Fuse data class."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Break circular import
    from . import Current


@dataclass(frozen=True)
class Fuse:
    """Fuse data class."""

    max_current: Current
    """Rated current of the fuse."""
