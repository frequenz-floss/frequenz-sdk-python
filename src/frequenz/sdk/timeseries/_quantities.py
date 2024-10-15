# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Types for holding quantities with units."""

# pylint: disable=too-many-lines

from __future__ import annotations

from typing import TypeVar

from frequenz.quantities import Quantity

QuantityT = TypeVar("QuantityT", bound=Quantity)
"""Type variable for representing various quantity types."""
