# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Model interface."""

from ._manager import (
    EnumeratedModelRepository,
    ModelManager,
    ModelRepository,
    SingleModelRepository,
    WeekdayModelRepository,
)

# Explicitly declare the public API.
__all__ = [
    "EnumeratedModelRepository",
    "ModelManager",
    "ModelRepository",
    "SingleModelRepository",
    "WeekdayModelRepository",
]
