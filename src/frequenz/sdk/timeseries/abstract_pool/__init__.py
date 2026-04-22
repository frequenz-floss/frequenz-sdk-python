# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Manage a pool of components."""

from ._abstract_pool import AbstractPool
from ._abstract_pool_reference_store import AbstractPoolReferenceStore

__all__ = [
    "AbstractPool",
    "AbstractPoolReferenceStore",
]
