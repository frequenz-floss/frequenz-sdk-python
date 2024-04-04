# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of PV inverters."""

import uuid

from ..._internal._channels import ReceiverFetcher
from .._base_types import SystemBounds
from ._pv_pool_reference_store import PVPoolReferenceStore


class PVPoolError(Exception):
    """An error that occurred in any of the PVPool methods."""


class PVPool:
    """An interface for interaction with pools of PV inverters.

    !!! note
        `PVPool` instances are not meant to be created directly by users. Use the
        [`microgrid.pv_pool`][frequenz.sdk.microgrid.pv_pool] method for creating
        `PVPool` instances.

    Provides:
      - Aggregate [`power`][frequenz.sdk.timeseries.pv_pool.PVPool.power]
        measurements of the PV inverters in the pool.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        pv_pool_ref: PVPoolReferenceStore,
        name: str | None,
        priority: int,
    ) -> None:
        """Initialize the instance.

        !!! note
            `PVPool` instances are not meant to be created directly by users. Use the
            [`microgrid.pv_pool`][frequenz.sdk.microgrid.pv_pool] method for creating
            `PVPool` instances.

        Args:
            pv_pool_ref: The reference store for the PV pool.
            name: The name of the PV pool.
            priority: The priority of the PV pool.
        """
        self._pv_pool_ref = pv_pool_ref
        unique_id = uuid.uuid4()
        self._source_id = unique_id if name is None else f"{name}-{unique_id}"
        self._priority = priority

    @property
    def _system_power_bounds(self) -> ReceiverFetcher[SystemBounds]:
        """Return a receiver fetcher for the system power bounds."""
        return self._pv_pool_ref.bounds_channel
