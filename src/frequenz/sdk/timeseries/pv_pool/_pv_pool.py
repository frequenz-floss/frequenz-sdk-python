# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of PV inverters."""

import asyncio
import typing
import uuid

from ..._internal._channels import ReceiverFetcher
from ...actor import _power_managing
from .._base_types import SystemBounds
from ._pv_pool_reference_store import PVPoolReferenceStore
from ._result_types import PVPoolReport


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
        self._source_id = str(unique_id) if name is None else f"{name}-{unique_id}"
        self._priority = priority

    @property
    def power_status(self) -> ReceiverFetcher[PVPoolReport]:
        """Get a receiver to receive new power status reports when they change.

        These include
          - the current inclusion/exclusion bounds available for the pool's priority,
          - the current target power for the pool's set of batteries,
          - the result of the last distribution request for the pool's set of batteries.

        Returns:
            A receiver that will stream power status reports for the pool's priority.
        """
        sub = _power_managing.ReportRequest(
            source_id=self._source_id,
            priority=self._priority,
            component_ids=self._pv_pool_ref.component_ids,
        )
        self._pv_pool_ref.power_bounds_subs[sub.get_channel_name()] = (
            asyncio.create_task(
                self._pv_pool_ref.power_manager_bounds_subs_sender.send(sub)
            )
        )
        channel = self._pv_pool_ref.channel_registry.get_or_create(
            _power_managing._Report,  # pylint: disable=protected-access
            sub.get_channel_name(),
        )
        channel.resend_latest = True

        # More details on why the cast is needed here:
        # https://github.com/frequenz-floss/frequenz-sdk-python/issues/823
        return typing.cast(ReceiverFetcher[PVPoolReport], channel)

    @property
    def _system_power_bounds(self) -> ReceiverFetcher[SystemBounds]:
        """Return a receiver fetcher for the system power bounds."""
        return self._pv_pool_ref.bounds_channel
