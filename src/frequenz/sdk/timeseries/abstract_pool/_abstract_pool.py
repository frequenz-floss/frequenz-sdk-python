# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Abstract base class for component pools."""

import asyncio
import uuid
from abc import ABC, abstractmethod
from collections import abc

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power

from frequenz.sdk._internal._channels import MappingReceiverFetcher, ReceiverFetcher
from frequenz.sdk.microgrid import _power_distributing, _power_managing
from frequenz.sdk.timeseries import Bounds
from frequenz.sdk.timeseries._base_types import SystemBounds
from frequenz.sdk.timeseries.abstract_pool._abstract_pool_reference_store import (
    AbstractPoolReferenceStore,
)
from frequenz.sdk.timeseries.formulas import Formula


class AbstractPool(ABC):
    """Abstract base class for component pools."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        pool_ref_store: AbstractPoolReferenceStore,
        name: str | None,
        priority: int,
    ) -> None:
        """Create an `AbstractPool` instance.

        Args:
            pool_ref_store: The pool reference store instance.
            name: An optional name used to identify this instance of the pool or a
                corresponding actor in the logs.
            priority: The priority of the actor using this wrapper.
        """
        self._pool_ref_store = pool_ref_store
        unique_id = str(uuid.uuid4())
        self._source_id = unique_id if name is None else f"{name}-{unique_id}"
        self._priority = priority

    @property
    def component_ids(self) -> abc.Set[ComponentId]:
        """Return component IDs of all component IDs managed by this pool.

        Returns:
            Set of managed component IDs.
        """
        return self._pool_ref_store.component_ids

    async def propose_power(
        self,
        power: Power | None,
        bounds: Bounds[Power | None] = Bounds(None, None),
    ) -> None:
        """Send a proposal to the power manager for the pool's underlying components.

        This proposal is for the maximum power that can be set for the components in
        the pool. The actual production or consumption might be lower.

        Details on how the power manager handles proposals can be found in the
        [Microgrid][frequenz.sdk.microgrid--setting-power] documentation.

        Args:
            power: The power to propose.  If `None`,
                this proposal will not have any effect on the target power, unless
                bounds are specified.  When specified without bounds, bounds for lower
                priority actors will be shifted by this power.  If both are `None`, it
                is equivalent to not having a proposal or withdrawing a previous one.
            bounds: The power bounds for the proposal. When specified, these bounds will
                limit the bounds for lower priority actors.
        """
        await self._pool_ref_store.power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._source_id,
                preferred_power=power,
                bounds=bounds,
                component_ids=self._pool_ref_store.component_ids,
                priority=self._priority,
                creation_time=asyncio.get_running_loop().time(),
            )
        )

    @property
    @abstractmethod
    def power(self) -> Formula[Power]:
        """Fetch the total power for the components in the pool.

        Returns:
            A Formula that will calculate and stream the total power of all
            components in the pool.
        """

    @property
    def power_status(self) -> ReceiverFetcher[_power_managing._Report]:
        """Get a receiver to receive new power status reports when they change.

        These include
          - the current inclusion/exclusion bounds available for the pool's priority,
          - the current target power for the pool's set of components,
          - the result of the last distribution request for the pool's set of components,.

        Returns:
            A receiver that will stream power status reports for the pool's priority.
        """
        sub = _power_managing.ReportRequest(
            source_id=self._source_id,
            priority=self._priority,
            component_ids=self._pool_ref_store.component_ids,
        )
        self._pool_ref_store.power_bounds_subs[sub.get_channel_name()] = (
            asyncio.create_task(
                self._pool_ref_store.power_manager_bounds_subs_sender.send(sub)
            )
        )
        channel = self._pool_ref_store.channel_registry.get_or_create(
            _power_managing._Report,  # pylint: disable=protected-access
            sub.get_channel_name(),
        )
        channel.resend_latest = True

        return channel

    @property
    def power_distribution_results(self) -> ReceiverFetcher[_power_distributing.Result]:
        """Get a receiver to receive power distribution results.

        Returns:
            A receiver that will stream power distribution results for the pool's set of
            components.
        """
        return MappingReceiverFetcher(
            self._pool_ref_store.power_distribution_results_fetcher,
            lambda recv: recv.filter(
                lambda x: x.request.component_ids == self._pool_ref_store.component_ids
            ),
        )

    @property
    def _system_power_bounds(self) -> ReceiverFetcher[SystemBounds]:
        """Return a receiver fetcher for the system power bounds."""
        return self._pool_ref_store.bounds_channel

    async def stop(self) -> None:
        """Stop all tasks and channels owned by the pool."""
        # This was closing the pool_ref_store, which is not correct, because those are
        # shared.
        #
        # This method will do until we have a mechanism to track the resources created
        # through it. It can also eventually cleanup the pool_ref_store, when it is
        # holding the last reference to it.
