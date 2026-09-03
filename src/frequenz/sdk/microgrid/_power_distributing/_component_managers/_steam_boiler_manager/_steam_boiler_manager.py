# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Manage steam boilers for the power distributor."""

import asyncio
import collections.abc
import logging
from datetime import timedelta

from frequenz.channels import LatestValueCache, Sender
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import SteamBoiler
from frequenz.quantities import Power
from typing_extensions import override

from ....._internal._math import is_close_to_zero
from .... import connection_manager
from ...._old_component_data import SteamBoilerData
from ..._component_pool_status_tracker import ComponentPoolStatusTracker
from ..._component_status import ComponentPoolStatus, SteamBoilerStatusTracker
from ...request import Request
from ...result import Result, Success
from .._component_manager import ComponentManager
from .._utils import _set_component_power

_logger = logging.getLogger(__name__)


class SteamBoilerManager(ComponentManager):
    """Manage steam boilers for the power distributor."""

    @override
    def __init__(
        self,
        component_pool_status_sender: Sender[ComponentPoolStatus],
        results_sender: Sender[Result],
        api_power_request_timeout: timedelta,
    ) -> None:
        """Initialize this instance.

        Args:
            component_pool_status_sender: Channel for sending information about which
                components are expected to be working.
            results_sender: Channel for sending results of power distribution.
            api_power_request_timeout: Timeout to use when making power requests to
                the microgrid API.
        """
        super().__init__()
        self._results_sender = results_sender
        self._api_power_request_timeout = api_power_request_timeout
        self._steam_boiler_ids = self._get_steam_boiler_ids()

        self._component_pool_status_tracker = (
            ComponentPoolStatusTracker(
                component_ids=self._steam_boiler_ids,
                component_status_sender=component_pool_status_sender,
                max_data_age=timedelta(seconds=10.0),
                max_blocking_duration=timedelta(seconds=30.0),
                component_status_tracker_type=SteamBoilerStatusTracker,
            )
            if self._steam_boiler_ids
            else None
        )
        self._component_data_caches: dict[
            ComponentId, LatestValueCache[SteamBoilerData]
        ] = {}

        self._task: asyncio.Task[None] | None = None

    @override
    def component_ids(self) -> collections.abc.Set[ComponentId]:
        """Return the set of steam boiler ids."""
        return self._steam_boiler_ids

    @override
    async def start(self) -> None:
        """Start the steam boiler manager."""
        self._component_data_caches = {
            steam_boiler_id: LatestValueCache(
                SteamBoilerData.subscribe(
                    connection_manager.get().api_client, steam_boiler_id
                ),
                unique_id=(
                    f"{type(self).__name__}«{hex(id(self))}»:"
                    f"steam_boiler«{steam_boiler_id}»"
                ),
            )
            for steam_boiler_id in self._steam_boiler_ids
        }

    @override
    async def stop(self) -> None:
        """Stop the steam boiler manager."""
        await asyncio.gather(
            *[cache.stop() for cache in self._component_data_caches.values()]
        )
        if self._component_pool_status_tracker:
            await self._component_pool_status_tracker.stop()
        await self._stop_all_unreachable_power_subscriptions()

    @override
    async def distribute_power(self, request: Request) -> None:
        """Distribute the requested power to the steam boilers.

        Args:
            request: Request to get the distribution for.

        Raises:
            ValueError: If no steam boilers are present in the component graph, but
                component_ids are provided in the request.
        """
        remaining_power = request.power
        allocations: dict[ComponentId, Power] = {}
        if not self._component_pool_status_tracker:
            if not request.component_ids:
                await self._results_sender.send(
                    Success(
                        succeeded_components=set(),
                        succeeded_power=Power.zero(),
                        excess_power=remaining_power,
                        request=request,
                    )
                )
                return
            raise ValueError(
                "Cannot distribute power to steam boilers - None found in the component graph."
            )

        working = self._component_pool_status_tracker.get_working_components(
            request.component_ids
        )
        await self._subscribe_to_unreachable_power(request.component_ids, working)
        unreachable_power = self._unreachable_power(request.component_ids)
        if unreachable_power is not None:
            remaining_power -= unreachable_power
            _logger.debug(
                "Excluding %s measured on unreachable steam boilers from the power "
                "to distribute on working steam boilers.",
                unreachable_power,
            )

        working_components: list[ComponentId] = []
        for comp_id in working:
            if self._component_data_caches[comp_id].has_value():
                working_components.append(comp_id)
            else:
                _logger.warning(
                    "Excluding steam boiler %s from power distribution due to "
                    "lack of data since startup.",
                    comp_id,
                )

        num_components = len(working_components)
        if num_components == 0:
            _logger.error(
                "No steam boilers available for power distribution. Aborting."
            )
            return

        working_components.sort(
            key=lambda comp_id: self._component_data_caches[comp_id]
            .get()
            .active_power_inclusion_upper_bound,
        )

        for idx, comp_id in enumerate(working_components):
            # When no power is left to distribute,
            # set power to zero for all remaining steam boilers.
            if remaining_power < Power.zero() or is_close_to_zero(
                remaining_power.as_watts()
            ):
                allocations[comp_id] = Power.zero()
                continue

            component_data = self._component_data_caches[comp_id]
            if not component_data.has_value():
                allocations[comp_id] = Power.zero()
                continue

            # Never allocate more than a steam boiler's upper power bound.
            upper_bound = Power.from_watts(
                component_data.get().active_power_inclusion_upper_bound
            )
            allocated_power = min(
                remaining_power,
                remaining_power / float(num_components - idx),
                upper_bound,
            )
            # A boiler with a minimum operating power can't run below it: raise
            # the allocation to the minimum when the remaining power covers it,
            # otherwise keep the boiler off.  Power that a kept-off boiler's
            # share would have used is not redistributed to earlier boilers;
            # it is reported as excess power in the result.
            lower_bound = Power.from_watts(
                component_data.get().active_power_inclusion_lower_bound
            )
            if Power.zero() < allocated_power < lower_bound:
                allocated_power = (
                    lower_bound
                    if lower_bound <= upper_bound and remaining_power >= lower_bound
                    else Power.zero()
                )
            allocations[comp_id] = allocated_power
            remaining_power -= allocated_power

        _logger.debug(
            "Distributing %s to steam boilers %s",
            request.power,
            allocations,
        )

        result = await _set_component_power(
            request=request,
            target_power=request.power,
            allocations=allocations,
            api_request_timeout=self._api_power_request_timeout,
            remaining_power=remaining_power,
            component_category="steam boiler",
        )
        await self._results_sender.send(result)

    @override
    def _unreachable_power_formula(
        self, component_ids: collections.abc.Set[ComponentId]
    ) -> str:
        """Return the formula for the active power of the given steam boilers."""
        return connection_manager.get().component_graph.steam_boiler_formula(
            component_ids
        )

    def _get_steam_boiler_ids(self) -> collections.abc.Set[ComponentId]:
        """Return the IDs of all steam boilers present in the component graph."""
        return {
            boiler.id
            for boiler in connection_manager.get().component_graph.components(
                matching_types=SteamBoiler
            )
        }
