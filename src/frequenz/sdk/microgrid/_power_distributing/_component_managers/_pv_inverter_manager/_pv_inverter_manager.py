# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Manage PV inverters for the power distributor."""

import asyncio
import collections.abc
import logging
from datetime import timedelta

from frequenz.channels import Broadcast, LatestValueCache, Sender
from frequenz.client.microgrid import (
    ApiClientError,
    ComponentCategory,
    InverterData,
    InverterType,
)
from frequenz.quantities import Power
from typing_extensions import override

from ....._internal._math import is_close_to_zero
from .... import connection_manager
from ..._component_pool_status_tracker import ComponentPoolStatusTracker
from ..._component_status import ComponentPoolStatus, PVInverterStatusTracker
from ...request import Request
from ...result import PartialFailure, Result, Success
from .._component_manager import ComponentManager

_logger = logging.getLogger(__name__)


class PVManager(ComponentManager):
    """Manage PV inverters for the power distributor."""

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
        self._results_sender = results_sender
        self._api_power_request_timeout = api_power_request_timeout
        self._pv_inverter_ids = self._get_pv_inverter_ids()

        self._component_pool_status_tracker = (
            ComponentPoolStatusTracker(
                component_ids=self._pv_inverter_ids,
                component_status_sender=component_pool_status_sender,
                max_data_age=timedelta(seconds=10.0),
                max_blocking_duration=timedelta(seconds=30.0),
                component_status_tracker_type=PVInverterStatusTracker,
            )
            if self._pv_inverter_ids
            else None
        )
        self._component_data_caches: dict[int, LatestValueCache[InverterData]] = {}
        self._target_power = Power.zero()
        self._target_power_channel = Broadcast[Request](name="target_power")
        self._target_power_tx = self._target_power_channel.new_sender()
        self._task: asyncio.Task[None] | None = None

    @override
    def component_ids(self) -> collections.abc.Set[int]:
        """Return the set of PV inverter ids."""
        return self._pv_inverter_ids

    @override
    async def start(self) -> None:
        """Start the PV inverter manager."""
        self._component_data_caches = {
            inv_id: LatestValueCache(
                await connection_manager.get().api_client.inverter_data(inv_id),
                unique_id=f"{type(self).__name__}«{hex(id(self))}»:inverter«{inv_id}»",
            )
            for inv_id in self._pv_inverter_ids
        }

    @override
    async def stop(self) -> None:
        """Stop the PV inverter manager."""
        await asyncio.gather(
            *[tracker.stop() for tracker in self._component_data_caches.values()]
        )
        if self._component_pool_status_tracker:
            await self._component_pool_status_tracker.stop()

    @override
    async def distribute_power(self, request: Request) -> None:
        """Distribute the requested power to the PV inverters.

        Args:
            request: Request to get the distribution for.

        Raises:
            ValueError: If no PV inverters are present in the component graph, but
                component_ids are provided in the request.
        """
        remaining_power = request.power
        allocations: dict[int, Power] = {}
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
                "Cannot distribute power to PV inverters without any inverters"
            )

        working_components: list[int] = []
        for inv_id in self._component_pool_status_tracker.get_working_components(
            request.component_ids
        ):
            if self._component_data_caches[inv_id].has_value():
                working_components.append(inv_id)
            else:
                _logger.warning(
                    "Excluding PV inverter %s from power distribution due to "
                    "lack of data since startup.",
                    inv_id,
                )

        # When sorting by lower bounds, which are negative for PV inverters, we have to
        # reverse the order, so that the inverters with the higher bounds i.e., the
        # least absolute value are first.
        working_components.sort(
            key=lambda inv_id: self._component_data_caches[inv_id]
            .get()
            .active_power_inclusion_lower_bound,
            reverse=True,
        )

        num_components = len(working_components)
        if num_components == 0:
            _logger.error("No PV inverters available for power distribution. Aborting.")
            return

        for idx, inv_id in enumerate(working_components):
            # Request powers are negative for PV inverters.  When remaining power is
            # greater than or equal to 0.0, we can stop allocating further, and set 0
            # power for all inverters for which no allocations were made.
            if remaining_power > Power.zero() or is_close_to_zero(
                remaining_power.as_watts()
            ):
                allocations[inv_id] = Power.zero()
                continue
            distribution = remaining_power / float(num_components - idx)
            inv_data = self._component_data_caches[inv_id]
            if not inv_data.has_value():
                allocations[inv_id] = Power.zero()
                # Can't get device bounds, so can't use inverter.
                continue
            discharge_bounds = Power.from_watts(
                inv_data.get().active_power_inclusion_lower_bound
            )
            # Because all 3 values are negative or 0, we use max, to get the value
            # with the least absolute value.
            allocated_power = max(remaining_power, discharge_bounds, distribution)
            allocations[inv_id] = allocated_power
            remaining_power -= allocated_power

        _logger.debug(
            "Distributing %s to PV inverters %s",
            request.power,
            allocations,
        )
        await self._set_api_power(request, allocations, remaining_power)

    async def _set_api_power(  # pylint: disable=too-many-locals
        self, request: Request, allocations: dict[int, Power], remaining_power: Power
    ) -> None:
        api_client = connection_manager.get().api_client
        tasks: dict[int, asyncio.Task[None]] = {}
        for component_id, power in allocations.items():
            tasks[component_id] = asyncio.create_task(
                api_client.set_power(component_id, power.as_watts())
            )
        _, pending = await asyncio.wait(
            tasks.values(),
            timeout=self._api_power_request_timeout.total_seconds(),
            return_when=asyncio.ALL_COMPLETED,
        )
        # collect the timed out tasks and cancel them while keeping the
        # exceptions, so that they can be processed later.
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        failed_components: set[int] = set()
        succeeded_components: set[int] = set()
        failed_power = Power.zero()
        for component_id, task in tasks.items():
            try:
                task.result()
            except asyncio.CancelledError:
                _logger.warning(
                    "Timeout while setting power to PV inverter %s", component_id
                )
            except ApiClientError as exc:
                _logger.warning(
                    "Got a client error while setting power to PV inverter %s: %s",
                    component_id,
                    exc,
                )
            except Exception:  # pylint: disable=broad-except
                _logger.exception(
                    "Unknown error while setting power to PV inverter: %s",
                    component_id,
                )
            else:
                succeeded_components.add(component_id)
                continue

            failed_components.add(component_id)
            failed_power += allocations[component_id]

        if failed_components:
            await self._results_sender.send(
                PartialFailure(
                    failed_components=failed_components,
                    succeeded_components=succeeded_components,
                    failed_power=failed_power,
                    succeeded_power=self._target_power - failed_power,
                    excess_power=remaining_power,
                    request=request,
                )
            )
            return
        await self._results_sender.send(
            Success(
                succeeded_components=succeeded_components,
                succeeded_power=self._target_power,
                excess_power=remaining_power,
                request=request,
            )
        )

    def _get_pv_inverter_ids(self) -> collections.abc.Set[int]:
        """Return the IDs of all PV inverters present in the component graph."""
        return {
            inv.component_id
            for inv in connection_manager.get().component_graph.components(
                component_categories={ComponentCategory.INVERTER}
            )
            if inv.type == InverterType.SOLAR
        }
