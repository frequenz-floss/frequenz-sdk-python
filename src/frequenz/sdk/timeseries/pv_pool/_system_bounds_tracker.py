# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""System bounds tracker for PV inverters."""

import asyncio
from collections import abc

from frequenz.channels import Receiver, Sender, merge, select, selected_from
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power

from ..._internal._asyncio import run_forever
from ...actor import BackgroundService
from ...microgrid import connection_manager
from ...microgrid._old_component_data import InverterData
from ...microgrid._power_distributing._component_status import ComponentPoolStatus
from ...microgrid._power_distributing._unreachable_power import (
    UnreachablePowerTracker,
)
from .._base_types import Sample, SystemBounds
from ..component_pool._system_bounds import aggregate_system_bounds


class PVSystemBoundsTracker(BackgroundService):
    """Track the system bounds for PV inverters.

    System bounds are the aggregate bounds for the PV inverters in the pool that are in
    a working state.  They are calculated from the individual bounds received from the
    microgrid API.

    The inclusion bounds are shifted by the power measured on the PV inverters that
    aren't working, because the power distributor subtracts it from the target power.

    The system bounds are sent to the `bounds_sender` whenever they change.
    """

    def __init__(
        self,
        component_ids: abc.Set[ComponentId],
        status_receiver: Receiver[ComponentPoolStatus],
        bounds_sender: Sender[SystemBounds],
    ):
        """Initialize the system bounds tracker.

        Args:
            component_ids: The ids of the components to track.
            status_receiver: A receiver that streams the status of the PV inverters in
                the pool.
            bounds_sender: A sender to send the system bounds to.
        """
        super().__init__()

        self._component_ids = component_ids
        self._status_receiver = status_receiver
        self._bounds_sender = bounds_sender
        self._latest_component_data: dict[ComponentId, InverterData] = {}
        self._last_sent_bounds: SystemBounds | None = None
        self._component_pool_status = ComponentPoolStatus(set(), set())
        self._unreachable_power = UnreachablePowerTracker(
            component_ids, connection_manager.get().component_graph.pv_formula
        )

    def start(self) -> None:
        """Start the PV inverter system bounds tracker."""
        self._tasks.add(asyncio.create_task(run_forever(self._run)))

    async def _send_bounds(self) -> None:
        """Calculate and send the aggregate system bounds if they have changed."""
        bounds = aggregate_system_bounds(
            self._latest_component_data.values(), self._unreachable_power.power
        )
        if bounds is None:
            return
        if (
            self._last_sent_bounds is None
            or self._last_sent_bounds.inclusion_bounds != bounds.inclusion_bounds
            or self._last_sent_bounds.exclusion_bounds != bounds.exclusion_bounds
        ):
            self._last_sent_bounds = bounds
            await self._bounds_sender.send(bounds)

    async def _run(self) -> None:
        """Run the system bounds tracker."""
        api_client = connection_manager.get().api_client
        pv_data_rx = merge(
            *(
                InverterData.subscribe(api_client, component_id)
                for component_id in self._component_ids
            )
        )

        unreachable_power_rx = self._unreachable_power.new_receiver()
        try:
            await self._track(pv_data_rx, unreachable_power_rx)
        finally:
            await self._unreachable_power.stop()

    async def _track(
        self,
        pv_data_rx: Receiver[InverterData],
        unreachable_power_rx: Receiver[Sample[Power]],
    ) -> None:
        status_rx = self._status_receiver
        async for selected in select(status_rx, pv_data_rx, unreachable_power_rx):
            if selected_from(selected, status_rx):
                self._component_pool_status = selected.message
                await self._unreachable_power.update(
                    self._component_pool_status.working
                    | self._component_pool_status.uncertain
                )
                to_remove: list[ComponentId] = []
                for comp_id in self._latest_component_data:
                    if (
                        comp_id not in self._component_pool_status.working
                        and comp_id not in self._component_pool_status.uncertain
                    ):
                        to_remove.append(comp_id)
                for comp_id in to_remove:
                    del self._latest_component_data[comp_id]
            elif selected_from(selected, pv_data_rx):
                data = selected.message
                comp_id = data.component_id
                if (
                    comp_id not in self._component_pool_status.working
                    and comp_id not in self._component_pool_status.uncertain
                ):
                    continue
                self._latest_component_data[data.component_id] = data
            elif selected_from(selected, unreachable_power_rx):
                pass  # Only the bounds need to be recalculated.

            await self._send_bounds()
