# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""System bounds tracker for PV inverters."""

import asyncio
from collections import abc

from frequenz.channels import Receiver, Sender, merge, select, selected_from
from frequenz.client.microgrid import InverterData
from frequenz.quantities import Power

from ..._internal._asyncio import run_forever
from ...actor import BackgroundService
from ...microgrid import connection_manager
from ...microgrid._power_distributing._component_status import ComponentPoolStatus
from .._base_types import Bounds, SystemBounds


class PVSystemBoundsTracker(BackgroundService):
    """Track the system bounds for PV inverters.

    System bounds are the aggregate bounds for the PV inverters in the pool that are in
    a working state.  They are calculated from the individual bounds received from the
    microgrid API.

    The system bounds are sent to the `bounds_sender` whenever they change.
    """

    def __init__(
        self,
        component_ids: abc.Set[int],
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
        self._latest_component_data: dict[int, InverterData] = {}
        self._last_sent_bounds: SystemBounds | None = None
        self._component_pool_status = ComponentPoolStatus(set(), set())

    def start(self) -> None:
        """Start the PV inverter system bounds tracker."""
        self._tasks.add(asyncio.create_task(run_forever(self._run)))

    async def _send_bounds(self) -> None:
        """Calculate and send the aggregate system bounds if they have changed."""
        if not self._latest_component_data:
            return
        inclusion_bounds = Bounds(
            lower=Power.from_watts(
                sum(
                    data.active_power_inclusion_lower_bound
                    for data in self._latest_component_data.values()
                )
            ),
            upper=Power.from_watts(
                sum(
                    data.active_power_inclusion_upper_bound
                    for data in self._latest_component_data.values()
                )
            ),
        )
        exclusion_bounds = Bounds(
            lower=Power.from_watts(
                sum(
                    data.active_power_exclusion_lower_bound
                    for data in self._latest_component_data.values()
                )
            ),
            upper=Power.from_watts(
                sum(
                    data.active_power_exclusion_upper_bound
                    for data in self._latest_component_data.values()
                )
            ),
        )

        if (
            self._last_sent_bounds is None
            or self._last_sent_bounds.inclusion_bounds != inclusion_bounds
            or self._last_sent_bounds.exclusion_bounds != exclusion_bounds
        ):
            self._last_sent_bounds = SystemBounds(
                timestamp=max(
                    data.timestamp for data in self._latest_component_data.values()
                ),
                inclusion_bounds=inclusion_bounds,
                exclusion_bounds=exclusion_bounds,
            )
            await self._bounds_sender.send(self._last_sent_bounds)

    async def _run(self) -> None:
        """Run the system bounds tracker."""
        api_client = connection_manager.get().api_client
        status_rx = self._status_receiver
        pv_data_rx = merge(
            *(
                await asyncio.gather(
                    *(
                        api_client.inverter_data(component_id)
                        for component_id in self._component_ids
                    )
                )
            )
        )

        async for selected in select(status_rx, pv_data_rx):
            if selected_from(selected, status_rx):
                self._component_pool_status = selected.message
                to_remove = []
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

            await self._send_bounds()
