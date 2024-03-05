# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""System bounds tracker for the EV chargers."""


import asyncio
import logging
from collections import abc
from datetime import datetime

from frequenz.channels import Receiver, Sender
from frequenz.channels.util import Merge, select, selected_from

from ... import microgrid
from ...actor import BackgroundService
from ...actor.power_distributing._component_status import ComponentPoolStatus
from .. import Power
from .._base_types import Bounds, SystemBounds

_logger = logging.getLogger(__name__)


class EVCSystemBoundsTracker(BackgroundService):
    """Track the system bounds for the EV chargers."""

    def __init__(
        self,
        component_ids: abc.Set[int],
        status_receiver: Receiver[ComponentPoolStatus],
        bounds_sender: Sender[SystemBounds],
    ):
        """Initialize the system bounds tracker.

        Args:
            component_ids: The ids of the components to track.
            status_receiver: A receiver that streams the status of the EV Chargers in
                the pool.
            bounds_sender: A sender to send the system bounds to.
        """
        super().__init__()

        self._component_ids = component_ids
        self._status_receiver = status_receiver
        self._bounds_sender = bounds_sender
        self._latest_component_data: dict[int, microgrid.component.EVChargerData] = {}
        self._last_sent_bounds: SystemBounds | None = None
        self._component_pool_status = ComponentPoolStatus(set(), set())

    def start(self) -> None:
        """Start the EV charger system bounds tracker."""
        self._tasks.add(asyncio.create_task(self._run()))

    async def _send_bounds(self) -> None:
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
            or inclusion_bounds != self._last_sent_bounds.inclusion_bounds
            or exclusion_bounds != self._last_sent_bounds.exclusion_bounds
        ):
            self._last_sent_bounds = SystemBounds(
                timestamp=max(
                    msg.timestamp for msg in self._latest_component_data.values()
                ),
                inclusion_bounds=inclusion_bounds,
                exclusion_bounds=exclusion_bounds,
            )
            await self._bounds_sender.send(self._last_sent_bounds)

    async def _run(self) -> None:
        """Run the system bounds tracker."""
        api_client = microgrid.connection_manager.get().api_client
        status_rx = self._status_receiver
        ev_data_rx = Merge(
            *(
                await asyncio.gather(
                    *[api_client.ev_charger_data(cid) for cid in self._component_ids]
                )
            )
        )

        try:
            async for selected in select(status_rx, ev_data_rx):
                if selected_from(selected, status_rx):
                    self._component_pool_status = selected.value
                    to_pop = []
                    for comp_id in self._latest_component_data:
                        if (
                            comp_id not in self._component_pool_status.working
                            and comp_id not in self._component_pool_status.uncertain
                        ):
                            to_pop.append(comp_id)
                    for comp_id in to_pop:
                        self._latest_component_data.pop(comp_id, None)
                elif selected_from(selected, ev_data_rx):
                    data = selected.value
                    comp_id = data.component_id
                    if (
                        comp_id not in self._component_pool_status.working
                        and comp_id not in self._component_pool_status.uncertain
                    ):
                        continue
                    self._latest_component_data[data.component_id] = data

                await self._send_bounds()
        except:
            _logger.exception("bounds tracker failed")
