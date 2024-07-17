# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A task for sending EV Charger power bounds to the microgrid API."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta

from frequenz.channels import Broadcast, LatestValueCache, Sender, select, selected_from
from frequenz.channels.timer import SkipMissedAndDrift, Timer
from frequenz.client.microgrid import ComponentCategory, MeterData

from ..._internal._asyncio import cancel_and_await
from ...microgrid import connection_manager

_logger = logging.getLogger(__name__)


@dataclass
class ComponentCurrentLimit:
    """A current limit, to be sent to the EV Charger."""

    component_id: int
    """The component ID of the EV Charger."""

    max_amps: float
    """The maximum current in amps, that an EV can draw from this EV Charger."""


class BoundsSetter:
    """A task for sending EV Charger power bounds to the microgrid API.

    Also, periodically resends the last set bounds to the microgrid API, if no new
    bounds have been set.
    """

    _NUM_PHASES = 3
    """Number of phases in the microgrid."""

    def __init__(self, repeat_interval: timedelta) -> None:
        """Create a `BoundsSetter` instance.

        Args:
            repeat_interval: Interval after which to repeat the last set bounds to the
                microgrid API, if no new calls to `set_bounds` have been made.
        """
        self._repeat_interval = repeat_interval

        self._task: asyncio.Task[None] = asyncio.create_task(self._run())
        self._bounds_chan: Broadcast[ComponentCurrentLimit] = Broadcast(
            name="BoundsSetter"
        )
        self._bounds_rx = self._bounds_chan.new_receiver()
        self._bounds_tx = self._bounds_chan.new_sender()
        self._meter_data_cache: LatestValueCache[MeterData] | None = None

    async def set(self, component_id: int, max_amps: float) -> None:
        """Send the given current limit to the microgrid for the given component id.

        Args:
            component_id: ID of EV Charger to set the current bounds to.
            max_amps: maximum current in amps, that an EV can draw from this EV Charger.
        """
        await self._bounds_tx.send(ComponentCurrentLimit(component_id, max_amps))

    def new_bounds_sender(self) -> Sender[ComponentCurrentLimit]:
        """Return a `Sender` for setting EV Charger current bounds with.

        Returns:
            A new `Sender`.
        """
        return self._bounds_chan.new_sender()

    async def stop(self) -> None:
        """Stop the BoundsSetter."""
        if self._meter_data_cache is not None:
            await self._meter_data_cache.stop()
        await self._bounds_chan.close()
        await cancel_and_await(self._task)

    async def _run(self) -> None:
        """Wait for new bounds and forward them to the microgrid API.

        Also, periodically resend the last set bounds to the microgrid API, if no new
        bounds have been set.

        Raises:
            RuntimeError: If no meters are found in the component graph.
            ValueError: If the meter channel is closed.
        """
        api_client = connection_manager.get().api_client
        graph = connection_manager.get().component_graph
        meters = graph.components(component_categories={ComponentCategory.METER})
        if not meters:
            err = "No meters found in the component graph."
            _logger.error(err)
            raise RuntimeError(err)

        meter_id = next(iter(meters)).component_id
        self._meter_data_cache = LatestValueCache(
            await api_client.meter_data(meter_id),
            unique_id=f"{type(self).__name__}«{hex(id(self))}»:meter«{meter_id}»",
        )
        latest_bound: dict[int, ComponentCurrentLimit] = {}

        bound_chan = self._bounds_rx
        timer = Timer(
            timedelta(self._repeat_interval.total_seconds()), SkipMissedAndDrift()
        )

        async for selected in select(bound_chan, timer):
            meter = self._meter_data_cache.get()
            if meter is None:
                raise ValueError("Meter channel closed.")

            if selected_from(selected, bound_chan):
                bound: ComponentCurrentLimit = selected.message
                if (
                    bound.component_id in latest_bound
                    and latest_bound[bound.component_id] == bound
                ):
                    continue
                latest_bound[bound.component_id] = bound
                min_voltage = min(meter.voltage_per_phase)
                _logger.info("sending new bounds: %s", bound)
                await api_client.set_bounds(
                    bound.component_id,
                    0,
                    bound.max_amps * min_voltage * self._NUM_PHASES,
                )
            elif selected_from(selected, timer):
                for bound in latest_bound.values():
                    min_voltage = min(meter.voltage_per_phase)
                    _logger.debug("resending bounds: %s", bound)
                    await api_client.set_bounds(
                        bound.component_id,
                        0,
                        bound.max_amps * min_voltage * self._NUM_PHASES,
                    )
