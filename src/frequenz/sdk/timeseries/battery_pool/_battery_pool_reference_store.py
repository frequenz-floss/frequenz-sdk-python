# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""User interface for requesting aggregated battery-inverter data."""


import asyncio
import uuid
from collections.abc import Awaitable, Set
from datetime import timedelta
from typing import Any

from frequenz.channels import Receiver, Sender
from frequenz.client.microgrid import ComponentCategory

from ..._internal._asyncio import cancel_and_await
from ...actor._channel_registry import ChannelRegistry
from ...actor._data_sourcing._component_metric_request import ComponentMetricRequest
from ...actor._power_managing._base_classes import Proposal, ReportRequest
from ...actor.power_distributing._component_status import ComponentPoolStatus
from ...microgrid import connection_manager
from ..formula_engine._formula_engine_pool import FormulaEnginePool
from ._methods import MetricAggregator


class BatteryPoolReferenceStore:  # pylint: disable=too-many-instance-attributes
    """A class for maintaining the shared state/tasks for a set of pool of batteries.

    This includes ownership of
    - the formula engine pool and metric calculators.
    - the tasks for updating the battery status for the metric calculators.

    These are independent of the priority of the actors and can be shared between
    multiple users of the same set of batteries.

    They are exposed through the BatteryPool class.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        batteries_status_receiver: Receiver[ComponentPoolStatus],
        power_manager_requests_sender: Sender[Proposal],
        power_manager_bounds_subscription_sender: Sender[ReportRequest],
        min_update_interval: timedelta,
        batteries_id: Set[int] | None = None,
    ) -> None:
        """Create the class instance.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
            batteries_status_receiver: Receiver to receive status of the batteries.
                Receivers should has maxsize = 1 to fetch only the latest status.
                Battery status channel should has resend_latest = True.
                It should send information when any battery changed status.
                Battery status should include status of the inverter adjacent to this
                battery.
            power_manager_requests_sender: A Channel sender for sending power
                requests to the power managing actor.
            power_manager_bounds_subscription_sender: A Channel sender for sending
                power bounds requests to the power managing actor.
            min_update_interval: Some metrics in BatteryPool are send only when they
                change. For these metrics min_update_interval is the minimum time
                interval between the following messages.
                Note that this argument is similar to the resampling period
                argument in the ComponentMetricsResamplingActor. But as opposed to
                ResamplingActor, timestamp returned in the resulting message will be
                the timestamp of the last received component data.
                It is currently impossible to use resampling actor for these metrics,
                because we can't specify resampling function for them.
            batteries_id: Subset of the batteries that should be included in the
                battery pool. If None or empty, then all batteries from the microgrid
                will be used.
        """
        self._batteries: frozenset[int]
        if batteries_id:
            self._batteries = frozenset(batteries_id)
        else:
            self._batteries = self._get_all_batteries()

        self._working_batteries: set[int] = set()

        self._update_battery_status_task: asyncio.Task[None] | None = None
        if self._batteries:
            self._update_battery_status_task = asyncio.create_task(
                self._update_battery_status(batteries_status_receiver)
            )

        self._min_update_interval: timedelta = min_update_interval

        self._power_manager_requests_sender: Sender[Proposal] = (
            power_manager_requests_sender
        )

        self._power_manager_bounds_subscription_sender: Sender[ReportRequest] = (
            power_manager_bounds_subscription_sender
        )

        self._active_methods: dict[str, MetricAggregator[Any]] = {}
        self._power_bounds_subs: dict[str, asyncio.Task[None]] = {}
        self._namespace: str = f"battery-pool-{self._batteries}-{uuid.uuid4()}"
        self._power_distributing_namespace: str = f"power-distributor-{self._namespace}"
        self._channel_registry: ChannelRegistry = channel_registry
        self._formula_pool: FormulaEnginePool = FormulaEnginePool(
            self._namespace,
            self._channel_registry,
            resampler_subscription_sender,
        )

    async def stop(self) -> None:
        """Stop all pending async tasks."""
        tasks_to_stop: list[Awaitable[Any]] = [
            method.stop() for method in self._active_methods.values()
        ]
        tasks_to_stop.append(self._formula_pool.stop())
        if self._update_battery_status_task:
            tasks_to_stop.append(cancel_and_await(self._update_battery_status_task))
        await asyncio.gather(*tasks_to_stop)

    def _get_all_batteries(self) -> frozenset[int]:
        """Get all batteries from the microgrid.

        Returns:
            All batteries in the microgrid.
        """
        graph = connection_manager.get().component_graph
        return frozenset(
            {
                battery.component_id
                for battery in graph.components(
                    component_categories={ComponentCategory.BATTERY}
                )
            }
        )

    async def _update_battery_status(
        self, receiver: Receiver[ComponentPoolStatus]
    ) -> None:
        async for status in receiver:
            self._working_batteries = status.get_working_components(self._batteries)
            for item in self._active_methods.values():
                item.update_working_batteries(self._working_batteries)
