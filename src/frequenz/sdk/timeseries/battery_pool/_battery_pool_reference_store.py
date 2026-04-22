# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""User interface for requesting aggregated battery-inverter data."""

import asyncio
import uuid
from collections.abc import Awaitable, Set
from datetime import timedelta
from typing import Any, Type

from frequenz.channels import Receiver, Sender
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import Battery, Component
from typing_extensions import override

from ..._internal._asyncio import cancel_and_await
from ..._internal._channels import ChannelRegistry, ReceiverFetcher
from ...microgrid._data_sourcing import ComponentMetricRequest
from ...microgrid._power_distributing import Result
from ...microgrid._power_distributing._component_status import ComponentPoolStatus
from ...microgrid._power_managing._base_classes import Proposal, ReportRequest
from ..abstract_pool import AbstractPoolReferenceStore
from ._methods import MetricAggregator


class BatteryPoolReferenceStore(AbstractPoolReferenceStore):
    """A class for maintaining the shared state/tasks for a set of pool of batteries.

    This includes ownership of
    - the formula pool and metric calculators.
    - the tasks for updating the battery status for the metric calculators.

    These are independent of the priority of the actors and can be shared between
    multiple users of the same set of batteries.

    They are exposed through the BatteryPool class.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        batteries_status_receiver: Receiver[ComponentPoolStatus],
        power_manager_requests_sender: Sender[Proposal],
        power_manager_bounds_subscription_sender: Sender[ReportRequest],
        power_distribution_results_fetcher: ReceiverFetcher[Result],
        min_update_interval: timedelta,
        batteries_id: Set[ComponentId] | None = None,
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
            power_distribution_results_fetcher: A ReceiverFetcher for the results from
                the power distributing actor.
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
        super().__init__(
            channel_registry=channel_registry,
            resampler_subscription_sender=resampler_subscription_sender,
            status_receiver=batteries_status_receiver,
            power_manager_requests_sender=power_manager_requests_sender,
            power_manager_bounds_subs_sender=power_manager_bounds_subscription_sender,
            power_distribution_results_fetcher=power_distribution_results_fetcher,
            component_ids=batteries_id,
        )

        self._batteries = self.component_ids
        self._working_batteries: set[ComponentId] = set()
        self._update_battery_status_task: asyncio.Task[None] | None = None
        self._batteries_status_receiver: Receiver[ComponentPoolStatus] = (
            batteries_status_receiver
        )
        if self._batteries:
            self._update_battery_status_task = asyncio.create_task(
                self._update_battery_status(self._batteries_status_receiver)
            )
        self._min_update_interval: timedelta = min_update_interval
        self._active_methods: dict[str, MetricAggregator[Any]] = {}
        self._power_distributing_namespace: str = f"power-distributor-{self.namespace}"

    @staticmethod
    def get_component_class() -> Type[Component]:
        """Class of the component type."""
        return Battery

    @staticmethod
    def get_pool_type_name() -> str:
        """Name of the pool type, for display purposes."""
        return "BatteryPool"

    @staticmethod
    def get_component_type_name_plural() -> str:
        """Name of the component type, for display purposes."""
        return "batteries"

    @override
    def get_namespace(self) -> str:
        """Namespace to use with the data pipeline."""
        return f"battery-pool-{self.component_ids}-{uuid.uuid4()}"

    @override
    def create_bounds_tracker(self) -> None:
        """Create the bounds tracker for the pool."""

    async def stop(self) -> None:
        """Stop all pending async tasks."""
        tasks_to_stop: list[Awaitable[Any]] = [
            method.stop() for method in self._active_methods.values()
        ]
        tasks_to_stop.append(self.formula_pool.stop())
        if self._update_battery_status_task:
            tasks_to_stop.append(cancel_and_await(self._update_battery_status_task))
        await asyncio.gather(*tasks_to_stop)
        self._batteries_status_receiver.close()

    async def _update_battery_status(
        self, receiver: Receiver[ComponentPoolStatus]
    ) -> None:
        async for status in receiver:
            self._working_batteries = status.get_working_components(self._batteries)
            for item in self._active_methods.values():
                item.update_working_batteries(self._working_batteries)
