# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Manages shared state/tasks for a set of PV inverters."""


import asyncio
import uuid
from collections import abc

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import SolarInverter

from ..._internal._channels import ChannelRegistry, ReceiverFetcher
from ...microgrid import connection_manager
from ...microgrid._data_sourcing import ComponentMetricRequest
from ...microgrid._power_distributing import ComponentPoolStatus, Result
from ...microgrid._power_managing._base_classes import Proposal, ReportRequest
from .._base_types import SystemBounds
from ..formula_engine._formula_engine_pool import FormulaEnginePool
from ._system_bounds_tracker import PVSystemBoundsTracker


class PVPoolReferenceStore:
    """A class for maintaining the shared state/tasks for a set of pool of PV inverters.

    This includes ownership of
    - the formula engine pool and metric calculators.
    - the tasks for calculating system bounds for the PV inverters.

    These are independent of the priority of the actors and can be shared between
    multiple users of the same set of PV inverters.

    They are exposed through the PVPool class.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        status_receiver: Receiver[ComponentPoolStatus],
        power_manager_requests_sender: Sender[Proposal],
        power_manager_bounds_subs_sender: Sender[ReportRequest],
        power_distribution_results_fetcher: ReceiverFetcher[Result],
        component_ids: abc.Set[ComponentId] | None = None,
    ):
        """Initialize this instance.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
            status_receiver: A receiver that streams the status of the PV inverters in
                the pool.
            power_manager_requests_sender: A Channel sender for sending power
                requests to the power managing actor.
            power_manager_bounds_subs_sender: A Channel sender for sending power bounds
                subscription requests to the power managing actor.
            power_distribution_results_fetcher: A ReceiverFetcher for the results from
                the power distributing actor.
            component_ids: An optional list of component_ids belonging to this pool.  If
                not specified, IDs of all PV inverters in the microgrid will be fetched
                from the component graph.
        """
        self.channel_registry = channel_registry
        self.resampler_subscription_sender = resampler_subscription_sender
        self.status_receiver = status_receiver
        self.power_manager_requests_sender = power_manager_requests_sender
        self.power_manager_bounds_subs_sender = power_manager_bounds_subs_sender
        self.power_distribution_results_fetcher = power_distribution_results_fetcher

        if component_ids is not None:
            self.component_ids: frozenset[ComponentId] = frozenset(component_ids)
        else:
            graph = connection_manager.get().component_graph
            self.component_ids = frozenset(
                {inv.id for inv in graph.components(matching_types=SolarInverter)}
            )

        self.power_bounds_subs: dict[str, asyncio.Task[None]] = {}

        self.namespace: str = f"pv-pool-{uuid.uuid4()}"
        self.formula_pool = FormulaEnginePool(
            self.namespace,
            self.channel_registry,
            self.resampler_subscription_sender,
        )
        self.bounds_channel: Broadcast[SystemBounds] = Broadcast(
            name=f"System Bounds for PV inverters: {component_ids}",
            resend_latest=True,
        )

        self.bounds_tracker: PVSystemBoundsTracker | None = None
        # In locations without PV inverters, the bounds tracker will not be started.
        if self.component_ids:
            self.bounds_tracker = PVSystemBoundsTracker(
                self.component_ids,
                self.status_receiver,
                self.bounds_channel.new_sender(),
            )
            self.bounds_tracker.start()

    async def stop(self) -> None:
        """Stop all tasks and channels owned by the PVInverterPool."""
        await self.formula_pool.stop()
        if self.bounds_tracker is not None:
            await self.bounds_tracker.stop()
        self.status_receiver.close()
