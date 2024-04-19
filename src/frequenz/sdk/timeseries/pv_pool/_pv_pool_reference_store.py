# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Manages shared state/tasks for a set of PV inverters."""


import asyncio
import uuid
from collections import abc

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.microgrid import ComponentCategory, InverterType

from ...actor import ChannelRegistry, ComponentMetricRequest
from ...actor._power_managing._base_classes import Proposal, ReportRequest
from ...actor.power_distributing import ComponentPoolStatus
from ...microgrid import connection_manager
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
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        status_receiver: Receiver[ComponentPoolStatus],
        power_manager_requests_sender: Sender[Proposal],
        power_manager_bounds_subs_sender: Sender[ReportRequest],
        component_ids: abc.Set[int] | None = None,
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
            component_ids: An optional list of component_ids belonging to this pool.  If
                not specified, IDs of all PV inverters in the microgrid will be fetched
                from the component graph.
        """
        self.channel_registry = channel_registry
        self.resampler_subscription_sender = resampler_subscription_sender
        self.status_receiver = status_receiver
        self.power_manager_requests_sender = power_manager_requests_sender
        self.power_manager_bounds_subs_sender = power_manager_bounds_subs_sender

        if component_ids is not None:
            self.component_ids: frozenset[int] = frozenset(component_ids)
        else:
            graph = connection_manager.get().component_graph
            self.component_ids = frozenset(
                {
                    inv.component_id
                    for inv in graph.components(
                        component_categories={ComponentCategory.INVERTER}
                    )
                    if inv.type == InverterType.SOLAR
                }
            )

        self.power_bounds_subs: dict[str, asyncio.Task[None]] = {}

        self.namespace: str = f"pv-pool-{uuid.uuid4()}"
        self.formula_pool = FormulaEnginePool(
            self.namespace,
            self.channel_registry,
            self.resampler_subscription_sender,
        )
        self.bounds_channel: Broadcast[SystemBounds] = Broadcast(
            name=f"System Bounds for PV inverters: {component_ids}"
        )
        self.bounds_tracker: PVSystemBoundsTracker = PVSystemBoundsTracker(
            self.component_ids,
            self.status_receiver,
            self.bounds_channel.new_sender(),
        )
        self.bounds_tracker.start()

    async def stop(self) -> None:
        """Stop all tasks and channels owned by the PVInverterPool."""
        await self.formula_pool.stop()
        await self.bounds_tracker.stop()
