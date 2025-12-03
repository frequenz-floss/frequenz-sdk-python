# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""The power manager."""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import assert_never

from frequenz.channels import Receiver, Sender, select, selected_from
from frequenz.channels.timer import SkipMissedAndDrift, Timer
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import Battery, EvCharger, SolarInverter
from typing_extensions import override

from ..._internal._asyncio import run_forever
from ..._internal._channels import ChannelRegistry
from ...actor import Actor
from ...timeseries._base_types import SystemBounds
from .. import _data_pipeline, _power_distributing
from ._base_classes import (
    Algorithm,
    BaseAlgorithm,
    DefaultPower,
    Proposal,
    ReportRequest,
    _Report,
)
from ._matryoshka import Matryoshka
from ._shifting_matryoshka import ShiftingMatryoshka

_logger = logging.getLogger(__name__)


class PowerManagingActor(Actor):
    """The power manager."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        proposals_receiver: Receiver[Proposal],
        bounds_subscription_receiver: Receiver[ReportRequest],
        power_distributing_requests_sender: Sender[_power_distributing.Request],
        power_distributing_results_receiver: Receiver[_power_distributing.Result],
        channel_registry: ChannelRegistry,
        algorithm: Algorithm,
        default_power: DefaultPower,
        component_class: type[Battery | EvCharger | SolarInverter],
    ):
        """Create a new instance of the power manager.

        Args:
            proposals_receiver: The receiver for proposals.
            bounds_subscription_receiver: The receiver for bounds subscriptions.
            power_distributing_requests_sender: The sender for power distribution
                requests.
            power_distributing_results_receiver: The receiver for power distribution
                results.
            channel_registry: The channel registry.
            algorithm: The power management algorithm to use.
            default_power: The default power to use for the components.
            component_class: The class of component this instance is going to support.
        """
        self._default_power = default_power
        self._component_class = component_class
        self._bounds_subscription_receiver = bounds_subscription_receiver
        self._power_distributing_requests_sender = power_distributing_requests_sender
        self._power_distributing_results_receiver = power_distributing_results_receiver
        self._channel_registry = channel_registry
        self._proposals_receiver = proposals_receiver

        self._system_bounds: dict[frozenset[ComponentId], SystemBounds] = {}
        self._bound_tracker_tasks: dict[frozenset[ComponentId], asyncio.Task[None]] = {}
        self._subscriptions: dict[
            frozenset[ComponentId], dict[int, Sender[_Report]]
        ] = {}

        match algorithm:
            case Algorithm.MATRYOSHKA:
                self._algorithm: BaseAlgorithm = Matryoshka(
                    max_proposal_age=timedelta(seconds=60.0),
                    default_power=default_power,
                )
            case Algorithm.SHIFTING_MATRYOSHKA:
                self._algorithm = ShiftingMatryoshka(
                    max_proposal_age=timedelta(seconds=60.0),
                    default_power=default_power,
                )
            case _:
                assert_never(algorithm)

        super().__init__()

    async def _send_reports(self, component_ids: frozenset[ComponentId]) -> None:
        """Send reports for a set of components, to all subscribers.

        Args:
            component_ids: The component IDs for which a collective report should be
                sent.
        """
        bounds = self._system_bounds.get(component_ids)
        if bounds is None:
            _logger.warning("PowerManagingActor: No bounds for %s", component_ids)
            return
        for priority, sender in self._subscriptions.get(component_ids, {}).items():
            status = self._algorithm.get_status(
                component_ids,
                priority,
                bounds,
            )
            await sender.send(status)

    async def _bounds_tracker(
        self,
        component_ids: frozenset[ComponentId],
        bounds_receiver: Receiver[SystemBounds],
    ) -> None:
        """Track the power bounds of a set of components and update the cache.

        Args:
            component_ids: The component IDs for which this task should track the
                collective bounds of.
            bounds_receiver: The receiver for power bounds.
        """
        last_bounds: SystemBounds | None = None
        async for bounds in bounds_receiver:
            if (
                last_bounds is not None
                and bounds.inclusion_bounds == last_bounds.inclusion_bounds
            ):
                continue
            last_bounds = bounds
            self._system_bounds[component_ids] = bounds
            await self._send_updated_target_power(component_ids, None)
            await self._send_reports(component_ids)

    def _add_system_bounds_tracker(self, component_ids: frozenset[ComponentId]) -> None:
        """Add a system bounds tracker for the given components.

        Args:
            component_ids: The component IDs for which to add a bounds tracker.
        """
        bounds_receiver: Receiver[SystemBounds]
        if issubclass(self._component_class, Battery):
            battery_pool = _data_pipeline.new_battery_pool(
                priority=-sys.maxsize - 1, component_ids=component_ids
            )
            # pylint: disable-next=protected-access
            bounds_receiver = battery_pool._system_power_bounds.new_receiver()
        elif issubclass(self._component_class, EvCharger):
            ev_charger_pool = _data_pipeline.new_ev_charger_pool(
                priority=-sys.maxsize - 1, component_ids=component_ids
            )
            # pylint: disable-next=protected-access
            bounds_receiver = ev_charger_pool._system_power_bounds.new_receiver()
        elif issubclass(self._component_class, SolarInverter):
            pv_pool = _data_pipeline.new_pv_pool(
                priority=-sys.maxsize - 1, component_ids=component_ids
            )
            # pylint: disable-next=protected-access
            bounds_receiver = pv_pool._system_power_bounds.new_receiver()
        else:
            _logger.error(
                "PowerManagingActor: Unsupported component class: %s",
                self._component_class.__name__,
            )
            assert_never(self._component_class)

        self._system_bounds[component_ids] = SystemBounds(
            timestamp=datetime.now(tz=timezone.utc),
            inclusion_bounds=None,
            exclusion_bounds=None,
        )

        # Start the bounds tracker, for ongoing updates.
        self._bound_tracker_tasks[component_ids] = asyncio.create_task(
            run_forever(lambda: self._bounds_tracker(component_ids, bounds_receiver))
        )

    async def _send_updated_target_power(
        self,
        component_ids: frozenset[ComponentId],
        proposal: Proposal | None,
    ) -> None:
        target_power = self._algorithm.calculate_target_power(
            component_ids,
            proposal,
            self._system_bounds[component_ids],
        )
        if target_power is not None:
            await self._power_distributing_requests_sender.send(
                _power_distributing.Request(
                    power=target_power,
                    component_ids=component_ids,
                    adjust_power=True,
                )
            )

    @override
    async def _run(self) -> None:
        """Run the power managing actor."""
        last_result_partial_failure = False
        drop_old_proposals_timer = Timer(timedelta(seconds=1.0), SkipMissedAndDrift())
        async for selected in select(
            self._proposals_receiver,
            self._bounds_subscription_receiver,
            self._power_distributing_results_receiver,
            drop_old_proposals_timer,
        ):
            if selected_from(selected, self._proposals_receiver):
                proposal = selected.message
                if proposal.component_ids not in self._bound_tracker_tasks:
                    self._add_system_bounds_tracker(proposal.component_ids)

                # TODO: must_send=True forces a new request to # pylint: disable=fixme
                # be sent to the PowerDistributor, even if there's no change in power.
                #
                # This is needed because requests would expire in the microgrid service
                # otherwise.
                #
                # This can be removed as soon as
                # https://github.com/frequenz-floss/frequenz-sdk-python/issues/293 is
                # implemented.
                await self._send_updated_target_power(proposal.component_ids, proposal)
                await self._send_reports(proposal.component_ids)

            elif selected_from(selected, self._bounds_subscription_receiver):
                sub = selected.message
                component_ids = sub.component_ids
                priority = sub.priority

                if component_ids not in self._subscriptions:
                    self._subscriptions[component_ids] = {
                        priority: self._channel_registry.get_or_create(
                            _Report, sub.get_channel_name()
                        ).new_sender()
                    }
                elif priority not in self._subscriptions[component_ids]:
                    self._subscriptions[component_ids][priority] = (
                        self._channel_registry.get_or_create(
                            _Report, sub.get_channel_name()
                        ).new_sender()
                    )

                if sub.component_ids not in self._bound_tracker_tasks:
                    self._add_system_bounds_tracker(sub.component_ids)

            elif selected_from(selected, self._power_distributing_results_receiver):
                result = selected.message
                if not isinstance(result, _power_distributing.Success):
                    _logger.warning(
                        "PowerManagingActor: PowerDistributing failed: %s", result
                    )
                match result:
                    case _power_distributing.PartialFailure(request):
                        if not last_result_partial_failure:
                            last_result_partial_failure = True
                            await self._send_updated_target_power(
                                frozenset(request.component_ids), None
                            )
                    case _power_distributing.Success():
                        last_result_partial_failure = False
                await self._send_reports(frozenset(result.request.component_ids))

            elif selected_from(selected, drop_old_proposals_timer):
                self._algorithm.drop_old_proposals(asyncio.get_event_loop().time())
