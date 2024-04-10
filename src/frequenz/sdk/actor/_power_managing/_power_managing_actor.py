# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""The power manager."""

from __future__ import annotations

import asyncio
import logging
import typing
from datetime import datetime, timedelta, timezone

from frequenz.channels import Receiver, Sender, select, selected_from
from frequenz.channels.timer import SkipMissedAndDrift, Timer
from frequenz.client.microgrid import ComponentCategory, ComponentType, InverterType
from typing_extensions import override

from ...timeseries._base_types import SystemBounds
from .._actor import Actor
from .._channel_registry import ChannelRegistry
from ._base_classes import Algorithm, BaseAlgorithm, Proposal, ReportRequest, _Report
from ._matryoshka import Matryoshka

_logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from .. import power_distributing


class PowerManagingActor(Actor):
    """The power manager."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        proposals_receiver: Receiver[Proposal],
        bounds_subscription_receiver: Receiver[ReportRequest],
        power_distributing_requests_sender: Sender[power_distributing.Request],
        power_distributing_results_receiver: Receiver[power_distributing.Result],
        channel_registry: ChannelRegistry,
        *,
        component_category: ComponentCategory,
        component_type: ComponentType | None = None,
        # arguments to actors need to serializable, so we pass an enum for the algorithm
        # instead of an instance of the algorithm.
        algorithm: Algorithm = Algorithm.MATRYOSHKA,
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
            component_category: The category of the component this power manager
                instance is going to support.
            component_type: The type of the component of the given category that this
                actor is responsible for.  This is used only when the component category
                is not enough to uniquely identify the component.  For example, when the
                category is `ComponentCategory.INVERTER`, the type is needed to identify
                the inverter as a solar inverter or a battery inverter.  This can be
                `None` when the component category is enough to uniquely identify the
                component.
            algorithm: The power management algorithm to use.

        Raises:
            NotImplementedError: When an unknown algorithm is given.
        """
        if algorithm is not Algorithm.MATRYOSHKA:
            raise NotImplementedError(
                f"PowerManagingActor: Unknown algorithm: {algorithm}"
            )

        self._component_category = component_category
        self._component_type = component_type
        self._bounds_subscription_receiver = bounds_subscription_receiver
        self._power_distributing_requests_sender = power_distributing_requests_sender
        self._power_distributing_results_receiver = power_distributing_results_receiver
        self._channel_registry = channel_registry
        self._proposals_receiver = proposals_receiver

        self._system_bounds: dict[frozenset[int], SystemBounds] = {}
        self._bound_tracker_tasks: dict[frozenset[int], asyncio.Task[None]] = {}
        self._subscriptions: dict[frozenset[int], dict[int, Sender[_Report]]] = {}
        self._distribution_results: dict[frozenset[int], power_distributing.Result] = {}

        self._algorithm: BaseAlgorithm = Matryoshka(
            max_proposal_age=timedelta(seconds=60.0)
        )

        super().__init__()

    async def _send_reports(self, component_ids: frozenset[int]) -> None:
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
                self._distribution_results.get(component_ids),
            )
            await sender.send(status)

    async def _bounds_tracker(
        self,
        component_ids: frozenset[int],
        bounds_receiver: Receiver[SystemBounds],
    ) -> None:
        """Track the power bounds of a set of components and update the cache.

        Args:
            component_ids: The component IDs for which this task should track the
                collective bounds of.
            bounds_receiver: The receiver for power bounds.
        """
        async for bounds in bounds_receiver:
            self._system_bounds[component_ids] = bounds
            await self._send_updated_target_power(component_ids, None)
            await self._send_reports(component_ids)

    def _add_bounds_tracker(self, component_ids: frozenset[int]) -> None:
        """Add a bounds tracker.

        Args:
            component_ids: The component IDs for which to add a bounds tracker.

        Raises:
            NotImplementedError: When the pool type is not supported.
        """
        # Pylint assumes that this import is cyclic, but it's not.
        from ... import (  # pylint: disable=import-outside-toplevel,cyclic-import
            microgrid,
        )

        bounds_receiver: Receiver[SystemBounds]
        # pylint: disable=protected-access
        if self._component_category is ComponentCategory.BATTERY:
            battery_pool = microgrid.battery_pool(component_ids)
            bounds_receiver = battery_pool._system_power_bounds.new_receiver()
        elif self._component_category is ComponentCategory.EV_CHARGER:
            ev_charger_pool = microgrid.ev_charger_pool(component_ids)
            bounds_receiver = ev_charger_pool._system_power_bounds.new_receiver()
        elif (
            self._component_category is ComponentCategory.INVERTER
            and self._component_type is InverterType.SOLAR
        ):
            pv_pool = microgrid.pv_pool(component_ids)
            bounds_receiver = pv_pool._system_power_bounds.new_receiver()
        # pylint: enable=protected-access
        else:
            err = (
                "PowerManagingActor: Unsupported component category: "
                f"{self._component_category}"
            )
            _logger.error(err)
            raise NotImplementedError(err)

        self._system_bounds[component_ids] = SystemBounds(
            timestamp=datetime.now(tz=timezone.utc),
            inclusion_bounds=None,
            exclusion_bounds=None,
        )

        # Start the bounds tracker, for ongoing updates.
        self._bound_tracker_tasks[component_ids] = asyncio.create_task(
            self._bounds_tracker(component_ids, bounds_receiver)
        )

    async def _send_updated_target_power(
        self,
        component_ids: frozenset[int],
        proposal: Proposal | None,
        must_send: bool = False,
    ) -> None:
        from .. import power_distributing  # pylint: disable=import-outside-toplevel

        target_power = self._algorithm.calculate_target_power(
            component_ids,
            proposal,
            self._system_bounds[component_ids],
            must_send,
        )
        request_timeout = (
            proposal.request_timeout if proposal else timedelta(seconds=5.0)
        )
        if target_power is not None:
            await self._power_distributing_requests_sender.send(
                power_distributing.Request(
                    power=target_power,
                    component_ids=component_ids,
                    request_timeout=request_timeout,
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
                    self._add_bounds_tracker(proposal.component_ids)

                # TODO: must_send=True forces a new request to # pylint: disable=fixme
                # be sent to the PowerDistributor, even if there's no change in power.
                #
                # This is needed because requests would expire in the microgrid service
                # otherwise.
                #
                # This can be removed as soon as
                # https://github.com/frequenz-floss/frequenz-sdk-python/issues/293 is
                # implemented.
                await self._send_updated_target_power(
                    proposal.component_ids, proposal, must_send=True
                )
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
                    self._add_bounds_tracker(sub.component_ids)

            elif selected_from(selected, self._power_distributing_results_receiver):
                from .. import (  # pylint: disable=import-outside-toplevel
                    power_distributing,
                )

                result = selected.message
                self._distribution_results[frozenset(result.request.component_ids)] = (
                    result
                )
                if not isinstance(result, power_distributing.Success):
                    _logger.warning(
                        "PowerManagingActor: PowerDistributing failed: %s", result
                    )
                match result:
                    case power_distributing.PartialFailure(request):
                        if not last_result_partial_failure:
                            last_result_partial_failure = True
                            await self._send_updated_target_power(
                                frozenset(request.component_ids), None, must_send=True
                            )
                    case power_distributing.Success():
                        last_result_partial_failure = False
                await self._send_reports(frozenset(result.request.component_ids))

            elif selected_from(selected, drop_old_proposals_timer):
                self._algorithm.drop_old_proposals(asyncio.get_event_loop().time())
