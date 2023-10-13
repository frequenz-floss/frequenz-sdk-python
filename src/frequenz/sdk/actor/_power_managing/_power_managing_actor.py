# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""The power manager."""

from __future__ import annotations

import asyncio
import logging
import typing
from datetime import datetime, timedelta, timezone

from frequenz.channels import Receiver, Sender
from frequenz.channels.util import select, selected_from
from typing_extensions import override

from .._actor import Actor
from .._channel_registry import ChannelRegistry
from ._base_classes import Algorithm, BaseAlgorithm, Proposal, Report, ReportRequest
from ._matryoshka import Matryoshka

_logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from ...timeseries.battery_pool import PowerMetrics
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
            algorithm: The power management algorithm to use.

        Raises:
            NotImplementedError: When an unknown algorithm is given.
        """
        if algorithm is not Algorithm.MATRYOSHKA:
            raise NotImplementedError(
                f"PowerManagingActor: Unknown algorithm: {algorithm}"
            )

        self._bounds_subscription_receiver = bounds_subscription_receiver
        self._power_distributing_requests_sender = power_distributing_requests_sender
        self._power_distributing_results_receiver = power_distributing_results_receiver
        self._channel_registry = channel_registry
        self._proposals_receiver = proposals_receiver

        self._system_bounds: dict[frozenset[int], PowerMetrics] = {}
        self._bound_tracker_tasks: dict[frozenset[int], asyncio.Task[None]] = {}
        self._subscriptions: dict[frozenset[int], dict[int, Sender[Report]]] = {}
        self._distribution_results: dict[frozenset[int], power_distributing.Result] = {}

        self._algorithm: BaseAlgorithm = Matryoshka()

        super().__init__()

    async def _send_reports(self, battery_ids: frozenset[int]) -> None:
        """Send reports for a set of batteries, to all subscribers.

        Args:
            battery_ids: The battery IDs.
        """
        bounds = self._system_bounds.get(battery_ids)
        if bounds is None:
            _logger.warning("PowerManagingActor: No bounds for %s", battery_ids)
            return
        for priority, sender in self._subscriptions.get(battery_ids, {}).items():
            status = self._algorithm.get_status(
                battery_ids,
                priority,
                bounds,
                self._distribution_results.get(battery_ids),
            )
            await sender.send(status)

    async def _bounds_tracker(
        self,
        battery_ids: frozenset[int],
        bounds_receiver: Receiver[PowerMetrics],
    ) -> None:
        """Track the power bounds of a set of batteries and update the cache.

        Args:
            battery_ids: The battery IDs.
            bounds_receiver: The receiver for power bounds.
        """
        async for bounds in bounds_receiver:
            self._system_bounds[battery_ids] = bounds
            await self._send_updated_target_power(battery_ids, None)
            await self._send_reports(battery_ids)

    def _add_bounds_tracker(self, battery_ids: frozenset[int]) -> None:
        """Add a bounds tracker.

        Args:
            battery_ids: The battery IDs.
        """
        # Pylint assumes that this import is cyclic, but it's not.
        from ... import (  # pylint: disable=import-outside-toplevel,cyclic-import
            microgrid,
        )
        from ...timeseries.battery_pool import (  # pylint: disable=import-outside-toplevel
            PowerMetrics,
        )

        battery_pool = microgrid.battery_pool(battery_ids)
        # pylint: disable=protected-access
        bounds_receiver = battery_pool._system_power_bounds.new_receiver()
        # pylint: enable=protected-access

        self._system_bounds[battery_ids] = PowerMetrics(
            timestamp=datetime.now(tz=timezone.utc),
            inclusion_bounds=None,
            exclusion_bounds=None,
        )

        # Start the bounds tracker, for ongoing updates.
        self._bound_tracker_tasks[battery_ids] = asyncio.create_task(
            self._bounds_tracker(battery_ids, bounds_receiver)
        )

    async def _send_updated_target_power(
        self,
        battery_ids: frozenset[int],
        proposal: Proposal | None,
        must_send: bool = False,
    ) -> None:
        from .. import power_distributing  # pylint: disable=import-outside-toplevel

        target_power = self._algorithm.calculate_target_power(
            battery_ids,
            proposal,
            self._system_bounds[battery_ids],
            must_send,
        )
        request_timeout = (
            proposal.request_timeout if proposal else timedelta(seconds=5.0)
        )
        include_broken_batteries = (
            proposal.include_broken_batteries if proposal else False
        )
        if target_power is not None:
            await self._power_distributing_requests_sender.send(
                power_distributing.Request(
                    power=target_power,
                    batteries=battery_ids,
                    request_timeout=request_timeout,
                    adjust_power=True,
                    include_broken_batteries=include_broken_batteries,
                )
            )

    @override
    async def _run(self) -> None:
        """Run the power managing actor."""
        async for selected in select(
            self._proposals_receiver,
            self._bounds_subscription_receiver,
            self._power_distributing_results_receiver,
        ):
            if selected_from(selected, self._proposals_receiver):
                proposal = selected.value
                if proposal.battery_ids not in self._bound_tracker_tasks:
                    self._add_bounds_tracker(proposal.battery_ids)

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
                    proposal.battery_ids, proposal, must_send=True
                )
                await self._send_reports(proposal.battery_ids)

            elif selected_from(selected, self._bounds_subscription_receiver):
                sub = selected.value
                battery_ids = sub.battery_ids
                priority = sub.priority

                if battery_ids not in self._subscriptions:
                    self._subscriptions[battery_ids] = {
                        priority: self._channel_registry.new_sender(
                            sub.get_channel_name()
                        )
                    }
                elif priority not in self._subscriptions[battery_ids]:
                    self._subscriptions[battery_ids][
                        priority
                    ] = self._channel_registry.new_sender(sub.get_channel_name())

                if sub.battery_ids not in self._bound_tracker_tasks:
                    self._add_bounds_tracker(sub.battery_ids)

            elif selected_from(selected, self._power_distributing_results_receiver):
                from .. import (  # pylint: disable=import-outside-toplevel
                    power_distributing,
                )

                result = selected.value
                self._distribution_results[frozenset(result.request.batteries)] = result
                match result:
                    case power_distributing.PartialFailure(request):
                        await self._send_updated_target_power(
                            frozenset(request.batteries), None, must_send=True
                        )
                await self._send_reports(frozenset(result.request.batteries))
