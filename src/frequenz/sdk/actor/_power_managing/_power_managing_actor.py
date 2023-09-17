# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""The power manager."""

from __future__ import annotations

import asyncio
import logging
import typing

from frequenz.channels import Receiver, Sender
from typing_extensions import override

from .._actor import Actor
from ._base_classes import Algorithm, BaseAlgorithm, Proposal
from ._matryoshka import Matryoshka

_logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from ...timeseries.battery_pool import PowerMetrics
    from .. import power_distributing


class PowerManagingActor(Actor):
    """The power manager."""

    def __init__(
        self,
        proposals_receiver: Receiver[Proposal],
        power_distributing_requests_sender: Sender[power_distributing.Request],
        algorithm: Algorithm = Algorithm.MATRYOSHKA,
    ):
        """Create a new instance of the power manager.

        Args:
            proposals_receiver: The receiver for proposals.
            power_distributing_requests_sender: The sender for power distribution
                requests.
            algorithm: The power management algorithm to use.

        Raises:
            NotImplementedError: When an unknown algorithm is given.
        """
        self._power_distributing_requests_sender = power_distributing_requests_sender
        self._proposals_receiver = proposals_receiver
        if algorithm is not Algorithm.MATRYOSHKA:
            raise NotImplementedError(
                f"PowerManagingActor: Unknown algorithm: {algorithm}"
            )

        self._system_bounds: dict[frozenset[int], PowerMetrics] = {}
        self._bound_tracker_tasks: dict[frozenset[int], asyncio.Task[None]] = {}

        super().__init__()

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

    async def _add_bounds_tracker(self, battery_ids: frozenset[int]) -> None:
        """Add a bounds tracker.

        Args:
            battery_ids: The battery IDs.
        """
        # Pylint assumes that this import is cyclic, but it's not.
        from ... import (  # pylint: disable=import-outside-toplevel,cyclic-import
            microgrid,
        )

        # Fetch the current bounds separately, so that when this function returns,
        # there's already some bounds available.
        battery_pool = microgrid.battery_pool(battery_ids)
        bounds_receiver = battery_pool.power_bounds.new_receiver()
        self._system_bounds[battery_ids] = await bounds_receiver.receive()

        # Start the bounds tracker, for ongoing updates.
        self._bound_tracker_tasks[battery_ids] = asyncio.create_task(
            self._bounds_tracker(battery_ids, bounds_receiver)
        )

    @override
    async def _run(self) -> None:
        from .. import power_distributing  # pylint: disable=import-outside-toplevel

        algorithm: BaseAlgorithm = Matryoshka()

        async for proposal in self._proposals_receiver:
            if proposal.battery_ids not in self._system_bounds:
                await self._add_bounds_tracker(proposal.battery_ids)

            target_power = algorithm.handle_proposal(
                proposal, self._system_bounds[proposal.battery_ids]
            )

            await self._power_distributing_requests_sender.send(
                power_distributing.Request(
                    power=target_power,
                    batteries=proposal.battery_ids,
                    request_timeout=proposal.request_timeout,
                    adjust_power=True,
                    include_broken_batteries=proposal.include_broken_batteries,
                )
            )
