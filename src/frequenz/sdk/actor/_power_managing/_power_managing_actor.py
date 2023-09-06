# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""The power manager."""

from __future__ import annotations

import logging
import typing

from frequenz.channels import Receiver, Sender
from typing_extensions import override

from .. import power_distributing
from .._actor import Actor
from ._base_classes import Algorithm, BaseAlgorithm, Proposal
from ._matryoshka import Matryoshka

_logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
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
        self._algorithm = algorithm
        super().__init__()

    @override
    async def _run(self) -> None:
        from .. import power_distributing  # pylint: disable=import-outside-toplevel

        if self._algorithm is not Algorithm.MATRYOSHKA:
            _logger.error(
                "PowerManagingActor: Algorithm %s not implemented", self._algorithm
            )
            return
        algorithm: BaseAlgorithm = Matryoshka()
        async for proposal in self._proposals_receiver:
            target_power = algorithm.handle_proposal(proposal)
            await self._power_distributing_requests_sender.send(
                power_distributing.Request(target_power, proposal.battery_ids)
            )
