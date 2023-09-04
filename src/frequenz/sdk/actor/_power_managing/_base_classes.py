# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Base classes for the power manager."""

from __future__ import annotations

import abc
import dataclasses
import enum

from ...timeseries import Power


@dataclasses.dataclass(frozen=True)
class Proposal:
    """A proposal for a battery to be charged or discharged."""

    source_id: str
    preferred_power: Power
    bounds: tuple[Power, Power]
    battery_ids: frozenset[int]
    priority: int

    def __lt__(self, other: Proposal) -> bool:
        """Compare two proposals by their priority.

        When they have the same priority, compare them by their source ID.

        Args:
            other: The other proposal to compare to.

        Returns:
            Whether this proposal has a higher priority than the other proposal.
        """
        return (self.priority < other.priority) or (
            self.priority == other.priority and self.source_id < other.source_id
        )


class Algorithm(enum.Enum):
    """The available algorithms for the power manager."""

    MATRYOSHKA = "matryoshka"


class BaseAlgorithm(abc.ABC):
    """The base class for algorithms."""

    @abc.abstractmethod
    def handle_proposal(self, proposal: Proposal) -> Power:
        """Handle a proposal.

        Args:
            proposal: The proposal to handle.

        Returns:
            The target power for the batteries in the proposal.
        """
