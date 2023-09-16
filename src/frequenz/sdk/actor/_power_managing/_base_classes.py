# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Base classes for the power manager."""

from __future__ import annotations

import abc
import dataclasses
import datetime
import enum
import typing

from ...timeseries import Power

if typing.TYPE_CHECKING:
    from ...timeseries.battery_pool import PowerMetrics


@dataclasses.dataclass(frozen=True)
class Proposal:
    """A proposal for a battery to be charged or discharged."""

    source_id: str
    preferred_power: Power
    bounds: tuple[Power, Power] | None
    battery_ids: frozenset[int]
    priority: int
    request_timeout: datetime.timedelta = datetime.timedelta(seconds=5.0)
    """The maximum amount of time to wait for the request to be fulfilled."""

    adjust_power: bool = True
    """Whether to adjust the power to match the bounds.

    If `True`, the power will be adjusted (lowered) to match the bounds, so
    only the reduced power will be set.

    If `False` and the power is outside the batteries' bounds, the request will
    fail and be replied to with an `OutOfBound` result.
    """

    include_broken_batteries: bool = False
    """Whether to use all batteries included in the batteries set regardless the status.

    If set to `True`, the power distribution algorithm will consider all batteries,
    including the broken ones, when distributing power.  In such cases, any remaining
    power after distributing among the available batteries will be distributed equally
    among the unavailable (broken) batteries.  If all batteries in the set are
    unavailable, the power will be equally distributed among all the unavailable
    batteries in the request.

    If set to `False`, the power distribution will only take into account the available
    batteries, excluding any broken ones.
    """

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
    def handle_proposal(
        self,
        proposal: Proposal,
        system_bounds: PowerMetrics,
    ) -> Power:
        """Handle a proposal.

        Args:
            proposal: The proposal to handle.
            system_bounds: The system bounds for the batteries in the proposal.

        Returns:
            The target power for the batteries in the proposal.
        """
