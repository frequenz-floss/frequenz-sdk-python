# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Base classes for the power manager."""

from __future__ import annotations

import abc
import dataclasses
import datetime
import enum
import typing

from ... import timeseries
from ...timeseries import Power

if typing.TYPE_CHECKING:
    from ...timeseries.battery_pool import PowerMetrics


@dataclasses.dataclass(frozen=True)
class ReportRequest:
    """A request to start a reporting stream."""

    source_id: str
    """The source ID of the actor sending the request."""

    battery_ids: frozenset[int]
    """The battery IDs to report on."""

    priority: int
    """The priority of the actor ."""

    def get_channel_name(self) -> str:
        """Get the channel name for the report request.

        Returns:
            The channel name to use to identify the corresponding report channel
                from the channel registry.
        """
        return f"power_manager.report.{self.battery_ids=}.{self.priority=}"


@dataclasses.dataclass(frozen=True)
class Report:
    """Current PowerManager report for a set of batteries."""

    target_power: Power | None
    """The currently set power for the batteries."""

    inclusion_bounds: timeseries.Bounds[Power] | None
    """The available inclusion bounds for the batteries, for the actor's priority.

    These bounds are adjusted to any restrictions placed by actors with higher
    priorities.
    """


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
    def get_target_power(
        self,
        battery_ids: frozenset[int],
        proposal: Proposal | None,
        system_bounds: PowerMetrics,
    ) -> Power | None:
        """Calculate and return the target power for the given batteries.

        Args:
            battery_ids: The battery IDs to calculate the target power for.
            proposal: If given, the proposal to added to the bucket, before the target
                power is calculated.
            system_bounds: The system bounds for the batteries in the proposal.

        Returns:
            The new target power for the batteries, or `None` if the target power
                didn't change.
        """

    # The arguments for this method are tightly coupled to the `Matryoshka` algorithm.
    # It can be loosened up when more algorithms are added.
    @abc.abstractmethod
    def get_status(
        self, battery_ids: frozenset[int], priority: int, system_bounds: PowerMetrics
    ) -> Report:
        """Get the bounds for a set of batteries, for the given priority.

        Args:
            battery_ids: The IDs of the batteries to get the bounds for.
            priority: The priority of the actor for which the bounds are requested.
            system_bounds: The system bounds for the batteries.

        Returns:
            The bounds for the batteries.
        """
