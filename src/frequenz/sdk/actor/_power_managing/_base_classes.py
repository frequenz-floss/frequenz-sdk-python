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
    from .. import power_distributing


@dataclasses.dataclass(frozen=True, kw_only=True)
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


@dataclasses.dataclass(frozen=True, kw_only=True)
class Report:
    """Current PowerManager report for a set of batteries."""

    target_power: Power | None
    """The currently set power for the batteries."""

    inclusion_bounds: timeseries.Bounds[Power] | None
    """The available inclusion bounds for the batteries, for the actor's priority.

    These bounds are adjusted to any restrictions placed by actors with higher
    priorities.
    """

    exclusion_bounds: timeseries.Bounds[Power] | None
    """The exclusion bounds for the batteries.

    The power manager doesn't manage exclusion bounds, so these are aggregations of
    values reported by the microgrid API.

    These bounds are adjusted to any restrictions placed by actors with higher
    priorities.
    """

    distribution_result: power_distributing.Result | None
    """The result of the last power distribution.

    This is `None` if no power distribution has been performed yet.
    """


@dataclasses.dataclass(frozen=True, kw_only=True)
class Proposal:
    """A proposal for a battery to be charged or discharged."""

    source_id: str
    """The source ID of the actor sending the request."""

    preferred_power: Power | None
    """The preferred power to be distributed to the batteries.

    If `None`, the preferred power of higher priority actors will get precedence.
    """

    bounds: timeseries.Bounds[Power | None]
    """The power bounds for the proposal.

    These bounds will apply to actors with a lower priority, and can be overridden by
    bounds from actors with a higher priority.  If None, the power bounds will be set to
    the maximum power of the batteries in the pool.  This is currently an experimental
    feature.
    """

    battery_ids: frozenset[int]
    """The battery IDs to distribute the power to."""

    priority: int
    """The priority of the actor sending the proposal."""

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
    def calculate_target_power(
        self,
        battery_ids: frozenset[int],
        proposal: Proposal | None,
        system_bounds: PowerMetrics,
        must_return_power: bool = False,
    ) -> Power | None:
        """Calculate and return the target power for the given batteries.

        Args:
            battery_ids: The battery IDs to calculate the target power for.
            proposal: If given, the proposal to added to the bucket, before the target
                power is calculated.
            system_bounds: The system bounds for the batteries in the proposal.
            must_return_power: If `True`, the algorithm must return a target power,
                even if it hasn't changed since the last call.

        Returns:
            The new target power for the batteries, or `None` if the target power
                didn't change.
        """

    # The arguments for this method are tightly coupled to the `Matryoshka` algorithm.
    # It can be loosened up when more algorithms are added.
    @abc.abstractmethod
    def get_status(
        self,
        battery_ids: frozenset[int],
        priority: int,
        system_bounds: PowerMetrics,
        distribution_result: power_distributing.Result | None,
    ) -> Report:
        """Get the bounds for a set of batteries, for the given priority.

        Args:
            battery_ids: The IDs of the batteries to get the bounds for.
            priority: The priority of the actor for which the bounds are requested.
            system_bounds: The system bounds for the batteries.
            distribution_result: The result of the last power distribution.

        Returns:
            The bounds for the batteries.
        """
