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
from . import _bounds

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

    distribution_result: power_distributing.Result | None
    """The result of the last power distribution.

    This is `None` if no power distribution has been performed yet.
    """

    _inclusion_bounds: timeseries.Bounds[Power] | None
    """The available inclusion bounds for the batteries, for the actor's priority.

    These bounds are adjusted to any restrictions placed by actors with higher
    priorities.
    """

    _exclusion_bounds: timeseries.Bounds[Power] | None
    """The exclusion bounds for the batteries.

    The power manager doesn't manage exclusion bounds, so these are aggregations of
    values reported by the microgrid API.

    These bounds are adjusted to any restrictions placed by actors with higher
    priorities.
    """

    @property
    def bounds(self) -> timeseries.Bounds[Power] | None:
        """The bounds for the batteries.

        These bounds are adjusted to any restrictions placed by actors with higher
        priorities.

        There might be exclusion zones within these bounds. If necessary, the
        [`adjust_to_bounds`][frequenz.sdk.timeseries.battery_pool.Report.adjust_to_bounds]
        method may be used to check if a desired power value fits the bounds, or to get
        the closest possible power values that do fit the bounds.
        """
        return self._inclusion_bounds

    def adjust_to_bounds(self, power: Power) -> tuple[Power | None, Power | None]:
        """Adjust a power value to the bounds.

        This method can be used to adjust a desired power value to the power bounds
        available to the actor.

        If the given power value falls within the usable bounds, it will be returned
        unchanged.

        If it falls outside the usable bounds, the closest possible value on the
        corresponding side will be returned.  For example, if the given power is lower
        than the lowest usable power, only the lowest usable power will be returned, and
        similarly for the highest usable power.

        If the given power falls within an exclusion zone that's contained within the
        usable bounds, the closest possible power values on both sides will be returned.

        !!! note
            It is completely optional to use this method to adjust power values before
            proposing them through the battery pool, because the battery pool will do
            this automatically.  This method is provided for convenience, and for
            granular control when there are two possible power values, both of which
            fall within the available bounds.

        Example:
            ```python
            from frequenz.sdk import microgrid

            power_status_rx = microgrid.battery_pool().power_status.new_receiver()
            power_status = await power_status_rx.receive()
            desired_power = Power.from_watts(1000.0)

            match power_status.adjust_to_bounds(desired_power):
                case (power, _) if power == desired_power:
                    print("Desired power is available.")
                case (None, power) | (power, None) if power:
                    print(f"Closest available power is {power}.")
                case (lower, upper) if lower and upper:
                    print(f"Two options {lower}, {upper} to propose to battery pool.")
                case (None, None):
                    print("No available power")
            ```

        Args:
            power: The power value to adjust.

        Returns:
            A tuple of the closest power values to the desired power that fall within
                the available bounds for the actor.
        """
        if self._inclusion_bounds is None:
            return None, None

        return _bounds.clamp_to_bounds(
            power,
            self._inclusion_bounds.lower,
            self._inclusion_bounds.upper,
            self._exclusion_bounds,
        )


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
