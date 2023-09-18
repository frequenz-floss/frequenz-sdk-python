# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A power manager implementation that uses the matryoshka algorithm."""

from __future__ import annotations

import typing

from typing_extensions import override

from ...timeseries import Power
from ._base_classes import BaseAlgorithm, Bounds, Proposal, Report
from ._sorted_set import SortedSet

if typing.TYPE_CHECKING:
    from ...timeseries.battery_pool import PowerMetrics


class Matryoshka(BaseAlgorithm):
    """The matryoshka algorithm."""

    def __init__(self) -> None:
        """Create a new instance of the matryoshka algorithm."""
        self._battery_buckets: dict[frozenset[int], SortedSet[Proposal]] = {}
        self._target_power: dict[frozenset[int], Power] = {}

    @override
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
            The battery IDs and the target power.

        Raises:
            NotImplementedError: When the proposal contains battery IDs that are
                already part of another bucket.
        """
        battery_ids = proposal.battery_ids
        if battery_ids not in self._battery_buckets:
            for bucket in self._battery_buckets:
                if any(battery_id in bucket for battery_id in battery_ids):
                    raise NotImplementedError(
                        f"PowerManagingActor: Battery IDs {battery_ids} are already "
                        "part of another bucket.  Overlapping buckets are not "
                        "yet supported."
                    )
        self._battery_buckets.setdefault(battery_ids, SortedSet()).insert(proposal)
        lower_bound = (
            system_bounds.inclusion_bounds.lower
            if system_bounds.inclusion_bounds
            else Power.zero()  # in the absence of system bounds, block all requests.
        )
        upper_bound = (
            system_bounds.inclusion_bounds.upper
            if system_bounds.inclusion_bounds
            else Power.zero()
        )
        target_power = Power.zero()
        for next_proposal in reversed(self._battery_buckets[battery_ids]):
            if (
                next_proposal.preferred_power > upper_bound
                or next_proposal.preferred_power < lower_bound
            ):
                continue
            target_power = next_proposal.preferred_power
            if next_proposal.bounds:
                lower_bound = next_proposal.bounds[0]
                upper_bound = next_proposal.bounds[1]

        self._target_power[battery_ids] = target_power
        return target_power

    @override
    def get_status(
        self, battery_ids: frozenset[int], priority: int, system_bounds: PowerMetrics
    ) -> Report:
        """Get the bounds for the algorithm.

        Args:
            battery_ids: The IDs of the batteries to get the bounds for.
            priority: The priority of the actor for which the bounds are requested.
            system_bounds: The system bounds for the batteries.

        Returns:
            The bounds.

        Raises:
            NotImplementedError: When the battery IDs are not part of a bucket.
        """
        lower_bound = (
            system_bounds.inclusion_bounds.lower
            if system_bounds.inclusion_bounds
            else Power.zero()  # in the absence of system bounds, block all requests.
        )
        upper_bound = (
            system_bounds.inclusion_bounds.upper
            if system_bounds.inclusion_bounds
            else Power.zero()
        )
        for next_proposal in reversed(self._battery_buckets[battery_ids]):
            if next_proposal.priority <= priority:
                break
            if next_proposal.bounds:
                lower_bound = next_proposal.bounds[0]
                upper_bound = next_proposal.bounds[1]

        return Report(
            target_power=self._target_power[battery_ids],
            available_bounds=Bounds(lower=lower_bound, upper=upper_bound),
        )
