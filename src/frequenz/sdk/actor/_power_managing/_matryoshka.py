# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A power manager implementation that uses the matryoshka algorithm."""

from __future__ import annotations

import typing

from typing_extensions import override

from ...timeseries import Power
from ._base_classes import BaseAlgorithm, Proposal
from ._sorted_set import SortedSet

if typing.TYPE_CHECKING:
    from ...timeseries.battery_pool import PowerMetrics


class Matryoshka(BaseAlgorithm):
    """The matryoshka algorithm."""

    def __init__(self) -> None:
        """Create a new instance of the matryoshka algorithm."""
        self._battery_buckets: dict[frozenset[int], SortedSet[Proposal]] = {}

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
        return target_power
