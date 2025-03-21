# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""A power manager implementation that uses the shifting matryoshka algorithm."""

from __future__ import annotations

import logging
import typing
from datetime import timedelta

from frequenz.quantities import Power
from typing_extensions import override

from frequenz.sdk.timeseries._base_types import Bounds

from ... import timeseries
from . import _bounds
from ._base_classes import BaseAlgorithm, Proposal, _Report

if typing.TYPE_CHECKING:
    from ...timeseries._base_types import SystemBounds

_logger = logging.getLogger(__name__)


class ShiftingMatryoshka(BaseAlgorithm):
    """The ShiftingMatryoshka algorithm.

    When there are multiple actors trying to control the same set of components, this
    algorithm will reconcile the different proposals and calculate the target power.

    Details about the algorithm can be found in the [microgrid module documentation](https://frequenz-floss.github.io/frequenz-sdk-python/v1.0-dev/user-guide/microgrid-concepts/#frequenz.sdk.microgrid--setting-power).
    """  # noqa: E501 (line too long)

    def __init__(self, max_proposal_age: timedelta) -> None:
        """Create a new instance of the matryoshka algorithm."""
        self._max_proposal_age_sec = max_proposal_age.total_seconds()
        self._component_buckets: dict[frozenset[int], set[Proposal]] = {}
        self._target_power: dict[frozenset[int], Power] = {}

    def _calc_targets(  # pylint: disable=too-many-locals,too-many-branches
        self,
        proposals: set[Proposal],
        system_bounds: SystemBounds,
        priority: int | None = None,
    ) -> tuple[Power | None, Bounds[Power]]:
        """Calculate the target power and bounds for the given components.

        Args:
            proposals: The proposals for the given components.
            system_bounds: The system bounds for the components in the proposal.
            priority: The priority of the actor for which the target power is calculated.

        Returns:
            The new target power and bounds for the components.
        """
        lower_bound = (
            system_bounds.inclusion_bounds.lower
            if system_bounds.inclusion_bounds
            # if a target power exists from a previous proposal, and the system bounds
            # have become unavailable, force the target power to be zero, by narrowing
            # the bounds to zero.
            else Power.zero()
        )
        upper_bound = (
            system_bounds.inclusion_bounds.upper
            if system_bounds.inclusion_bounds
            else Power.zero()
        )

        if not proposals:
            return None, Bounds[Power](lower=lower_bound, upper=upper_bound)

        available_bounds = Bounds[Power](lower=lower_bound, upper=upper_bound)
        top_pri_bounds: Bounds[Power] | None = None

        target_power = Power.zero()
        for next_proposal in sorted(proposals, reverse=True):
            if priority is not None and next_proposal.priority <= priority:
                break

            if upper_bound < lower_bound:
                break

            proposal_lower = next_proposal.bounds.lower or lower_bound
            proposal_upper = next_proposal.bounds.upper or upper_bound
            proposal_power = next_proposal.preferred_power

            if proposal_upper < proposal_lower:
                continue

            if proposal_power and (
                proposal_power < proposal_lower or proposal_power > proposal_upper
            ):
                continue

            if proposal_lower >= upper_bound:
                proposal_lower = upper_bound
                proposal_upper = upper_bound
            elif proposal_upper <= lower_bound:
                proposal_lower = lower_bound
                proposal_upper = lower_bound

            lower_bound = max(lower_bound, proposal_lower)
            upper_bound = min(upper_bound, proposal_upper)

            if proposal_power is not None:
                if top_pri_bounds is None and proposal_power != Power.zero():
                    top_pri_bounds = Bounds[Power](lower=lower_bound, upper=upper_bound)
                clamped = _bounds.clamp_to_bounds(
                    proposal_power,
                    lower_bound,
                    upper_bound,
                    None,
                )
                match clamped:
                    case (None, None):
                        proposal_power = Power.zero()
                    case (None, power) | (power, None) if power:
                        proposal_power = power
                    case (power_low, power_high) if power_low and power_high:
                        if power_high - proposal_power < proposal_power - power_low:
                            proposal_power = power_high
                        else:
                            proposal_power = power_low
                lower_bound = lower_bound - proposal_power
                upper_bound = upper_bound - proposal_power
                target_power += proposal_power

        if top_pri_bounds is not None:
            available_bounds = top_pri_bounds

        clamped = _bounds.clamp_to_bounds(
            target_power,
            available_bounds.lower,
            available_bounds.upper,
            system_bounds.exclusion_bounds,
        )
        match clamped:
            case (None, None):
                target_power = Power.zero()
            case (None, power) | (power, None) if power:
                target_power = power
            case (power_low, power_high) if power_low and power_high:
                if power_high - target_power < target_power - power_low:
                    target_power = power_high
                else:
                    target_power = power_low

        return target_power, Bounds[Power](lower=lower_bound, upper=upper_bound)

    def _validate_component_ids(
        self,
        component_ids: frozenset[int],
        proposal: Proposal | None,
        system_bounds: SystemBounds,
    ) -> bool:
        if component_ids not in self._component_buckets:
            # if there are no previous proposals and there are no system bounds, then
            # don't calculate a target power and fail the validation.
            if (
                system_bounds.inclusion_bounds is None
                and system_bounds.exclusion_bounds is None
            ):
                if proposal is not None:
                    _logger.warning(
                        "PowerManagingActor: No system bounds available for component "
                        + "IDs %s, but a proposal was given.  The proposal will be "
                        + "ignored.",
                        component_ids,
                    )
                return False

            for bucket in self._component_buckets:
                if any(component_id in bucket for component_id in component_ids):
                    raise NotImplementedError(
                        f"PowerManagingActor: component IDs {component_ids} are already"
                        + " part of another bucket.  Overlapping buckets are not"
                        + " yet supported."
                    )
        return True

    @override
    def calculate_target_power(
        self,
        component_ids: frozenset[int],
        proposal: Proposal | None,
        system_bounds: SystemBounds,
        must_return_power: bool = False,
    ) -> Power | None:
        """Calculate and return the target power for the given components.

        Args:
            component_ids: The component IDs to calculate the target power for.
            proposal: If given, the proposal to added to the bucket, before the target
                power is calculated.
            system_bounds: The system bounds for the components in the proposal.
            must_return_power: If `True`, the algorithm must return a target power,
                even if it hasn't changed since the last call.

        Returns:
            The new target power for the components, or `None` if the target power
                didn't change.

        Raises:  # noqa: DOC502
            NotImplementedError: When the proposal contains component IDs that are
                already part of another bucket.
        """
        if not self._validate_component_ids(component_ids, proposal, system_bounds):
            return None

        if proposal is not None:
            bucket = self._component_buckets.setdefault(component_ids, set())
            if proposal in bucket:
                bucket.remove(proposal)
            if (
                proposal.preferred_power is not None
                or proposal.bounds.lower is not None
                or proposal.bounds.upper is not None
            ):
                bucket.add(proposal)
            elif not bucket:
                del self._component_buckets[component_ids]
                _ = self._target_power.pop(component_ids, None)

        target_power, _ = self._calc_targets(
            self._component_buckets.get(component_ids, set()), system_bounds
        )

        if target_power is not None and (
            must_return_power
            or component_ids not in self._target_power
            or self._target_power[component_ids] != target_power
        ):
            self._target_power[component_ids] = target_power
            return target_power
        return None

    @override
    def get_status(  # pylint: disable=too-many-locals
        self,
        component_ids: frozenset[int],
        priority: int,
        system_bounds: SystemBounds,
    ) -> _Report:
        """Get the bounds for the algorithm.

        Args:
            component_ids: The IDs of the components to get the bounds for.
            priority: The priority of the actor for which the bounds are requested.
            system_bounds: The system bounds for the components.

        Returns:
            The target power and the available bounds for the given components, for
                the given priority.
        """
        target_power = self._target_power.get(component_ids)
        _, bounds = self._calc_targets(
            self._component_buckets.get(component_ids, set()), system_bounds, priority
        )
        return _Report(
            target_power=target_power,
            _inclusion_bounds=timeseries.Bounds[Power](
                lower=bounds.lower, upper=bounds.upper
            ),
            _exclusion_bounds=system_bounds.exclusion_bounds,
        )

    @override
    def drop_old_proposals(self, loop_time: float) -> None:
        """Drop old proposals.

        This will remove all proposals that have not been updated for longer than
        `max_proposal_age`.

        Args:
            loop_time: The current loop time.
        """
        buckets_to_delete: list[frozenset[int]] = []
        for component_ids, proposals in self._component_buckets.items():
            to_delete: list[Proposal] = []
            for proposal in proposals:
                if (loop_time - proposal.creation_time) > self._max_proposal_age_sec:
                    to_delete.append(proposal)
            for proposal in to_delete:
                proposals.remove(proposal)
            if not proposals:
                buckets_to_delete.append(component_ids)

        for component_ids in buckets_to_delete:
            del self._component_buckets[component_ids]
            _ = self._target_power.pop(component_ids, None)
