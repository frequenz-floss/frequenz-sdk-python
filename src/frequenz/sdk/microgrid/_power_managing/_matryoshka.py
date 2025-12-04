# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A power manager implementation that uses the matryoshka algorithm.

When there are multiple proposals from different actors for the same set of components,
the matryoshka algorithm will consider the priority of the actors, the bounds they set
and their preferred power to determine the target power for the components.

The preferred power of lower priority actors will take precedence as long as they
respect the bounds set by higher priority actors.  If lower priority actors request
power values outside the bounds set by higher priority actors, the target power will
be the closest value to the preferred power that is within the bounds.

When there is only a single proposal for a set of components, its preferred power would
be the target power, as long as it falls within the system power bounds for the
components.
"""

from __future__ import annotations

import logging
import typing
from datetime import timedelta

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power
from typing_extensions import override

from ... import timeseries
from . import _bounds
from ._base_classes import BaseAlgorithm, DefaultPower, Proposal, _Report

if typing.TYPE_CHECKING:
    from ...timeseries._base_types import SystemBounds

_logger = logging.getLogger(__name__)


class Matryoshka(BaseAlgorithm):
    """The matryoshka algorithm."""

    def __init__(
        self, max_proposal_age: timedelta, default_power: DefaultPower
    ) -> None:
        """Create a new instance of the matryoshka algorithm."""
        self._max_proposal_age_sec = max_proposal_age.total_seconds()
        self._default_power = default_power
        self._component_buckets: dict[frozenset[ComponentId], set[Proposal]] = {}
        self._target_power: dict[frozenset[ComponentId], Power] = {}

    def _calc_target_power(
        self,
        proposals: set[Proposal],
        system_bounds: SystemBounds,
    ) -> Power | None:
        """Calculate the target power for the given components.

        Args:
            proposals: The proposals for the given components.
            system_bounds: The system bounds for the components in the proposal.

        Returns:
            The new target power for the components.
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

        exclusion_bounds = None
        if system_bounds.exclusion_bounds is not None and (
            system_bounds.exclusion_bounds.lower != Power.zero()
            or system_bounds.exclusion_bounds.upper != Power.zero()
        ):
            exclusion_bounds = system_bounds.exclusion_bounds

        target_power = None
        for next_proposal in sorted(proposals, reverse=True):
            if upper_bound < lower_bound:
                break
            if next_proposal.preferred_power:
                match _bounds.clamp_to_bounds(
                    next_proposal.preferred_power,
                    lower_bound,
                    upper_bound,
                    exclusion_bounds,
                ):
                    case (None, power) | (power, None) if power:
                        target_power = power
                    case (power_low, power_high) if power_low and power_high:
                        if (
                            power_high - next_proposal.preferred_power
                            < next_proposal.preferred_power - power_low
                        ):
                            target_power = power_high
                        else:
                            target_power = power_low

            proposal_lower = next_proposal.bounds.lower or lower_bound
            proposal_upper = next_proposal.bounds.upper or upper_bound
            # If the bounds from the current proposal are fully within the exclusion
            # bounds, then don't use them to narrow the bounds further. This allows
            # subsequent proposals to not be blocked by the current proposal.
            match _bounds.check_exclusion_bounds_overlap(
                proposal_lower, proposal_upper, exclusion_bounds
            ):
                case (True, True):
                    continue
            lower_bound = max(lower_bound, proposal_lower)
            upper_bound = min(upper_bound, proposal_upper)
            lower_bound, upper_bound = _bounds.adjust_exclusion_bounds(
                lower_bound, upper_bound, exclusion_bounds
            )

        return target_power

    def _validate_component_ids(
        self,
        component_ids: frozenset[ComponentId],
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
                        "IDs %s, but a proposal was given.  The proposal will be "
                        "ignored.",
                        component_ids,
                    )
                return False

            for bucket in self._component_buckets:
                if any(component_id in bucket for component_id in component_ids):
                    comp_ids = ", ".join(map(str, sorted(component_ids)))
                    raise NotImplementedError(
                        f"PowerManagingActor: {comp_ids} are already part of another "
                        "bucket. Overlapping buckets are not yet supported."
                    )
        return True

    @override
    def calculate_target_power(
        self,
        component_ids: frozenset[ComponentId],
        proposal: Proposal | None,
        system_bounds: SystemBounds,
    ) -> Power | None:
        """Calculate and return the target power for the given components.

        Args:
            component_ids: The component IDs to calculate the target power for.
            proposal: If given, the proposal to added to the bucket, before the target
                power is calculated.
            system_bounds: The system bounds for the components in the proposal.

        Returns:
            The new target power for the components, or `None` if the target power
                couldn't be calculated.

        Raises:  # noqa: DOC502
            NotImplementedError: When the proposal contains component IDs that are
                already part of another bucket.
        """
        if not self._validate_component_ids(component_ids, proposal, system_bounds):
            return None

        self._update_buckets(component_ids, proposal)

        # If there has not been any proposal for the given components, don't calculate a
        # target power and just return `None`.
        proposals = self._component_buckets.get(component_ids)

        target_power = None
        if proposals is not None:
            target_power = self._calc_target_power(proposals, system_bounds)

        if target_power is not None:
            self._target_power[component_ids] = target_power
        elif self._target_power.get(component_ids) is not None:
            # If the target power was previously set, but is now `None`, then we send
            # the default power of the component category, to reset it immediately.
            del self._target_power[component_ids]
            bounds = system_bounds.inclusion_bounds
            if bounds is None:
                return None
            match self._default_power:
                case DefaultPower.MIN:
                    return bounds.lower
                case DefaultPower.MAX:
                    return bounds.upper
                case DefaultPower.ZERO:
                    return Power.zero()
                case other:
                    typing.assert_never(other)

        return target_power

    def _update_buckets(
        self, component_ids: frozenset[ComponentId], proposal: Proposal | None
    ) -> None:
        """Update the component buckets with the given proposal."""
        if proposal is None:
            return

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

    @override
    def get_status(
        self,
        component_ids: frozenset[ComponentId],
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
        if system_bounds.inclusion_bounds is None:
            return _Report(
                target_power=target_power,
                _inclusion_bounds=None,
                _exclusion_bounds=system_bounds.exclusion_bounds,
            )

        lower_bound = system_bounds.inclusion_bounds.lower
        upper_bound = system_bounds.inclusion_bounds.upper

        exclusion_bounds = None
        if system_bounds.exclusion_bounds is not None and (
            system_bounds.exclusion_bounds.lower != Power.zero()
            or system_bounds.exclusion_bounds.upper != Power.zero()
        ):
            exclusion_bounds = system_bounds.exclusion_bounds

        for next_proposal in sorted(
            self._component_buckets.get(component_ids, []), reverse=True
        ):
            if next_proposal.priority <= priority:
                break
            proposal_lower = next_proposal.bounds.lower or lower_bound
            proposal_upper = next_proposal.bounds.upper or upper_bound
            match _bounds.check_exclusion_bounds_overlap(
                proposal_lower, proposal_upper, exclusion_bounds
            ):
                case (True, True):
                    continue
            calc_lower_bound = max(lower_bound, proposal_lower)
            calc_upper_bound = min(upper_bound, proposal_upper)
            if calc_lower_bound <= calc_upper_bound:
                lower_bound, upper_bound = _bounds.adjust_exclusion_bounds(
                    calc_lower_bound, calc_upper_bound, exclusion_bounds
                )
            else:
                break
        return _Report(
            target_power=target_power,
            _inclusion_bounds=timeseries.Bounds[Power](
                lower=lower_bound, upper=upper_bound
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
        buckets_to_delete: list[frozenset[ComponentId]] = []
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
