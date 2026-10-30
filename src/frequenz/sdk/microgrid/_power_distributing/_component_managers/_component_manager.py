# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Manage the data streams for the components of the power distributor."""

import abc
import collections.abc
import logging

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power

from .._unreachable_power import UnreachablePowerTracker
from ..request import Request

_logger = logging.getLogger(__name__)


class ComponentManager(abc.ABC):
    """Abstract class to manage the data streams for components."""

    def __init__(self) -> None:
        """Initialize the state shared by all component managers.

        Subclasses must call `super().__init__()`.
        """
        self._unreachable_power_trackers: dict[
            frozenset[ComponentId], UnreachablePowerTracker
        ] = {}
        """Maps a requested component set to the power of its unreachable subset.

        When components are inaccessible, for example due to network issues, they
        can't be controlled but might still be producing or consuming power.  That
        power is measured by a formula and subtracted from the distribution target
        so the reachable components compensate for it.
        """

    @abc.abstractmethod
    def component_ids(self) -> collections.abc.Set[ComponentId]:
        """Return the set of component ids."""

    @abc.abstractmethod
    async def start(self) -> None:
        """Start the component data manager."""

    @abc.abstractmethod
    async def distribute_power(self, request: Request) -> None:
        """Distribute the requested power to the components.

        Args:
            request: Request to get the distribution for.
        """

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop the component data manager."""

    def _unreachable_power_formula(
        self, component_ids: collections.abc.Set[ComponentId]
    ) -> str:
        """Return a formula string for the active power of the given components.

        Managers that subscribe to unreachable power must override this to return
        the component-graph formula for their component type (e.g. `battery_formula`
        or `pv_formula`).

        Args:
            component_ids: The components to build the power formula for.

        Returns:
            A formula string for the active power of the given components.

        Raises:
            NotImplementedError: If the manager subscribes to unreachable power
                without overriding this method.
        """
        raise NotImplementedError(
            f"{type(self).__name__} subscribed to unreachable power without "
            "providing an unreachable-power formula."
        )

    async def _subscribe_to_unreachable_power(
        self,
        requested_ids: collections.abc.Set[ComponentId],
        working_ids: collections.abc.Set[ComponentId],
    ) -> None:
        """Track the power of the unreachable subset of the requested components.

        Idempotent per requested set: re-subscribes only when the unreachable subset
        changes, and stops the formula once nothing is unreachable anymore.

        Args:
            requested_ids: The components the current request targets.
            working_ids: The subset of `requested_ids` that is currently reachable.
        """
        requested = frozenset(requested_ids)
        tracker = self._unreachable_power_trackers.get(requested)
        if tracker is None:
            if not requested - working_ids:
                return
            tracker = UnreachablePowerTracker(
                requested, self._unreachable_power_formula
            )
            self._unreachable_power_trackers[requested] = tracker
        await tracker.update(working_ids)

    def _unreachable_power(
        self, requested_ids: collections.abc.Set[ComponentId]
    ) -> Power | None:
        """Return the latest power measured on the unreachable components.

        Args:
            requested_ids: The components the request targets.

        Returns:
            The latest power of the unreachable subset, or `None` if nothing is
                tracked for the requested components or no value has arrived yet.
        """
        tracker = self._unreachable_power_trackers.get(frozenset(requested_ids))
        return tracker.power if tracker is not None else None

    async def _stop_all_unreachable_power_subscriptions(self) -> None:
        """Stop every unreachable-power tracker."""
        # Take the trackers out before awaiting, because a request being distributed
        # meanwhile can add trackers.
        trackers = list(self._unreachable_power_trackers.values())
        self._unreachable_power_trackers.clear()
        for tracker in trackers:
            await tracker.stop()
