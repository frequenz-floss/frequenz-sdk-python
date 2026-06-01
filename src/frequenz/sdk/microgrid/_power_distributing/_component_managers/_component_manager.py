# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Manage the data streams for the components of the power distributor."""

import abc
import collections.abc
import logging
import typing

from frequenz.channels import LatestValueCache
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Power, Quantity

from ....timeseries import Sample
from ..request import Request

if typing.TYPE_CHECKING:
    from ....timeseries.formulas._formula_pool import FormulaPool

_logger = logging.getLogger(__name__)


def _quantity_sample_to_power(sample: Sample[Quantity]) -> Sample[Power]:
    """Convert a generic quantity sample from a formula into a power sample.

    Args:
        sample: The sample produced by an unreachable-power formula.

    Returns:
        The same sample with its value expressed as `Power`.
    """
    return Sample[Power](
        sample.timestamp,
        (
            Power.from_watts(sample.value.base_value)
            if sample.value is not None
            else None
        ),
    )


class _UnreachablePower(typing.NamedTuple):
    """A live subscription to the power of an unreachable subset of components."""

    component_ids: collections.abc.Set[ComponentId]
    """The inaccessible components whose power this subscription tracks."""

    formula: str
    """The formula string the pooled formula was created from.

    Stored so teardown can stop and evict exactly that formula from the owned pool
    by its string, instead of recomputing it (which would couple correct eviction
    to the formula generator staying deterministic).
    """

    cache: LatestValueCache[Sample[Power]]
    """A latest-value cache of the formula's result."""


class ComponentManager(abc.ABC):
    """Abstract class to manage the data streams for components."""

    def __init__(self) -> None:
        """Initialize the state shared by all component managers.

        Subclasses must call `super().__init__()`.
        """
        self._unreachable_power_subscriptions: dict[
            frozenset[ComponentId], _UnreachablePower
        ] = {}
        """Maps a requested component set to its unreachable subset's power.

        When components are inaccessible, for example due to network issues, they
        can't be controlled but might still be producing or consuming power.  That
        power is measured by a formula and subtracted from the distribution target
        so the reachable components compensate for it.  So for each requested set we
        track the inaccessible subset, the formula string, and a latest-value cache
        of the formula's result for that subset.
        """

        self._unreachable_power_pool: "FormulaPool | None" = None
        """A formula pool owned by this manager for the unreachable-power formulas.

        Created lazily on the first subscription (so managers that never subscribe
        pay nothing).  Owning the pool keeps these formulas isolated from the shared
        logical meter and lets the manager stop all of them together, bounding the
        leak to the manager's own lifetime.
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
        changes, and tears the subscription down once nothing is unreachable anymore.

        Args:
            requested_ids: The components the current request targets.
            working_ids: The subset of `requested_ids` that is currently reachable.
        """
        requested = frozenset(requested_ids)
        unreachable = requested - working_ids

        existing = self._unreachable_power_subscriptions.get(requested)
        if not unreachable:
            if existing is not None:
                _logger.debug(
                    "All requested components are reachable again; stopping the "
                    "unreachable-power subscription for %s.",
                    sorted(requested),
                )
                del self._unreachable_power_subscriptions[requested]
                await self._stop_unreachable_power_subscription(existing)
            return
        if existing is not None:
            if existing.component_ids == unreachable:
                return
            _logger.debug(
                "Unreachable subset of %s changed to %s; restarting its power "
                "subscription.",
                sorted(requested),
                sorted(unreachable),
            )
            del self._unreachable_power_subscriptions[requested]
            await self._stop_unreachable_power_subscription(existing)

        if self._unreachable_power_pool is None:
            # Imported here, and the pool created lazily, to avoid an import cycle
            # with the data pipeline (which imports the power distributor).
            #
            # pylint: disable-next=import-outside-toplevel,cyclic-import
            from ..._data_pipeline import _new_formula_pool

            self._unreachable_power_pool = _new_formula_pool(
                "unreachable-component-power"
            )

        formula = self._unreachable_power_formula(unreachable)
        _logger.debug(
            "Subscribing to the power of unreachable components %s with formula: %s",
            sorted(unreachable),
            formula,
        )
        cache = LatestValueCache(
            self._unreachable_power_pool.from_string(formula, Metric.AC_POWER_ACTIVE)
            .new_receiver()
            .map(_quantity_sample_to_power)
        )
        self._unreachable_power_subscriptions[requested] = _UnreachablePower(
            unreachable, formula, cache
        )

    def _unreachable_power(
        self, requested_ids: collections.abc.Set[ComponentId]
    ) -> Power | None:
        """Return the latest power measured on the unreachable components.

        Args:
            requested_ids: The components the request targets.

        Returns:
            The latest cached power of the unreachable subset, or `None` if there is
            no subscription for this set or no value has arrived yet.
        """
        subscription = self._unreachable_power_subscriptions.get(
            frozenset(requested_ids)
        )
        if subscription is not None and subscription.cache.has_value():
            return subscription.cache.get().value
        return None

    async def _stop_unreachable_power_subscription(
        self, subscription: _UnreachablePower
    ) -> None:
        """Tear down one unreachable-power subscription.

        The subscription must already have been removed from
        `_unreachable_power_subscriptions` so the shared-formula check below is
        accurate.

        Args:
            subscription: The subscription to stop.
        """
        try:
            # try/finally so the formula is stopped even if stopping the cache fails
            # — otherwise the formula would leak.
            await subscription.cache.stop()
        finally:
            # The pool caches one formula per distinct formula string and keeps it
            # (and its evaluating actor) running for the life of the manager, so stop
            # it once no remaining subscription references the same formula.
            if self._unreachable_power_pool is not None and not any(
                other.formula == subscription.formula
                for other in self._unreachable_power_subscriptions.values()
            ):
                await self._unreachable_power_pool.stop_string_formula(
                    subscription.formula, Metric.AC_POWER_ACTIVE
                )

    async def _stop_all_unreachable_power_subscriptions(self) -> None:
        """Stop every unreachable-power subscription and the owned formula pool."""
        for subscription in self._unreachable_power_subscriptions.values():
            await subscription.cache.stop()
        self._unreachable_power_subscriptions.clear()
        if self._unreachable_power_pool is not None:
            await self._unreachable_power_pool.stop()
            self._unreachable_power_pool = None
