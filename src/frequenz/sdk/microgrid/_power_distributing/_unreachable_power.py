# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Track the power of the components that can't be controlled right now."""

import asyncio
import collections.abc
import logging
import typing

from frequenz.channels import Broadcast, Receiver
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.metrics import Metric
from frequenz.microgrid_component_graph import FormulaGenerationError
from frequenz.quantities import Power, Quantity

from ..._internal._asyncio import cancel_and_await
from ...timeseries import Sample

if typing.TYPE_CHECKING:
    from ...timeseries.formulas._formula_pool import FormulaPool

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


class UnreachablePowerTracker:
    """Track the power of the components that can't be controlled right now.

    Here, a component is unreachable when neither it nor any component of its
    sibling group is in the working set given to `update`.  It might still be
    producing or consuming power.

    That power is measured by a formula, which is restarted whenever the set of
    unreachable components changes.  Nothing is tracked while no component is
    working.  How the power is measured, for example from a meter when a component
    stops sending data, depends on the formula.
    """

    def __init__(
        self,
        component_ids: collections.abc.Set[ComponentId],
        formula_generator: collections.abc.Callable[
            [collections.abc.Set[ComponentId]], str
        ],
        sibling_groups: (
            collections.abc.Mapping[ComponentId, frozenset[ComponentId]] | None
        ) = None,
    ) -> None:
        """Initialize this instance.

        Args:
            component_ids: The components to track.
            formula_generator: A function that returns the power formula for a
                subset of the components, for example the component graph's
                `battery_formula`.
            sibling_groups: The group of components that each component is
                controlled with, for example the batteries behind one inverter.  A
                component counts as working when a component of its group is
                working, because its power then flows through a controlled
                component.
        """
        self._component_ids = frozenset(component_ids)
        self._formula_generator = formula_generator
        self._sibling_groups = sibling_groups or {}
        self._lock = asyncio.Lock()

        self._unreachable_ids: frozenset[ComponentId] = frozenset()
        """The components whose power is being tracked."""

        self._formula_str: str | None = None
        """The formula string of the running formula, used to stop it."""

        self._forward_task: asyncio.Task[None] | None = None
        self._latest: Sample[Power] | None = None
        self._updates = Broadcast[Sample[Power]](name="unreachable-power-updates")
        self._updates_sender = self._updates.new_sender()

        self._pool: "FormulaPool | None" = None
        """A formula pool owned by this tracker for its formulas.

        Created lazily on the first subscription, so trackers that never have
        unreachable components pay nothing.
        """

    @property
    def power(self) -> Power | None:
        """The latest power measured on the components that aren't working.

        `None` while nothing is tracked (all components are working, none of them
        is, or their formula can't be generated), before the first value arrives,
        and when the formula sends a sample without a value.
        """
        return self._latest.value if self._latest is not None else None

    def new_receiver(self) -> Receiver[Sample[Power]]:
        """Return a receiver for the measured power of the unreachable components.

        Returns:
            A receiver that gets each new value of the running formula.
        """
        return self._updates.new_receiver()

    async def update(self, working_ids: collections.abc.Set[ComponentId]) -> None:
        """Set the working components, and track the power of the unreachable ones.

        This is a no-op if the set of unreachable components hasn't changed since the
        last call, unless `stop` was called in between.  Nothing is tracked while no
        component is working, because then there is nothing to distribute power to,
        and no bounds to shift.

        Args:
            working_ids: The components that are working.
        """
        async with self._lock:
            working = set(working_ids).union(
                *(self._sibling_groups.get(cid, ()) for cid in working_ids)
            )
            unreachable = (
                frozenset()
                if self._component_ids.isdisjoint(working)
                else self._component_ids - working
            )
            if unreachable == self._unreachable_ids:
                return
            if self._unreachable_ids:
                _logger.debug(
                    "Unreachable subset of %s changed from %s to %s.",
                    sorted(self._component_ids),
                    sorted(self._unreachable_ids),
                    sorted(unreachable),
                )
            await self._stop_formula()
            self._unreachable_ids = unreachable
            if not unreachable:
                return

            try:
                formula = self._formula_generator(unreachable)
            except FormulaGenerationError:
                _logger.exception(
                    "Can't measure the power of unreachable components %s; leaving "
                    "it out until the set of unreachable components changes.",
                    sorted(unreachable),
                )
                return

            if self._pool is None:
                # Imported here, and the pool created lazily, to avoid an import
                # cycle with the data pipeline (which imports the power distributor).
                #
                # pylint: disable-next=import-outside-toplevel,cyclic-import
                from .._data_pipeline import _new_formula_pool

                self._pool = _new_formula_pool("unreachable-component-power")

            _logger.debug(
                "Subscribing to the power of unreachable components %s with "
                "formula: %s",
                sorted(unreachable),
                formula,
            )
            receiver = self._pool.from_string(
                formula, Metric.AC_POWER_ACTIVE
            ).new_receiver()
            self._formula_str = formula
            self._forward_task = asyncio.create_task(self._forward(receiver))

    async def stop(self) -> None:
        """Stop the running formula and the owned formula pool.

        The tracker can be updated again afterwards, which starts tracking from
        scratch.
        """
        async with self._lock:
            await self._stop_formula()
            self._unreachable_ids = frozenset()
            if self._pool is not None:
                await self._pool.stop()
                self._pool = None

    async def _forward(self, receiver: Receiver[Sample[Quantity]]) -> None:
        async for sample in receiver:
            self._latest = _quantity_sample_to_power(sample)
            await self._updates_sender.send(self._latest)

    async def _stop_formula(self) -> None:
        self._latest = None
        task, self._forward_task = self._forward_task, None
        formula, self._formula_str = self._formula_str, None
        try:
            if task is not None:
                await cancel_and_await(task)
        finally:
            # try/finally so the formula is stopped even if stopping the task fails
            # — otherwise the formula would leak.
            if formula is not None and self._pool is not None:
                await self._pool.stop_string_formula(formula, Metric.AC_POWER_ACTIVE)
