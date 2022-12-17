# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Power."""

import asyncio

from frequenz.channels import Receiver

from .....sdk import microgrid
from ....microgrid.component import ComponentCategory, ComponentMetricId, InverterType
from ... import Sample
from .._formula_engine import FormulaEngine
from ._formula_generator import (
    ComponentNotFound,
    FormulaGenerationError,
    FormulaGenerator,
)


class _ActiveBatteryReceiver(Receiver[Sample]):
    """Returns a Sample from a battery, only if the attached inverter is active."""

    def __init__(self, inv_recv: Receiver[Sample], bat_recv: Receiver[Sample]):
        self._inv_recv = inv_recv
        self._bat_recv = bat_recv

    async def ready(self) -> None:
        """Wait until the next Sample is ready."""
        await asyncio.gather(self._inv_recv.ready(), self._bat_recv.ready())

    def consume(self) -> Sample:
        """Return the next Sample.

        Returns:
            the next Sample.
        """
        inv = self._inv_recv.consume()
        bat = self._bat_recv.consume()
        if inv.value is None:
            return inv
        return bat


class BatterySoCFormula(FormulaGenerator):
    """Creates a formula engine from the component graph for calculating battery soc."""

    async def generate(
        self,
    ) -> FormulaEngine:
        """Make a formula for the average battery soc of a microgrid.

        If there's no data coming from an inverter or a battery, the corresponding
        battery will be excluded from the calculation.

        Returns:
            A formula engine that will calculate average battery soc values.

        Raises:
            ComponentNotFound: if there are no batteries in the component graph, or if
                they don't have an inverter as a predecessor.
            FormulaGenerationError: If a battery has a non-inverter predecessor
                in the component graph.
        """
        builder = self._get_builder("soc", ComponentMetricId.ACTIVE_POWER)
        component_graph = microgrid.get().component_graph
        inv_bat_pairs = {
            comp: component_graph.successors(comp.component_id)
            for comp in component_graph.components()
            if comp.category == ComponentCategory.INVERTER
            and comp.type == InverterType.BATTERY
        }

        if not inv_bat_pairs:
            raise ComponentNotFound(
                "Unable to find any battery inverters in the component graph."
            )

        soc_streams = []
        for inv, bats in inv_bat_pairs.items():
            bat = list(bats)[0]
            if len(bats) != 1:
                raise FormulaGenerationError(
                    f"Expected exactly one battery for inverter {inv}, got {bats}"
                )

            # pylint: disable=protected-access
            soc_recv = _ActiveBatteryReceiver(
                await builder._get_resampled_receiver(
                    inv.component_id, ComponentMetricId.ACTIVE_POWER
                ),
                await builder._get_resampled_receiver(
                    bat.component_id, ComponentMetricId.SOC
                ),
            )
            # pylint: enable=protected-access

            soc_streams.append((f"{bat.component_id}", soc_recv, False))

        builder.push_average(soc_streams)

        return builder.build()
