# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Power."""

import logging

from .....sdk import microgrid
from ....microgrid.component import ComponentCategory, ComponentMetricId, InverterType
from .._formula_engine import FormulaEngine
from ._formula_generator import NON_EXISTING_COMPONENT_ID, FormulaGenerator

logger = logging.getLogger(__name__)


class BatteryPowerFormula(FormulaGenerator):
    """Creates a formula engine from the component graph for calculating grid power."""

    async def generate(
        self,
    ) -> FormulaEngine:
        """Make a formula for the cumulative AC battery power of a microgrid.

        The calculation is performed by adding the Active Powers of all the inverters
        that are attached to batteries.

        If there's no data coming from an inverter, that inverter's power will be
        treated as 0.

        Returns:
            A formula engine that will calculate cumulative battery power values.

        Raises:
            ComponentNotFound: if there are no batteries in the component graph, or if
                they don't have an inverter as a predecessor.
            FormulaGenerationError: If a battery has a non-inverter predecessor
                in the component graph.
        """
        builder = self._get_builder("battery-power", ComponentMetricId.ACTIVE_POWER)
        component_graph = microgrid.get().component_graph
        battery_inverters = list(
            comp
            for comp in component_graph.components()
            if comp.category == ComponentCategory.INVERTER
            and comp.type == InverterType.BATTERY
        )

        if not battery_inverters:
            logger.warning(
                "Unable to find any battery inverters in the component graph. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no battery inverters, we have to send 0 values as the same
            # frequency as the other streams.  So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            await builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        for idx, comp in enumerate(battery_inverters):
            if idx > 0:
                builder.push_oper("+")
            await builder.push_component_metric(comp.component_id, nones_are_zeros=True)

        return builder.build()
