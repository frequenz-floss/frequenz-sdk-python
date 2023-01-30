# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Power."""

import logging

from .....sdk import microgrid
from ....microgrid.component import ComponentCategory, ComponentMetricId
from .._formula_engine import FormulaEngine
from ._formula_generator import NON_EXISTING_COMPONENT_ID, FormulaGenerator

logger = logging.getLogger(__name__)


class EVChargerPowerFormula(FormulaGenerator):
    """Create a formula engine from the component graph for calculating grid power."""

    async def generate(self) -> FormulaEngine:
        """Generate a formula for calculating total EV power from the component graph.

        Returns:
            A formula engine that calculates total EV charger power values.
        """
        builder = self._get_builder("ev-power", ComponentMetricId.ACTIVE_POWER)
        component_graph = microgrid.get().component_graph
        ev_chargers = [
            comp
            for comp in component_graph.components()
            if comp.category == ComponentCategory.EV_CHARGER
        ]

        if not ev_chargers:
            logger.warning(
                "Unable to find any EV Chargers in the component graph. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no EV Chargers, we have to send 0 values as the same
            # frequency as the other streams. So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            await builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        for idx, comp in enumerate(ev_chargers):
            if idx > 0:
                builder.push_oper("+")

            await builder.push_component_metric(comp.component_id, nones_are_zeros=True)

        return builder.build()
