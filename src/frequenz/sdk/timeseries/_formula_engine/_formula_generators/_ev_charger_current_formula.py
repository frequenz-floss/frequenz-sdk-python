# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for 3-phase Grid Current."""

import logging
from typing import List

from .....sdk import microgrid
from ....microgrid.component import Component, ComponentCategory, ComponentMetricId
from .._formula_engine import FormulaEngine, FormulaEngine3Phase
from ._formula_generator import NON_EXISTING_COMPONENT_ID, FormulaGenerator

logger = logging.getLogger(__name__)


class EVChargerCurrentFormula(FormulaGenerator):
    """Create a formula engine from the component graph for calculating grid current."""

    async def generate(self) -> FormulaEngine3Phase:
        """Generate a formula for calculating total ev current from the component graph.

        Returns:
            A formula engine that calculates total 3-phase ev charger current values.
        """
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
            # frequency as the other streams.  So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            builder = self._get_builder("ev-current", ComponentMetricId.ACTIVE_POWER)
            await builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            engine = builder.build()
            return FormulaEngine3Phase(
                "ev-current",
                (engine.new_receiver(), engine.new_receiver(), engine.new_receiver()),
            )

        return FormulaEngine3Phase(
            "ev-current",
            (
                (
                    await self._gen_phase_formula(
                        ev_chargers, ComponentMetricId.CURRENT_PHASE_1
                    )
                ).new_receiver(),
                (
                    await self._gen_phase_formula(
                        ev_chargers, ComponentMetricId.CURRENT_PHASE_2
                    )
                ).new_receiver(),
                (
                    await self._gen_phase_formula(
                        ev_chargers, ComponentMetricId.CURRENT_PHASE_3
                    )
                ).new_receiver(),
            ),
        )

    async def _gen_phase_formula(
        self,
        ev_chargers: List[Component],
        metric_id: ComponentMetricId,
    ) -> FormulaEngine:
        builder = self._get_builder("ev-current", metric_id)

        # generate a formula that just adds values from all ev-chargers.
        for idx, comp in enumerate(ev_chargers):
            if idx > 0:
                builder.push_oper("+")

            await builder.push_component_metric(comp.component_id, nones_are_zeros=True)

        return builder.build()
