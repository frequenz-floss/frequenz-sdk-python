# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for 3-phase Grid Current."""

from __future__ import annotations

import logging

from ....microgrid.component import ComponentMetricId
from .._formula_engine import FormulaEngine, FormulaEngine3Phase
from ._formula_generator import NON_EXISTING_COMPONENT_ID, FormulaGenerator

logger = logging.getLogger(__name__)


class EVChargerCurrentFormula(FormulaGenerator):
    """Create a formula engine from the component graph for calculating grid current."""

    async def generate(self) -> FormulaEngine3Phase:
        """Generate a formula for calculating total ev current for given component ids.

        Returns:
            A formula engine that calculates total 3-phase ev charger current values.
        """
        component_ids = self._config.component_ids

        if not component_ids:
            logger.warning(
                "No ev charger component IDs specified. "
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
                        component_ids, ComponentMetricId.CURRENT_PHASE_1
                    )
                ).new_receiver(),
                (
                    await self._gen_phase_formula(
                        component_ids, ComponentMetricId.CURRENT_PHASE_2
                    )
                ).new_receiver(),
                (
                    await self._gen_phase_formula(
                        component_ids, ComponentMetricId.CURRENT_PHASE_3
                    )
                ).new_receiver(),
            ),
        )

    async def _gen_phase_formula(
        self,
        component_ids: set[int],
        metric_id: ComponentMetricId,
    ) -> FormulaEngine:
        builder = self._get_builder("ev-current", metric_id)

        # generate a formula that just adds values from all ev-chargers.
        for idx, component_id in enumerate(component_ids):
            if idx > 0:
                builder.push_oper("+")

            await builder.push_component_metric(component_id, nones_are_zeros=True)

        return builder.build()
