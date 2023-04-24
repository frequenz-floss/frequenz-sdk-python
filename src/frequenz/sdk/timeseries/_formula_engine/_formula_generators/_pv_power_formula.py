# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator for PV Power, from the component graph."""

import logging

from ....microgrid import connection_manager
from ....microgrid.component import ComponentCategory, ComponentMetricId, InverterType
from .._formula_engine import FormulaEngine
from ._formula_generator import NON_EXISTING_COMPONENT_ID, FormulaGenerator

_logger = logging.getLogger(__name__)


class PVPowerFormula(FormulaGenerator):
    """Creates a formula engine for calculating the PV power production."""

    def generate(self) -> FormulaEngine:
        """Make a formula for the PV power production of a microgrid.

        Returns:
            A formula engine that will calculate PV power production values.

        Raises:
            ComponentNotFound: if there are no PV inverters in the component graph.
        """
        builder = self._get_builder("pv-power", ComponentMetricId.ACTIVE_POWER)

        component_graph = connection_manager.get().component_graph
        pv_inverters = list(
            comp
            for comp in component_graph.components()
            if comp.category == ComponentCategory.INVERTER
            and comp.type == InverterType.SOLAR
        )

        if not pv_inverters:
            _logger.warning(
                "Unable to find any PV inverters in the component graph. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no PV inverters, we have to send 0 values as the same
            # frequency as the other streams.  So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        for idx, comp in enumerate(pv_inverters):
            if idx > 0:
                builder.push_oper("+")

            builder.push_component_metric(comp.component_id, nones_are_zeros=True)

        return builder.build()
