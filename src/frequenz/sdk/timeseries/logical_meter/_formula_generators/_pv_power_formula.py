# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator for PV Power, from the component graph."""

from .....sdk import microgrid
from ....microgrid.component import ComponentCategory, ComponentMetricId, InverterType
from .._formula_engine import FormulaEngine
from ._formula_generator import ComponentNotFound, FormulaGenerator


class PVPowerFormula(FormulaGenerator):
    """Creates a formula engine for calculating the PV power production."""

    async def generate(self) -> FormulaEngine:
        """Make a formula for the PV power production of a microgrid.

        Returns:
            A formula engine that will calculate PV power production values.

        Raises:
            ComponentNotFound: if there are no PV inverters in the component graph.
        """
        builder = self._get_builder(ComponentMetricId.ACTIVE_POWER)

        component_graph = microgrid.get().component_graph
        pv_inverters = list(
            comp
            for comp in component_graph.components()
            if comp.category == ComponentCategory.INVERTER
            and comp.type == InverterType.SOLAR
        )

        if not pv_inverters:
            raise ComponentNotFound(
                "Unable to find any PV inverters in the component graph."
            )

        for idx, comp in enumerate(pv_inverters):
            if idx > 0:
                builder.push_oper("+")

            await builder.push_component_metric(comp.component_id, nones_are_zeros=True)

        return builder.build()
