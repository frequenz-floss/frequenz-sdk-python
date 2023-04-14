# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for 3-phase Grid Current."""

from typing import Set

from ....microgrid import connection_manager
from ....microgrid.component import Component, ComponentCategory, ComponentMetricId
from .._formula_engine import FormulaEngine, FormulaEngine3Phase
from ._formula_generator import ComponentNotFound, FormulaGenerator


class GridCurrentFormula(FormulaGenerator):
    """Create a formula engine from the component graph for calculating grid current."""

    def generate(self) -> FormulaEngine3Phase:
        """Generate a formula for calculating grid current from the component graph.

        Returns:
            A formula engine that will calculate 3-phase grid current values.

        Raises:
            ComponentNotFound: when the component graph doesn't have a `GRID` component.
        """
        component_graph = connection_manager.get().component_graph
        grid_component = next(
            (
                comp
                for comp in component_graph.components()
                if comp.category == ComponentCategory.GRID
            ),
            None,
        )

        if grid_component is None:
            raise ComponentNotFound(
                "Unable to find a GRID component from the component graph."
            )

        grid_successors = component_graph.successors(grid_component.component_id)

        return FormulaEngine3Phase(
            "grid-current",
            (
                self._gen_phase_formula(
                    grid_successors, ComponentMetricId.CURRENT_PHASE_1
                ),
                self._gen_phase_formula(
                    grid_successors, ComponentMetricId.CURRENT_PHASE_2
                ),
                self._gen_phase_formula(
                    grid_successors, ComponentMetricId.CURRENT_PHASE_3
                ),
            ),
        )

    def _gen_phase_formula(
        self,
        grid_successors: Set[Component],
        metric_id: ComponentMetricId,
    ) -> FormulaEngine:
        builder = self._get_builder("grid-current", metric_id)

        # generate a formula that just adds values from all components that are
        # directly connected to the grid.
        for idx, comp in enumerate(grid_successors):
            if idx > 0:
                builder.push_oper("+")

            # When inverters or ev chargers produce `None` samples, those
            # inverters are excluded from the calculation by treating their
            # `None` values as `0`s.
            #
            # This is not possible for Meters, so when they produce `None`
            # values, those values get propagated as the output.
            if comp.category in (
                ComponentCategory.INVERTER,
                ComponentCategory.EV_CHARGER,
            ):
                nones_are_zeros = True
            elif comp.category == ComponentCategory.METER:
                nones_are_zeros = False
            else:
                continue

            builder.push_component_metric(
                comp.component_id, nones_are_zeros=nones_are_zeros
            )

        return builder.build()
