# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Power."""

from ....microgrid import connection_manager
from ....microgrid.component import ComponentCategory, ComponentMetricId
from .._formula_engine import FormulaEngine
from ._formula_generator import ComponentNotFound, FormulaGenerator


class GridPowerFormula(FormulaGenerator):
    """Creates a formula engine from the component graph for calculating grid power."""

    def generate(
        self,
    ) -> FormulaEngine:
        """Generate a formula for calculating grid power from the component graph.

        Returns:
            A formula engine that will calculate grid power values.

        Raises:
            ComponentNotFound: when the component graph doesn't have a `GRID` component.
        """
        builder = self._get_builder("grid-power", ComponentMetricId.ACTIVE_POWER)
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

        # generate a formula that just adds values from all commponents that are
        # directly connected to the grid.
        for idx, comp in enumerate(grid_successors):
            if idx > 0:
                builder.push_oper("+")

            # Ensure the device has an `ACTIVE_POWER` metric.  When inverters
            # produce `None` samples, those inverters are excluded from the
            # calculation by treating their `None` values as `0`s.
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
