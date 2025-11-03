# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for 3-phase Grid Current."""

from frequenz.client.microgrid.component import Component, EvCharger, Inverter, Meter
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Current

from .._formula_engine import FormulaEngine, FormulaEngine3Phase
from ._formula_generator import FormulaGenerator


class GridCurrentFormula(FormulaGenerator[Current]):
    """Create a formula engine from the component graph for calculating grid current."""

    def generate(  # noqa: DOC502
        # ComponentNotFound is raised indirectly by _get_grid_component_successors
        self,
    ) -> FormulaEngine3Phase[Current]:
        """Generate a formula for calculating grid current from the component graph.

        Returns:
            A formula engine that will calculate 3-phase grid current values.

        Raises:
            ComponentNotFound: when the component graph doesn't have a `GRID` component.
        """
        grid_successors = self._get_grid_component_successors()

        return FormulaEngine3Phase(
            "grid-current",
            Current.from_amperes,
            (
                self._gen_phase_formula(grid_successors, Metric.AC_CURRENT_PHASE_1),
                self._gen_phase_formula(grid_successors, Metric.AC_CURRENT_PHASE_2),
                self._gen_phase_formula(grid_successors, Metric.AC_CURRENT_PHASE_3),
            ),
        )

    def _gen_phase_formula(
        self,
        grid_successors: set[Component],
        metric: Metric,
    ) -> FormulaEngine[Current]:
        builder = self._get_builder("grid-current", metric, Current.from_amperes)

        # generate a formula that just adds values from all components that are
        # directly connected to the grid.
        for idx, comp in enumerate(grid_successors):
            # When inverters or ev chargers produce `None` samples, those
            # inverters are excluded from the calculation by treating their
            # `None` values as `0`s.
            #
            # This is not possible for Meters, so when they produce `None`
            # values, those values get propagated as the output.
            match comp:
                case Inverter() | EvCharger():
                    nones_are_zeros = True
                case Meter():
                    nones_are_zeros = False
                case _:
                    continue

            if idx > 0:
                builder.push_oper("+")

            builder.push_component_metric(comp.id, nones_are_zeros=nones_are_zeros)

        return builder.build()
