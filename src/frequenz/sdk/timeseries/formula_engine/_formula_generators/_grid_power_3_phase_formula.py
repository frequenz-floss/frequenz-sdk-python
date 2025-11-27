# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for 3-phase Grid Power."""

from frequenz.client.microgrid.component import Component, EvCharger, Inverter, Meter
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Power

from .._formula_engine import FormulaEngine, FormulaEngine3Phase
from ._formula_generator import FormulaGenerator


class GridPower3PhaseFormula(FormulaGenerator[Power]):
    """Create a formula engine for calculating the grid 3-phase power."""

    def generate(  # noqa: DOC502
        # ComponentNotFound is raised indirectly by _get_grid_component_successors
        self,
    ) -> FormulaEngine3Phase[Power]:
        """Generate a formula for calculating grid 3-phase power.

        Raises:
            ComponentNotFound: when the component graph doesn't have a `GRID` component.

        Returns:
            A formula engine that will calculate grid 3-phase power values.
        """
        grid_successors = self._get_grid_component_successors()

        return FormulaEngine3Phase(
            "grid-power-3-phase",
            Power.from_watts,
            (
                self._gen_phase_formula(
                    grid_successors, Metric.AC_ACTIVE_POWER_PHASE_1
                ),
                self._gen_phase_formula(
                    grid_successors, Metric.AC_ACTIVE_POWER_PHASE_2
                ),
                self._gen_phase_formula(
                    grid_successors, Metric.AC_ACTIVE_POWER_PHASE_3
                ),
            ),
        )

    def _gen_phase_formula(
        self,
        grid_successors: set[Component],
        metric: Metric,
    ) -> FormulaEngine[Power]:
        """Generate a formula for calculating grid 3-phase power from the component graph.

        Generate a formula that adds values from all components that are directly
        connected to the grid.

        Args:
            grid_successors: The set of components that are directly connected to the grid.
            metric: The metric to use for the formula.

        Returns:
            A formula engine that will calculate grid 3-phase power values.
        """
        formula_builder = self._get_builder(
            "grid-power-3-phase", metric, Power.from_watts
        )

        for idx, comp in enumerate(grid_successors):
            # When inverters or EV chargers produce `None` samples, they are
            # excluded from the calculation by treating their `None` values
            # as `0`s.
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
                formula_builder.push_oper("+")

            formula_builder.push_component_metric(
                comp.id, nones_are_zeros=nones_are_zeros
            )

        return formula_builder.build()
