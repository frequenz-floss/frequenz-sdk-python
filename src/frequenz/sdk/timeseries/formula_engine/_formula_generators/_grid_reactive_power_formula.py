# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Reactive Power."""


from frequenz.client.microgrid import ComponentMetricId
from frequenz.quantities import Power

from .._formula_engine import FormulaEngine
from ._grid_power_formula_base import GridPowerFormulaBase


class GridReactivePowerFormula(GridPowerFormulaBase):
    """Creates a formula engine from the component graph for calculating grid reactive power."""

    def generate(  # noqa: DOC502
        # * ComponentNotFound is raised indirectly by _get_grid_component_successors
        self,
    ) -> FormulaEngine[Power]:
        """Generate a formula for calculating grid reactive power from the component graph.

        Returns:
            A formula engine that will calculate grid reactive power values.

        Raises:
            ComponentNotFound: when the component graph doesn't have a `GRID` component.
        """
        builder = self._get_builder(
            f"grid-{ComponentMetricId.REACTIVE_POWER.value}",
            ComponentMetricId.REACTIVE_POWER,
            Power.from_watts,
        )
        return self._generate(builder)
