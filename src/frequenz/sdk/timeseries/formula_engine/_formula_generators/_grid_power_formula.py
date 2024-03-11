# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Power."""

from frequenz.client.microgrid import ComponentCategory, ComponentMetricId

from ..._quantities import Power
from .._formula_engine import FormulaEngine
from ._formula_generator import FormulaGenerator


class GridPowerFormula(FormulaGenerator[Power]):
    """Creates a formula engine from the component graph for calculating grid power."""

    def generate(  # noqa: DOC502
        # * ComponentNotFound is raised indirectly by _get_grid_component_successors
        self,
    ) -> FormulaEngine[Power]:
        """Generate a formula for calculating grid power from the component graph.

        Returns:
            A formula engine that will calculate grid power values.

        Raises:
            ComponentNotFound: when the component graph doesn't have a `GRID` component.
        """
        builder = self._get_builder(
            "grid-power", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )
        grid_successors = self._get_grid_component_successors()

        # generate a formula that just adds values from all components that are
        # directly connected to the grid.  If the requested formula type is
        # `PASSIVE_SIGN_CONVENTION`, there is nothing more to do.  If the requested
        # formula type is `PRODUCTION`, the formula output is negated, then clipped to
        # 0.  If the requested formula type is `CONSUMPTION`, the formula output is
        # already positive, so it is just clipped to 0.
        #
        # So the formulas would look like:
        #  - `PASSIVE_SIGN_CONVENTION`: `(grid-successor-1 + grid-successor-2 + ...)`
        #  - `PRODUCTION`: `max(0, -(grid-successor-1 + grid-successor-2 + ...))`
        #  - `CONSUMPTION`: `max(0, (grid-successor-1 + grid-successor-2 + ...))`
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
