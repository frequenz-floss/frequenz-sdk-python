# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Producer Power."""

from __future__ import annotations

from ....microgrid import connection_manager
from ....microgrid.component import ComponentCategory, ComponentMetricId
from ..._quantities import Power
from .._formula_engine import FormulaEngine
from ._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
    ComponentNotFound,
    FormulaGenerator,
)


class ProducerPowerFormula(FormulaGenerator[Power]):
    """Formula generator from component graph for calculating the Producer Power.

    The producer power is calculated by summing up the power of all power producers,
    which are CHP and PV.
    """

    def generate(self) -> FormulaEngine[Power]:
        """Generate formula for calculating producer power from the component graph.

        Returns:
            A formula engine that will calculate the producer power.

        Raises:
            ComponentNotFound: If the component graph does not contain a producer power
                component.
            RuntimeError: If the grid component has a single successor that is not a
                meter.
        """
        builder = self._get_builder(
            "producer_power", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )
        component_graph = connection_manager.get().component_graph
        grid_component = next(
            iter(
                component_graph.components(component_category={ComponentCategory.GRID})
            ),
            None,
        )

        if grid_component is None:
            raise ComponentNotFound("Grid component not found in the component graph.")

        grid_successors = component_graph.successors(grid_component.component_id)
        if not grid_successors:
            raise ComponentNotFound("No components found in the component graph.")

        if len(grid_successors) == 1:
            grid_meter = next(iter(grid_successors))
            if grid_meter.category != ComponentCategory.METER:
                raise RuntimeError(
                    "Only grid successor in the component graph is not a meter."
                )
            grid_successors = component_graph.successors(grid_meter.component_id)

        first_iteration = True
        for successor in iter(grid_successors):
            # if in the future we support additional producers, we need to add them here
            if component_graph.is_chp_chain(successor) or component_graph.is_pv_chain(
                successor
            ):
                if not first_iteration:
                    builder.push_oper("+")

                first_iteration = False

                builder.push_component_metric(
                    successor.component_id,
                    nones_are_zeros=successor.category != ComponentCategory.METER,
                )

        if first_iteration:
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )

        return builder.build()
