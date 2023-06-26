# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Consumer Power."""

from __future__ import annotations

from collections import abc

from ....microgrid import connection_manager
from ....microgrid.component import Component, ComponentCategory, ComponentMetricId
from ..._quantities import Power
from .._formula_engine import FormulaEngine
from .._resampled_formula_builder import ResampledFormulaBuilder
from ._formula_generator import ComponentNotFound, FormulaGenerator


class ConsumerPowerFormula(FormulaGenerator[Power]):
    """Formula generator from component graph for calculating the Consumer Power.

    The consumer power is calculated by summing up the power of all components that
    are not part of a battery, CHP, PV or EV charger chain.
    """

    def generate(self) -> FormulaEngine[Power]:
        """Generate formula for calculating consumer power from the component graph.

        Returns:
            A formula engine that will calculate the consumer power.

        Raises:
            ComponentNotFound: If the component graph does not contain a consumer power
                component.
            RuntimeError: If the grid component has a single successor that is not a
                meter.
        """
        builder = self._get_builder(
            "consumer-power", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )
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
            return self._gen_with_grid_meter(builder, grid_meter)
        return self._gen_without_grid_meter(builder, grid_successors)

    def _gen_with_grid_meter(
        self,
        builder: ResampledFormulaBuilder[Power],
        grid_meter: Component,
    ) -> FormulaEngine[Power]:
        """Generate formula for calculating consumer power with grid meter.

        Args:
            builder: The formula engine builder.
            grid_meter: The grid meter component.

        Returns:
            A formula engine that will calculate the consumer power.
        """
        component_graph = connection_manager.get().component_graph
        successors = component_graph.successors(grid_meter.component_id)

        builder.push_component_metric(grid_meter.component_id, nones_are_zeros=False)

        for successor in successors:
            # If the component graph supports additional types of grid successors in the
            # future, additional checks need to be added here.
            if (
                component_graph.is_battery_chain(successor)
                or component_graph.is_chp_chain(successor)
                or component_graph.is_pv_chain(successor)
                or component_graph.is_ev_charger_chain(successor)
            ):
                builder.push_oper("-")
                nones_are_zeros = True
                if successor.category == ComponentCategory.METER:
                    nones_are_zeros = False
                builder.push_component_metric(
                    successor.component_id, nones_are_zeros=nones_are_zeros
                )

        return builder.build()

    def _gen_without_grid_meter(
        self,
        builder: ResampledFormulaBuilder[Power],
        grid_successors: abc.Iterable[Component],
    ) -> FormulaEngine[Power]:
        """Generate formula for calculating consumer power without a grid meter.

        Args:
            builder: The formula engine builder.
            grid_successors: The grid successors.

        Returns:
            A formula engine that will calculate the consumer power.
        """
        component_graph = connection_manager.get().component_graph
        is_first = True
        for successor in grid_successors:
            # If the component graph supports additional types of grid successors in the
            # future, additional checks need to be added here.
            if (
                component_graph.is_battery_chain(successor)
                or component_graph.is_chp_chain(successor)
                or component_graph.is_pv_chain(successor)
                or component_graph.is_ev_charger_chain(successor)
            ):
                continue
            if not is_first:
                builder.push_oper("+")
            is_first = False
            builder.push_component_metric(successor.component_id, nones_are_zeros=False)

        return builder.build()
