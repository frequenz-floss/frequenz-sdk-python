# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph."""

from frequenz.client.microgrid import ComponentCategory, ComponentMetricId

from ....microgrid import connection_manager
from ..._quantities import Power
from .._formula_engine import FormulaEngine
from ._formula_generator import FormulaGenerator


class SimplePowerFormula(FormulaGenerator[Power]):
    """Formula generator from component graph for calculating sum of Power.

    Raises:
        RuntimeError: If no components are defined in the config or if any
            component is not found in the component graph.
    """

    def generate(  # noqa: DOC502
        # * ComponentNotFound is raised indirectly by _get_grid_component()
        # * RuntimeError is raised indirectly by connection_manager.get()
        self,
    ) -> FormulaEngine[Power]:
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
            "simple_power_formula", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )

        component_graph = connection_manager.get().component_graph
        if self._config.component_ids is None:
            raise RuntimeError("Power formula without component ids is not supported.")

        components = component_graph.components(
            component_ids=set(self._config.component_ids)
        )

        not_found_components = self._config.component_ids - {
            c.component_id for c in components
        }
        if not_found_components:
            raise RuntimeError(
                f"Unable to find {not_found_components} components in the component graph. ",
            )

        for idx, component in enumerate(components):
            if idx > 0:
                builder.push_oper("+")

            builder.push_component_metric(
                component.component_id,
                nones_are_zeros=component.category != ComponentCategory.METER,
            )

        return builder.build()
