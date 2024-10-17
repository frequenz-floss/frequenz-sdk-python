# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph."""

from frequenz.client.microgrid import ComponentCategory, ComponentMetricId
from frequenz.quantities import Power, ReactivePower

from ....microgrid import connection_manager
from ..._base_types import QuantityT
from .._formula_engine import FormulaEngine
from .._resampled_formula_builder import ResampledFormulaBuilder
from ._formula_generator import FormulaGenerator


class SimpleFormulaBase(FormulaGenerator[QuantityT]):
    """Base class for simple formula generators."""

    def _generate(
        self, builder: ResampledFormulaBuilder[QuantityT]
    ) -> FormulaEngine[QuantityT]:
        """Generate formula for calculating quantity from the component graph.

        Args:
            builder: The builder to use for generating the formula.

        Returns:
            A formula engine that will calculate the quantity.

        Raises:
            RuntimeError: If components ids in config are not specified
              or component graph does not contain all specified components.
        """
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


class SimplePowerFormula(SimpleFormulaBase[Power]):
    """Formula generator from component graph for calculating sum of Power."""

    def generate(  # noqa: DOC502
        # * ComponentNotFound is raised indirectly by _get_grid_component()
        # * RuntimeError is raised indirectly by connection_manager.get()
        self,
    ) -> FormulaEngine[Power]:
        """Generate formula for calculating sum of power from the component graph.

        Returns:
            A formula engine that will calculate the power.

        Raises:
            RuntimeError: If components ids in config are not specified
                or component graph does not contain all specified components.
        """
        builder = self._get_builder(
            "simple_power_formula",
            ComponentMetricId.ACTIVE_POWER,
            Power.from_watts,
        )
        return self._generate(builder)


class SimpleReactivePowerFormula(SimpleFormulaBase[ReactivePower]):
    """Formula generator from component graph for calculating sum of reactive power."""

    def generate(  # noqa: DOC502
        # * ComponentNotFound is raised indirectly by _get_grid_component()
        # * RuntimeError is raised indirectly by connection_manager.get()
        self,
    ) -> FormulaEngine[ReactivePower]:
        """Generate formula for calculating sum of reactive power from the component graph.

        Returns:
            A formula engine that will calculate the power.

        Raises:
            RuntimeError: If components ids in config are not specified
                or component graph does not contain all specified components.
        """
        builder = self._get_builder(
            "simple_reactive_power_formula",
            ComponentMetricId.REACTIVE_POWER,
            ReactivePower.from_volt_amperes_reactive,
        )
        return self._generate(builder)
