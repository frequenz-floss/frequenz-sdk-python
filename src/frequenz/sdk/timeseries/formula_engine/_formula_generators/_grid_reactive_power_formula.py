# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Reactive Power."""


from frequenz.client.microgrid import Component, ComponentMetricId
from frequenz.quantities import ReactivePower

from .._formula_engine import FormulaEngine
from ._fallback_formula_metric_fetcher import FallbackFormulaMetricFetcher
from ._formula_generator import FormulaGeneratorConfig
from ._grid_power_formula_base import GridPowerFormulaBase
from ._simple_formula import SimpleReactivePowerFormula


class GridReactivePowerFormula(GridPowerFormulaBase[ReactivePower]):
    """Creates a formula engine from the component graph for calculating grid reactive power."""

    def generate(  # noqa: DOC502
        # * ComponentNotFound is raised indirectly by _get_grid_component_successors
        self,
    ) -> FormulaEngine[ReactivePower]:
        """Generate a formula for calculating grid reactive power from the component graph.

        Returns:
            A formula engine that will calculate grid reactive power values.

        Raises:
            ComponentNotFound: when the component graph doesn't have a `GRID` component.
        """
        builder = self._get_builder(
            "grid_reactive_power_formula",
            ComponentMetricId.REACTIVE_POWER,
            ReactivePower.from_volt_amperes_reactive,
        )
        return self._generate(builder)

    def _get_fallback_formulas(
        self, components: set[Component]
    ) -> dict[Component, FallbackFormulaMetricFetcher[ReactivePower] | None]:
        """Find primary and fallback components and create fallback formulas.

        The primary component is the one that will be used to calculate the grid reactive power.
        If it is not available, the fallback formula will be used instead.
        Fallback formulas calculate the grid power using the fallback components.
        Fallback formulas are wrapped in `FallbackFormulaMetricFetcher`.

        Args:
            components: The producer components.

        Returns:
            A dictionary mapping primary components to their FallbackFormulaMetricFetcher.
        """
        fallbacks = self._get_metric_fallback_components(components)

        fallback_formulas: dict[
            Component, FallbackFormulaMetricFetcher[ReactivePower] | None
        ] = {}

        for primary_component, fallback_components in fallbacks.items():
            if len(fallback_components) == 0:
                fallback_formulas[primary_component] = None
                continue

            fallback_ids = [c.component_id for c in fallback_components]
            generator = SimpleReactivePowerFormula(
                f"{self._namespace}_fallback_{fallback_ids}",
                self._channel_registry,
                self._resampler_subscription_sender,
                FormulaGeneratorConfig(
                    component_ids=set(fallback_ids),
                    allow_fallback=False,
                ),
            )

            fallback_formulas[primary_component] = FallbackFormulaMetricFetcher(
                generator
            )

        return fallback_formulas
