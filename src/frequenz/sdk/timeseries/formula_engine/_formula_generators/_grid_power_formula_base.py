# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Base formula generator from component graph for Grid Power."""

from abc import ABC, abstractmethod

from frequenz.client.microgrid import Component, ComponentCategory

from ..._base_types import QuantityT
from .._formula_engine import FormulaEngine
from .._resampled_formula_builder import ResampledFormulaBuilder
from ._fallback_formula_metric_fetcher import FallbackFormulaMetricFetcher
from ._formula_generator import ComponentNotFound, FormulaGenerator


class GridPowerFormulaBase(FormulaGenerator[QuantityT], ABC):
    """Base class for grid power formula generators."""

    def _generate(
        self, builder: ResampledFormulaBuilder[QuantityT]
    ) -> FormulaEngine[QuantityT]:
        """Generate a formula for calculating grid power from the component graph.

        Args:
            builder: The builder to use to create the formula.

        Returns:
            A formula engine that will calculate grid power values.

        Raises:
            ComponentNotFound: when the component graph doesn't have a `GRID` component.
        """
        grid_successors = self._get_grid_component_successors()

        components = {
            c
            for c in grid_successors
            if c.category
            in {
                ComponentCategory.INVERTER,
                ComponentCategory.EV_CHARGER,
                ComponentCategory.METER,
            }
        }

        if not components:
            raise ComponentNotFound("No grid successors found")

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
        if self._config.allow_fallback:
            fallbacks = self._get_fallback_formulas(components)

            for idx, (primary_component, fallback_formula) in enumerate(
                fallbacks.items()
            ):
                if idx > 0:
                    builder.push_oper("+")

                # should only be the case if the component is not a meter
                builder.push_component_metric(
                    primary_component.component_id,
                    nones_are_zeros=(
                        primary_component.category != ComponentCategory.METER
                    ),
                    fallback=fallback_formula,
                )
        else:
            for idx, comp in enumerate(components):
                if idx > 0:
                    builder.push_oper("+")

                builder.push_component_metric(
                    comp.component_id,
                    nones_are_zeros=(comp.category != ComponentCategory.METER),
                )

        return builder.build()

    @abstractmethod
    def _get_fallback_formulas(
        self, components: set[Component]
    ) -> dict[Component, FallbackFormulaMetricFetcher[QuantityT] | None]:
        """Find primary and fallback components and create fallback formulas.

        The primary component is the one that will be used to calculate the producer power.
        If it is not available, the fallback formula will be used instead.
        Fallback formulas calculate the grid power using the fallback components.
        Fallback formulas are wrapped in `FallbackFormulaMetricFetcher`.

        Args:
            components: The producer components.

        Returns:
            A dictionary mapping primary components to their FallbackFormulaMetricFetcher.
        """
