# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator for PV Power, from the component graph."""

import logging

from frequenz.client.microgrid import Component, ComponentCategory, ComponentMetricId
from frequenz.quantities import Power

from ....microgrid import connection_manager
from .._formula_engine import FormulaEngine
from ._fallback_formula_metric_fetcher import FallbackFormulaMetricFetcher
from ._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
    FormulaGenerator,
    FormulaGeneratorConfig,
)

_logger = logging.getLogger(__name__)


class PVPowerFormula(FormulaGenerator[Power]):
    """Creates a formula engine for calculating the PV power production."""

    def generate(  # noqa: DOC502
        # * ComponentNotFound is raised indirectly by _get_pv_power_components
        # * RuntimeError is also raised indirectly by _get_pv_power_components
        self,
    ) -> FormulaEngine[Power]:
        """Make a formula for the PV power production of a microgrid.

        Returns:
            A formula engine that will calculate PV power production values.

        Raises:
            ComponentNotFound: if there is a problem finding the needed components.
            RuntimeError: if the grid component has no PV inverters or meters as
                successors.
        """
        builder = self._get_builder(
            "pv-power", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )

        component_graph = connection_manager.get().component_graph
        component_ids = self._config.component_ids
        if component_ids:
            pv_components = component_graph.components(set(component_ids))
        else:
            pv_components = component_graph.dfs(
                self._get_grid_component(),
                set(),
                component_graph.is_pv_chain,
            )

        if not pv_components:
            _logger.warning(
                "Unable to find any PV components in the component graph. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no PV components, we have to send 0 values at the same
            # frequency as the other streams.  So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID,
                nones_are_zeros=True,
            )
            return builder.build()

        if self._config.allow_fallback:
            fallbacks = self._get_fallback_formulas(pv_components)

            for idx, (primary_component, fallback_formula) in enumerate(
                fallbacks.items()
            ):
                if idx > 0:
                    builder.push_oper("+")

                builder.push_component_metric(
                    primary_component.component_id,
                    nones_are_zeros=(
                        primary_component.category != ComponentCategory.METER
                    ),
                    fallback=fallback_formula,
                )
        else:
            for idx, component in enumerate(pv_components):
                if idx > 0:
                    builder.push_oper("+")

                builder.push_component_metric(
                    component.component_id,
                    nones_are_zeros=component.category != ComponentCategory.METER,
                )

        return builder.build()

    def _get_fallback_formulas(
        self, components: set[Component]
    ) -> dict[Component, FallbackFormulaMetricFetcher[Power] | None]:
        """Find primary and fallback components and create fallback formulas.

        The primary component is the one that will be used to calculate the PV power.
        If it is not available, the fallback formula will be used instead.
        Fallback formulas calculate the PV power using the fallback components.
        Fallback formulas are wrapped in `FallbackFormulaMetricFetcher`.

        Args:
            components: The PV components.

        Returns:
            A dictionary mapping primary components to their corresponding
                FallbackFormulaMetricFetcher.
        """
        fallbacks = self._get_metric_fallback_components(components)

        fallback_formulas: dict[
            Component, FallbackFormulaMetricFetcher[Power] | None
        ] = {}
        for primary_component, fallback_components in fallbacks.items():
            if len(fallback_components) == 0:
                fallback_formulas[primary_component] = None
                continue
            fallback_ids = [c.component_id for c in fallback_components]

            generator = PVPowerFormula(
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
