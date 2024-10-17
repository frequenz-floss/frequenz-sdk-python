# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Producer Power."""

import logging
from typing import Callable

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
from ._simple_formula import SimplePowerFormula

_logger = logging.getLogger(__name__)


class ProducerPowerFormula(FormulaGenerator[Power]):
    """Formula generator from component graph for calculating the Producer Power.

    The producer power is calculated by summing up the power of all power producers,
    which are CHP and PV.
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
            "producer_power", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )

        component_graph = connection_manager.get().component_graph
        # if in the future we support additional producers, we need to add them to the lambda
        producer_components = component_graph.dfs(
            self._get_grid_component(),
            set(),
            lambda component: component_graph.is_pv_chain(component)
            or component_graph.is_chp_chain(component),
        )

        if not producer_components:
            _logger.warning(
                "Unable to find any producer components in the component graph. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no producer components, we have to send 0 values at the same
            # frequency as the other streams.  So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        is_not_meter: Callable[[Component], bool] = (
            lambda component: component.category != ComponentCategory.METER
        )

        if self._config.allow_fallback:
            fallbacks = self._get_fallback_formulas(producer_components)

            for idx, (primary_component, fallback_formula) in enumerate(
                fallbacks.items()
            ):
                if idx > 0:
                    builder.push_oper("+")

                # should only be the case if the component is not a meter
                builder.push_component_metric(
                    primary_component.component_id,
                    nones_are_zeros=is_not_meter(primary_component),
                    fallback=fallback_formula,
                )
        else:
            for idx, component in enumerate(producer_components):
                if idx > 0:
                    builder.push_oper("+")

                builder.push_component_metric(
                    component.component_id,
                    nones_are_zeros=is_not_meter(component),
                )

        return builder.build()

    def _get_fallback_formulas(
        self, components: set[Component]
    ) -> dict[Component, FallbackFormulaMetricFetcher[Power] | None]:
        """Find primary and fallback components and create fallback formulas.

        The primary component is the one that will be used to calculate the producer power.
        However, if it is not available, the fallback formula will be used instead.
        Fallback formulas calculate the producer power using the fallback components.
        Fallback formulas are wrapped in `FallbackFormulaMetricFetcher`.

        Args:
            components: The producer components.

        Returns:
            A dictionary mapping primary components to their FallbackFormulaMetricFetcher.
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
            generator = SimplePowerFormula(
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
