# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Consumer Power."""

import logging

from frequenz.client.microgrid import Component, ComponentCategory, ComponentMetricId
from frequenz.quantities import Power

from ....microgrid import connection_manager
from .._formula_engine import FormulaEngine
from .._resampled_formula_builder import ResampledFormulaBuilder
from ._fallback_formula_metric_fetcher import FallbackFormulaMetricFetcher
from ._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
    ComponentNotFound,
    FormulaGenerator,
    FormulaGeneratorConfig,
)
from ._simple_formula import SimplePowerFormula

_logger = logging.getLogger(__name__)


class ConsumerPowerFormula(FormulaGenerator[Power]):
    """Formula generator from component graph for calculating the Consumer Power.

    The consumer power is calculated by summing up the power of all components that
    are not part of a battery, CHP, PV or EV charger chain.
    """

    def _are_grid_meters(self, grid_successors: set[Component]) -> bool:
        """Check if the grid successors are grid meters.

        Args:
            grid_successors: The successors of the grid component.

        Returns:
            True if the provided components are grid meters, False otherwise.
        """
        component_graph = connection_manager.get().component_graph
        return all(
            successor.category == ComponentCategory.METER
            and not component_graph.is_battery_chain(successor)
            and not component_graph.is_chp_chain(successor)
            and not component_graph.is_pv_chain(successor)
            and not component_graph.is_ev_charger_chain(successor)
            for successor in grid_successors
        )

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

        grid_successors = self._get_grid_component_successors()

        if not grid_successors:
            raise ComponentNotFound("No components found in the component graph.")

        if self._are_grid_meters(grid_successors):
            return self._gen_with_grid_meter(builder, grid_successors)

        return self._gen_without_grid_meter(builder, self._get_grid_component())

    def _gen_with_grid_meter(
        self,
        builder: ResampledFormulaBuilder[Power],
        grid_meters: set[Component],
    ) -> FormulaEngine[Power]:
        """Generate formula for calculating consumer power with grid meter.

        Args:
            builder: The formula engine builder.
            grid_meters: The grid meter component.

        Returns:
            A formula engine that will calculate the consumer power.
        """
        assert grid_meters
        component_graph = connection_manager.get().component_graph

        def non_consumer_component(component: Component) -> bool:
            """
            Check if a component is not a consumer component.

            Args:
                component: The component to check.

            Returns:
                True if the component is not a consumer component, False otherwise.
            """
            # If the component graph supports additional types of grid successors in the
            # future, additional checks need to be added here.
            return (
                component_graph.is_battery_chain(component)
                or component_graph.is_chp_chain(component)
                or component_graph.is_pv_chain(component)
                or component_graph.is_ev_charger_chain(component)
            )

        # join all non consumer components reachable from the different grid meters
        non_consumer_components: set[Component] = set()
        for grid_meter in grid_meters:
            non_consumer_components = non_consumer_components.union(
                component_graph.dfs(grid_meter, set(), non_consumer_component)
            )

        # push all grid meters
        for idx, grid_meter in enumerate(grid_meters):
            if idx > 0:
                builder.push_oper("+")
            builder.push_component_metric(
                grid_meter.component_id, nones_are_zeros=False
            )

        if self._config.allow_fallback:
            fallbacks = self._get_fallback_formulas(non_consumer_components)

            for idx, (primary_component, fallback_formula) in enumerate(
                fallbacks.items()
            ):
                builder.push_oper("-")

                # should only be the case if the component is not a meter
                builder.push_component_metric(
                    primary_component.component_id,
                    nones_are_zeros=(
                        primary_component.category != ComponentCategory.METER
                    ),
                    fallback=fallback_formula,
                )
        else:
            # push all non consumer components and subtract them from the grid meters
            for component in non_consumer_components:
                builder.push_oper("-")
                builder.push_component_metric(
                    component.component_id,
                    nones_are_zeros=component.category != ComponentCategory.METER,
                )

        return builder.build()

    def _gen_without_grid_meter(
        self,
        builder: ResampledFormulaBuilder[Power],
        grid: Component,
    ) -> FormulaEngine[Power]:
        """Generate formula for calculating consumer power without a grid meter.

        Args:
            builder: The formula engine builder.
            grid: The grid component.

        Returns:
            A formula engine that will calculate the consumer power.
        """

        def consumer_component(component: Component) -> bool:
            """
            Check if a component is a consumer component.

            Args:
                component: The component to check.

            Returns:
                True if the component is a consumer component, False otherwise.
            """
            # If the component graph supports additional types of grid successors in the
            # future, additional checks need to be added here.
            return (
                component.category
                in {ComponentCategory.METER, ComponentCategory.INVERTER}
                and not component_graph.is_battery_chain(component)
                and not component_graph.is_chp_chain(component)
                and not component_graph.is_pv_chain(component)
                and not component_graph.is_ev_charger_chain(component)
            )

        component_graph = connection_manager.get().component_graph
        consumer_components = component_graph.dfs(grid, set(), consumer_component)

        if not consumer_components:
            _logger.warning(
                "Unable to find any consumers in the component graph. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no consumer components, we have to send 0 values at the same
            # frequency as the other streams.  So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        if self._config.allow_fallback:
            fallbacks = self._get_fallback_formulas(consumer_components)

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
            for idx, component in enumerate(consumer_components):
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

        The primary component is the one that will be used to calculate the consumer power.
        However, if it is not available, the fallback formula will be used instead.
        Fallback formulas calculate the consumer power using the fallback components.
        Fallback formulas are wrapped in `FallbackFormulaMetricFetcher` to allow
        for lazy initialization.

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
