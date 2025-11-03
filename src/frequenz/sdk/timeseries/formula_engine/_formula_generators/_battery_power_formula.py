# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Power."""

import itertools
import logging

from frequenz.client.microgrid.component import Component, Meter
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Power

from ....microgrid import connection_manager
from ...formula_engine import FormulaEngine
from ._fallback_formula_metric_fetcher import FallbackFormulaMetricFetcher
from ._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
    ComponentNotFound,
    FormulaGenerationError,
    FormulaGenerator,
    FormulaGeneratorConfig,
)

_logger = logging.getLogger(__name__)


class BatteryPowerFormula(FormulaGenerator[Power]):
    """Creates a formula engine from the component graph for calculating battery power."""

    def generate(
        self,
    ) -> FormulaEngine[Power]:
        """Make a formula for the cumulative AC battery power of a microgrid.

        The calculation is performed by adding the Active Powers of all the inverters
        that are attached to batteries.

        If there's no data coming from an inverter, that inverter's power will be
        treated as 0.

        Returns:
            A formula engine that will calculate cumulative battery power values.

        Raises:
            ComponentNotFound: if there are no batteries in the component graph, or if
                they don't have an inverter as a predecessor.
            FormulaGenerationError: If a battery has a non-inverter predecessor
                in the component graph, or if not all batteries behind a set of
                inverters have been requested.
        """
        builder = self._get_builder(
            "battery-power", Metric.AC_ACTIVE_POWER, Power.from_watts
        )

        if not self._config.component_ids:
            _logger.warning(
                "No Battery component IDs specified. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no Batteries, we have to send 0 values as the same
            # frequency as the other streams. So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        component_ids = set(self._config.component_ids)
        component_graph = connection_manager.get().component_graph
        inv_bat_mapping: dict[Component, set[Component]] = {}

        for bat_id in component_ids:
            inverters = set(
                filter(
                    component_graph.is_battery_inverter,
                    component_graph.predecessors(bat_id),
                )
            )
            if len(inverters) == 0:
                raise ComponentNotFound(
                    "All batteries must have at least one inverter as a predecessor."
                    f"Battery ID {bat_id} has no inverter as a predecessor.",
                )

            for inverter in inverters:
                all_connected_batteries = component_graph.successors(inverter.id)
                battery_ids = set(
                    map(lambda battery: battery.id, all_connected_batteries)
                )
                if not battery_ids.issubset(component_ids):
                    raise FormulaGenerationError(
                        f"Not all batteries behind {inverter} "
                        f"are requested. Missing: {battery_ids - component_ids}"
                    )

                inv_bat_mapping[inverter] = all_connected_batteries

        if self._config.allow_fallback:
            fallbacks = self._get_fallback_formulas(inv_bat_mapping)

            for idx, (primary_component, fallback_formula) in enumerate(
                fallbacks.items()
            ):
                if idx > 0:
                    builder.push_oper("+")

                builder.push_component_metric(
                    primary_component.id,
                    nones_are_zeros=not isinstance(primary_component, Meter),
                    fallback=fallback_formula,
                )
        else:
            for idx, comp in enumerate(inv_bat_mapping.keys()):
                if idx > 0:
                    builder.push_oper("+")
                builder.push_component_metric(comp.id, nones_are_zeros=True)

        return builder.build()

    def _get_fallback_formulas(
        self, inv_bat_mapping: dict[Component, set[Component]]
    ) -> dict[Component, FallbackFormulaMetricFetcher[Power] | None]:
        """Find primary and fallback components and create fallback formulas.

        The primary component is the one that will be used to calculate the battery power.
        If it is not available, the fallback formula will be used instead.
        Fallback formulas calculate the battery power using the fallback components.
        Fallback formulas are wrapped in `FallbackFormulaMetricFetcher`.

        Args:
            inv_bat_mapping: A mapping from inverter to connected batteries.

        Returns:
            A dictionary mapping primary components to their FallbackFormulaMetricFetcher.
        """
        fallbacks = self._get_metric_fallback_components(set(inv_bat_mapping.keys()))

        fallback_formulas: dict[
            Component, FallbackFormulaMetricFetcher[Power] | None
        ] = {}
        for primary_component, fallback_components in fallbacks.items():
            if len(fallback_components) == 0:
                fallback_formulas[primary_component] = None
                continue

            battery_ids = set(
                map(
                    lambda battery: battery.id,
                    itertools.chain.from_iterable(
                        inv_bat_mapping[inv] for inv in fallback_components
                    ),
                )
            )

            generator = BatteryPowerFormula(
                f"{self._namespace}_fallback_{battery_ids}",
                self._channel_registry,
                self._resampler_subscription_sender,
                FormulaGeneratorConfig(
                    component_ids=battery_ids,
                    allow_fallback=False,
                ),
            )

            fallback_formulas[primary_component] = FallbackFormulaMetricFetcher(
                generator
            )

        return fallback_formulas
