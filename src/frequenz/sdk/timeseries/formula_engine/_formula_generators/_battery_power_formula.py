# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Power."""

import logging

from frequenz.client.microgrid import ComponentMetricId

from ....microgrid import connection_manager
from ..._quantities import Power
from ...formula_engine import FormulaEngine
from ._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
    ComponentNotFound,
    FormulaGenerationError,
    FormulaGenerator,
)

_logger = logging.getLogger(__name__)


class BatteryPowerFormula(FormulaGenerator[Power]):
    """Creates a formula engine from the component graph for calculating grid power."""

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
                in the component graph.
            FormulaGenerationError: If not all batteries behind a set of inverters
                have been requested.
        """
        builder = self._get_builder(
            "battery-power", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )
        component_ids = self._config.component_ids
        if not component_ids:
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

        component_graph = connection_manager.get().component_graph

        battery_inverters = frozenset(
            frozenset(
                filter(
                    component_graph.is_battery_inverter,
                    component_graph.predecessors(bat_id),
                )
            )
            for bat_id in component_ids
        )

        if not all(battery_inverters):
            raise ComponentNotFound(
                "All batteries must have at least one inverter as a predecessor."
            )

        all_connected_batteries = set()
        for inverters in battery_inverters:
            for inverter in inverters:
                all_connected_batteries.update(
                    component_graph.successors(inverter.component_id)
                )

        if len(all_connected_batteries) != len(component_ids):
            raise FormulaGenerationError(
                "All batteries behind a set of inverters must be requested."
            )

        builder.push_oper("(")
        builder.push_oper("(")
        # Iterate over the flattened list of inverters
        for idx, comp in enumerate(
            inverter for inverters in battery_inverters for inverter in inverters
        ):
            if idx > 0:
                builder.push_oper("+")
            builder.push_component_metric(comp.component_id, nones_are_zeros=True)

        return builder.build()
