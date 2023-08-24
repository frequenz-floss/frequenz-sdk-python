# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Producer Power."""

import logging

from ....microgrid import connection_manager
from ....microgrid.component import ComponentCategory, ComponentMetricId
from ..._quantities import Power
from .._formula_engine import FormulaEngine
from ._formula_generator import NON_EXISTING_COMPONENT_ID, FormulaGenerator

_logger = logging.getLogger(__name__)


class ProducerPowerFormula(FormulaGenerator[Power]):
    """Formula generator from component graph for calculating the Producer Power.

    The producer power is calculated by summing up the power of all power producers,
    which are CHP and PV.
    """

    def generate(self) -> FormulaEngine[Power]:
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

        for idx, component in enumerate(producer_components):
            if idx > 0:
                builder.push_oper("+")

            builder.push_component_metric(
                component.component_id,
                nones_are_zeros=component.category != ComponentCategory.METER,
            )

        return builder.build()
