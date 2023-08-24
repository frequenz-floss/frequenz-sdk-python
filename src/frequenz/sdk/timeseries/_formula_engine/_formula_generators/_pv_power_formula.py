# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator for PV Power, from the component graph."""

import logging

from ....microgrid import connection_manager
from ....microgrid.component import ComponentCategory, ComponentMetricId
from ..._quantities import Power
from .._formula_engine import FormulaEngine
from ._formula_generator import NON_EXISTING_COMPONENT_ID, FormulaGenerator, FormulaType

_logger = logging.getLogger(__name__)


class PVPowerFormula(FormulaGenerator[Power]):
    """Creates a formula engine for calculating the PV power production."""

    def generate(self) -> FormulaEngine[Power]:
        """Make a formula for the PV power production of a microgrid.

        Returns:
            A formula engine that will calculate PV power production values.

        Raises:
            ComponentNotFound: if there are no PV inverters in the component graph.
        """
        builder = self._get_builder(
            "pv-power", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )

        component_graph = connection_manager.get().component_graph
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
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        builder.push_oper("(")
        builder.push_oper("(")
        for idx, component in enumerate(pv_components):
            if idx > 0:
                builder.push_oper("+")

            # should only be the case if the component is not a meter
            builder.push_component_metric(
                component.component_id,
                nones_are_zeros=component.category != ComponentCategory.METER,
            )
        builder.push_oper(")")
        if self._config.formula_type == FormulaType.PRODUCTION:
            builder.push_oper("*")
            builder.push_constant(-1)
        builder.push_oper(")")

        if self._config.formula_type != FormulaType.PASSIVE_SIGN_CONVENTION:
            builder.push_clipper(0.0, None)

        return builder.build()
