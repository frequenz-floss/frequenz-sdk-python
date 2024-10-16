# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for Grid Power."""

import logging

from frequenz.client.microgrid import ComponentMetricId
from frequenz.quantities import Power

from .._formula_engine import FormulaEngine
from ._formula_generator import NON_EXISTING_COMPONENT_ID, FormulaGenerator

_logger = logging.getLogger(__name__)


class EVChargerPowerFormula(FormulaGenerator[Power]):
    """Create a formula engine from the component graph for calculating grid power."""

    def generate(self) -> FormulaEngine[Power]:
        """Generate a formula for calculating total EV power for given component ids.

        Returns:
            A formula engine that calculates total EV Charger power values.
        """
        builder = self._get_builder(
            "ev-power", ComponentMetricId.ACTIVE_POWER, Power.from_watts
        )

        component_ids = self._config.component_ids
        if not component_ids:
            _logger.warning(
                "No EV Charger component IDs specified. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no EV Chargers, we have to send 0 values as the same
            # frequency as the other streams. So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        for idx, component_id in enumerate(component_ids):
            if idx > 0:
                builder.push_oper("+")
            builder.push_component_metric(component_id, nones_are_zeros=True)

        return builder.build()
