# License: MIT
# Copyright  © 2023 Frequenz Energy-as-a-Service GmbH

"""Formula generator from component graph for CHP Power."""


import logging
from collections import abc

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import Chp, Meter
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Power

from ....microgrid import connection_manager
from ...formula_engine import FormulaEngine
from ._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
    FormulaGenerationError,
    FormulaGenerator,
)

_logger = logging.getLogger(__name__)


class CHPPowerFormula(FormulaGenerator[Power]):
    """Formula generator for CHP Power."""

    def generate(  # noqa: DOC502 (FormulaGenerationError is raised indirectly by _get_chp_meters)
        self,
    ) -> FormulaEngine[Power]:
        """Make a formula for the cumulative CHP power of a microgrid.

        The calculation is performed by adding the active power measurements from
        dedicated meters attached to CHPs.

        Returns:
            A formula engine that will calculate cumulative CHP power values.

        Raises:
            FormulaGenerationError: If there's no dedicated meter attached to every CHP.

        """
        builder = self._get_builder(
            "chp-power", Metric.AC_ACTIVE_POWER, Power.from_watts
        )

        chp_meter_ids = self._get_chp_meters()
        if not chp_meter_ids:
            _logger.warning("No CHPs found in the component graph.")
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        for idx, chp_meter_id in enumerate(chp_meter_ids):
            if idx > 0:
                builder.push_oper("+")
            builder.push_component_metric(chp_meter_id, nones_are_zeros=False)

        return builder.build()

    def _get_chp_meters(self) -> abc.Set[ComponentId]:
        """Get the meter IDs of the CHPs from the component graph.

        Returns:
            A set of meter IDs of the CHPs in the component graph. If no CHPs are
                found, None is returned.

        Raises:
            FormulaGenerationError: If there's no dedicated meter attached to every CHP.
        """
        component_graph = connection_manager.get().component_graph
        chps = component_graph.components(matching_types=Chp)

        chp_meters: set[ComponentId] = set()
        for chp in chps:
            predecessors = component_graph.predecessors(chp.id)
            if len(predecessors) != 1:
                raise FormulaGenerationError(
                    f"CHP {chp.id} has {len(predecessors)} predecessors. "
                    " Expected exactly one."
                )
            meter = next(iter(predecessors))
            if not isinstance(meter, Meter):
                raise FormulaGenerationError(
                    f"CHP {chp.id} has a predecessor of category "
                    f"{meter.category}. Expected ComponentCategory.METER."
                )
            meter_successors = component_graph.successors(meter.id)
            if not all(successor in chps for successor in meter_successors):
                raise FormulaGenerationError(
                    f"Meter {meter.id} connected to CHP {chp.id}"
                    "has non-chp successors."
                )
            chp_meters.add(meter.id)
        return chp_meters
