# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Formula generator for PV Power, from the component graph."""

from __future__ import annotations

import logging
from collections import abc

from ....microgrid import connection_manager
from ....microgrid.component import ComponentCategory, ComponentMetricId, InverterType
from ..._quantities import Power
from .._formula_engine import FormulaEngine
from ._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
    FormulaGenerationError,
    FormulaGenerator,
    FormulaType,
)

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

        pv_meters = self._get_pv_meters()
        if not pv_meters:
            _logger.warning(
                "Unable to find any PV inverters in the component graph. "
                "Subscribing to the resampling actor with a non-existing "
                "component id, so that `0` values are sent from the formula."
            )
            # If there are no PV inverters, we have to send 0 values as the same
            # frequency as the other streams.  So we subscribe with a non-existing
            # component id, just to get a `None` message at the resampling interval.
            builder.push_component_metric(
                NON_EXISTING_COMPONENT_ID, nones_are_zeros=True
            )
            return builder.build()

        builder.push_oper("(")
        builder.push_oper("(")
        for idx, comp_id in enumerate(pv_meters):
            if idx > 0:
                builder.push_oper("+")

            builder.push_component_metric(comp_id, nones_are_zeros=True)
        builder.push_oper(")")
        if self._config.formula_type == FormulaType.PRODUCTION:
            builder.push_oper("*")
            builder.push_constant(-1)
        builder.push_oper(")")

        if self._config.formula_type != FormulaType.PASSIVE_SIGN_CONVENTION:
            builder.push_clipper(0.0, None)

        return builder.build()

    def _get_pv_meters(self) -> abc.Set[int]:
        component_graph = connection_manager.get().component_graph

        pv_inverters = list(
            comp
            for comp in component_graph.components()
            if comp.category == ComponentCategory.INVERTER
            and comp.type == InverterType.SOLAR
        )
        pv_meters: set[int] = set()

        if not pv_inverters:
            return pv_meters

        for pv_inverter in pv_inverters:
            predecessors = component_graph.predecessors(pv_inverter.component_id)
            if len(predecessors) != 1:
                raise FormulaGenerationError(
                    "Expected exactly one predecessor for PV inverter "
                    f"{pv_inverter.component_id}, but found {len(predecessors)}."
                )
            meter = next(iter(predecessors))
            if meter.category != ComponentCategory.METER:
                raise FormulaGenerationError(
                    f"Expected predecessor of PV inverter {pv_inverter.component_id} "
                    f"to be a meter, but found {meter.category}."
                )
            meter_successors = component_graph.successors(meter.component_id)
            if len(meter_successors) != 1:
                raise FormulaGenerationError(
                    f"Expected exactly one successor for meter {meter.component_id}"
                    f", connected to PV inverter {pv_inverter.component_id}"
                    f", but found {len(meter_successors)}."
                )
            pv_meters.add(meter.component_id)
        return pv_meters
