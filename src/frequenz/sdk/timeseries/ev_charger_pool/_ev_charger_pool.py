# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of EV Chargers."""

from frequenz.quantities import Current, Power
from typing_extensions import override

from ...microgrid import connection_manager
from ...timeseries import Bounds
from ..component_pool import ComponentPool
from ..formulas import Formula, Formula3Phase
from ._ev_charger_pool_reference_store import EVChargerPoolReferenceStore
from ._result_types import EVChargerPoolReport


class EVChargerPoolError(Exception):
    """An error that occurred in any of the EVChargerPool methods."""


class EVChargerPool(ComponentPool[EVChargerPoolReferenceStore, EVChargerPoolReport]):
    """An interface for interaction with pools of EV Chargers.

    Provides:
      - Aggregate [`power`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.power]
        and
        [`current_per_phase`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.current_per_phase]
        measurements of the EV Chargers in the pool.
    """

    @override
    async def propose_power(
        self,
        power: Power | None,
        bounds: Bounds[Power | None] = Bounds(None, None),
    ) -> None:
        """Send a proposal to the power manager for the pool's set of EV chargers.

        This proposal is for the maximum power that can be set for the EV chargers in
        the pool.  The actual consumption might be lower based on the number of phases
        an EV is drawing power from, and its current state of charge.

        Details on how the power manager handles proposals can be found in the
        [Microgrid][frequenz.sdk.microgrid--setting-power] documentation.

        Args:
            power: The power to propose for the EV chargers in the pool.  If `None`,
                this proposal will not have any effect on the target power, unless
                bounds are specified.  When specified without bounds, bounds for lower
                priority actors will be shifted by this power.  If both are `None`, it
                is equivalent to not having a proposal or withdrawing a previous one.
            bounds: The power bounds for the proposal. When specified, these bounds will
                limit the bounds for lower priority actors.

        Raises:
            EVChargerPoolError: If a discharge power for EV chargers is requested.
        """
        if power is not None and power < Power.zero():
            raise EVChargerPoolError(
                "Discharging from EV chargers is currently not supported."
            )
        await super().propose_power(power, bounds=bounds)

    @property
    def current_per_phase(self) -> Formula3Phase[Current]:
        """Fetch the total current for the EV Chargers in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula to calculate EV Charger current is not already running, it
        will be started.

        A receiver from the formula can be created using the `new_receiver`
        method.

        Returns:
            A Formula that will calculate and stream the total current of all EV
                Chargers.
        """
        return self._pool_ref_store.formula_pool.from_current_3_phase_formula(
            "ev_charger_total_current",
            connection_manager.get().component_graph.ev_charger_formula(
                self._pool_ref_store.component_ids
            ),
        )

    @property
    @override
    def power(self) -> Formula[Power]:
        """Fetch the total power for the EV Chargers in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula to calculate EV Charger power is not already running, it
        will be started.

        A receiver from the formula can be created using the `new_receiver`
        method.

        Returns:
            A Formula that will calculate and stream the total power of all EV
                Chargers.
        """
        return self._pool_ref_store.formula_pool.from_power_formula(
            "ev_charger_power",
            connection_manager.get().component_graph.ev_charger_formula(
                self._pool_ref_store.component_ids
            ),
        )
