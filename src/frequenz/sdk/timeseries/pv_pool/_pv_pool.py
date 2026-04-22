# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of PV inverters."""

from frequenz.quantities import Power
from typing_extensions import override

from frequenz.sdk.microgrid import connection_manager

from ...timeseries import Bounds
from ..abstract_pool import AbstractPool
from ..formulas import Formula


class PVPoolError(Exception):
    """An error that occurred in any of the PVPool methods."""


class PVPool(AbstractPool):
    """An interface for interaction with pools of PV inverters.

    Provides:
      - Aggregate [`power`][frequenz.sdk.timeseries.pv_pool.PVPool.power]
        measurements of the PV inverters in the pool.
    """

    @override
    async def propose_power(
        self,
        power: Power | None,
        bounds: Bounds[Power | None] = Bounds(None, None),
    ) -> None:
        """Send a proposal to the power manager for the pool's set of PV inverters.

        This proposal is for the maximum power that can be set for the PV inverters in
        the pool.  The actual production might be lower.

        Power values need to follow the Passive Sign Convention (PSC). That is, positive
        values indicate charge power and negative values indicate discharge power.  Only
        discharge powers are allowed for PV inverters.

        Details on how the power manager handles proposals can be found in the
        [Microgrid][frequenz.sdk.microgrid--setting-power] documentation.

        Args:
            power: The power to propose for the PV inverters in the pool.  If `None`,
                this proposal will not have any effect on the target power, unless
                bounds are specified.  When specified without bounds, bounds for lower
                priority actors will be shifted by this power.  If both are `None`, it
                is equivalent to not having a proposal or withdrawing a previous one.
            bounds: The power bounds for the proposal.  When specified, this will limit
                the bounds for lower priority actors.

        Raises:
            PVPoolError: If a charge power for PV inverters is requested.
        """
        if power is not None and power > Power.zero():
            raise PVPoolError("Charge powers for PV inverters is not supported.")
        await super().propose_power(power, bounds=bounds)

    @property
    @override
    def power(self) -> Formula[Power]:
        """Fetch the total power for the PV Inverters in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula to calculate PV Inverter power is not already running, it
        will be started.

        A receiver from the formula can be created using the `new_receiver`
        method.

        Returns:
            A Formula that will calculate and stream the total power of all PV
                Inverters.
        """
        return self._pool_ref_store.formula_pool.from_power_formula(
            "pv_power",
            connection_manager.get().component_graph.pv_formula(
                self._pool_ref_store.component_ids
            ),
        )
