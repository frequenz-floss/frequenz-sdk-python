# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of steam boilers."""

from frequenz.quantities import Power
from typing_extensions import override

from ...microgrid import connection_manager
from ...timeseries import Bounds
from ..component_pool import ComponentPool
from ..formulas import Formula
from ._result_types import SteamBoilerPoolReport
from ._steam_boiler_pool_reference_store import SteamBoilerPoolReferenceStore


class SteamBoilerPoolError(Exception):
    """An error that occurred in any of the SteamBoilerPool methods."""


class SteamBoilerPool(
    ComponentPool[SteamBoilerPoolReferenceStore, SteamBoilerPoolReport]
):
    """An interface for interaction with pools of steam boilers.

    Provides:
      - Aggregate
        [`power`][frequenz.sdk.timeseries.steam_boiler_pool.SteamBoilerPool.power]
        measurements of the steam boilers in the pool.
    """

    @override
    async def propose_power(
        self,
        power: Power | None,
        bounds: Bounds[Power | None] = Bounds(None, None),
    ) -> None:
        """Send a proposal to the power manager for the pool's set of steam boilers.

        Steam boilers are controllable loads, so this proposal is for the power the
        boilers in the pool should consume.

        Details on how the power manager handles proposals can be found in the
        [Microgrid][frequenz.sdk.microgrid--setting-power] documentation.

        Args:
            power: The power to propose for the steam boilers in the pool.  If `None`,
                this proposal will not have any effect on the target power, unless
                bounds are specified.  When specified without bounds, bounds for lower
                priority actors will be shifted by this power.  If both are `None`, it
                is equivalent to not having a proposal or withdrawing a previous one.
            bounds: The power bounds for the proposal. When specified, these bounds will
                limit the bounds for lower priority actors.

        Raises:
            SteamBoilerPoolError: If a discharge power for steam boilers is requested.
        """
        if power is not None and power < Power.zero():
            raise SteamBoilerPoolError(
                "Discharging from steam boilers is not supported."
            )
        await super().propose_power(power, bounds=bounds)

    @property
    @override
    def power(self) -> Formula[Power]:
        """Fetch the total power for the steam boilers in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula to calculate steam boiler power is not already running, it
        will be started.

        A receiver from the formula can be created using the `new_receiver`
        method.

        Returns:
            A Formula that will calculate and stream the total power of all steam boilers.
        """
        return self._pool_ref_store.formula_pool.from_power_formula(
            "steam_boiler_power",
            connection_manager.get().component_graph.steam_boiler_formula(
                self._pool_ref_store.component_ids
            ),
        )
