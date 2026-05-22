# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of steam boilers."""

from frequenz.quantities import Power
from typing_extensions import override

from ...microgrid import connection_manager
from ..component_pool import ComponentPool
from ..formulas import Formula
from ._result_types import SteamBoilerPoolReport
from ._steam_boiler_pool_reference_store import SteamBoilerPoolReferenceStore


class SteamBoilerPoolError(Exception):
    """An error that occurred in any of the SteamBoilerPool methods."""


class SteamBoilerPool(
    ComponentPool[SteamBoilerPoolReferenceStore, SteamBoilerPoolReport]
):
    """An interface for interaction with pools of steam boilers."""

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
