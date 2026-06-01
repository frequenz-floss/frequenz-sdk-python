# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Interactions with pools of steam boilers."""

from ._result_types import SteamBoilerPoolReport
from ._steam_boiler_pool import SteamBoilerPool, SteamBoilerPoolError

__all__ = [
    "SteamBoilerPool",
    "SteamBoilerPoolError",
    "SteamBoilerPoolReport",
]
