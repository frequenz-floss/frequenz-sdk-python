# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Types for exposing steam boiler pool reports."""

import typing

from frequenz.quantities import Power

from .. import Bounds
from ..component_pool._component_pool_report import ComponentPoolReport


class SteamBoilerPoolReport(ComponentPoolReport, typing.Protocol):
    """A status report for a steam boiler pool."""

    @property
    def target_power(self) -> Power | None:
        """The currently set power for the steam boilers."""

    @property
    def bounds(self) -> Bounds[Power] | None:
        """The usable bounds for the steam boilers.

        These bounds are adjusted to any restrictions placed by actors with higher
        priorities.
        """
