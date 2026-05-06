# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Types for exposing EV charger pool reports."""

import typing

from frequenz.quantities import Power

from .. import Bounds
from ..component_pool._component_pool_report import ComponentPoolReport


class EVChargerPoolReport(ComponentPoolReport, typing.Protocol):
    """A status report for an EV chargers pool."""

    @property
    def target_power(self) -> Power | None:
        """The currently set power for the EV chargers."""

    @property
    def bounds(self) -> Bounds[Power] | None:
        """The usable bounds for the EV chargers.

        These bounds are adjusted to any restrictions placed by actors with higher
        priorities.
        """
