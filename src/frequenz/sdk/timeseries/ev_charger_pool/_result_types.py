# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Types for exposing EV charger pool reports."""

import typing

from frequenz.quantities import Power

from .._base_types import Bounds


class EVChargerPoolReport(typing.Protocol):
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
