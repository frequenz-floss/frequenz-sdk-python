# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Types for exposing PV pool reports."""

import typing

from ...actor import power_distributing
from .._base_types import Bounds
from .._quantities import Power


class PVPoolReport(typing.Protocol):
    """A status report for a PV pool."""

    target_power: Power | None
    """The currently set power for the PV inverters."""

    distribution_result: power_distributing.Result | None
    """The result of the last power distribution.

    This is `None` if no power distribution has been performed yet.
    """

    @property
    def bounds(self) -> Bounds[Power] | None:
        """The usable bounds for the PV inverters.

        These bounds are adjusted to any restrictions placed by actors with higher
        priorities.
        """
