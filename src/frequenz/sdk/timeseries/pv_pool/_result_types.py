# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Types for exposing PV pool reports."""

import typing

from frequenz.quantities import Power

from .._base_types import Bounds


class PVPoolReport(typing.Protocol):
    """A status report for a PV pool."""

    @property
    def target_power(self) -> Power | None:
        """The currently set power for the PV inverters."""

    @property
    def bounds(self) -> Bounds[Power] | None:
        """The usable bounds for the PV inverters.

        These bounds are adjusted to any restrictions placed by actors with higher
        priorities.
        """
