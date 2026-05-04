# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Types for exposing component pool reports."""

import typing

from frequenz.quantities import Power

from .._base_types import Bounds


class ComponentPoolReport(typing.Protocol):
    """A status report for a component pool."""

    @property
    def target_power(self) -> Power | None:
        """The currently set power for the components."""

    @property
    def bounds(self) -> Bounds[Power] | None:
        """The usable bounds for the components.

        These bounds are adjusted to any restrictions placed by actors with higher
        priorities.
        """
