# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Classes to track the status of components in the microgrid."""


import enum
import typing
from collections import abc
from dataclasses import dataclass


@dataclass
class ComponentPoolStatus:
    """Status of all components of a certain category in the microgrid."""

    working: set[int]
    """Set of working component ids."""

    uncertain: set[int]
    """Set of components to be used only when there are none known to be working."""

    def get_working_components(self, components: abc.Set[int]) -> set[int]:
        """From the given set of components return the working ones.

        Args:
            components: Set of components.

        Returns:
            Subset with working components.
        """
        working = self.working.intersection(components)
        if len(working) > 0:
            return working
        return self.uncertain.intersection(components)


class ComponentStatusEnum(enum.Enum):
    """Enum for component status."""

    NOT_WORKING = 0
    """Component is not working and should not be used."""

    UNCERTAIN = 1
    """Component should work, although the last request to it failed.

    It is blocked for few seconds and it is not recommended to use it unless it is
    necessary.
    """

    WORKING = 2
    """Component is working"""


@dataclass(frozen=True)
class ComponentStatus:
    """Status of a single component."""

    component_id: int
    """Component ID."""

    value: ComponentStatusEnum
    """Component status."""


@dataclass(frozen=True, kw_only=True)
class SetPowerResult:
    """Lists of components for which the last set power command succeeded or failed."""

    succeeded: typing.Iterable[int]
    """Component IDs for which the last set power command succeeded."""

    failed: typing.Iterable[int]
    """Component IDs for which the last set power command failed."""
