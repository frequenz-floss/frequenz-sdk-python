# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Classes to track the status of components in the microgrid."""


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
