# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Aggregate the system bounds of a pool of components."""

from collections import abc
from datetime import datetime
from typing import Protocol

from frequenz.quantities import Power

from .._base_types import Bounds, SystemBounds


class ComponentBoundsData(Protocol):
    """The component data needed to aggregate the system bounds."""

    @property
    def timestamp(self) -> datetime:
        """The timestamp of when the data was measured."""

    @property
    def active_power_inclusion_lower_bound(self) -> float:
        """The active power inclusion lower bound, in Watts."""

    @property
    def active_power_exclusion_lower_bound(self) -> float:
        """The active power exclusion lower bound, in Watts."""

    @property
    def active_power_exclusion_upper_bound(self) -> float:
        """The active power exclusion upper bound, in Watts."""

    @property
    def active_power_inclusion_upper_bound(self) -> float:
        """The active power inclusion upper bound, in Watts."""


def aggregate_system_bounds(
    working_data: abc.Collection[ComponentBoundsData],
) -> SystemBounds | None:
    """Sum the working components' bounds; shift inclusion bounds by unreachable power.

    Args:
        working_data: The latest data of the working components.

    Returns:
        The system bounds, or `None` if there is no data from a working component.
    """
    if not working_data:
        return None

    def total(bound: abc.Callable[[ComponentBoundsData], float]) -> Power:
        return Power.from_watts(sum(bound(data) for data in working_data))

    return SystemBounds(
        timestamp=max(data.timestamp for data in working_data),
        inclusion_bounds=Bounds(
            lower=total(lambda data: data.active_power_inclusion_lower_bound),
            upper=total(lambda data: data.active_power_inclusion_upper_bound),
        ),
        exclusion_bounds=Bounds(
            lower=total(lambda data: data.active_power_exclusion_lower_bound),
            upper=total(lambda data: data.active_power_exclusion_upper_bound),
        ),
    )
