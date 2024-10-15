# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Configuration for the power distributor's EV charger manager."""

from collections import abc
from dataclasses import dataclass, field
from datetime import timedelta

from frequenz.quantities import Current


@dataclass(frozen=True)
class EVDistributionConfig:
    """Configuration for the power distributor's EV charger manager."""

    component_ids: abc.Set[int]
    """The component ids of the EV chargers."""

    min_current: Current = field(default_factory=lambda: Current.from_amperes(6.0))
    """The minimum current that can be allocated to an EV charger."""

    initial_current: Current = field(default_factory=lambda: Current.from_amperes(10.0))
    """The initial current that can be allocated to an EV charger."""

    increase_power_interval: timedelta = timedelta(seconds=60)
    """The interval at which the power can be increased for an EV charger."""
