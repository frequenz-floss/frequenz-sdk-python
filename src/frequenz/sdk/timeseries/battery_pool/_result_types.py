# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Types for exposing battery pool reports."""

import abc

from ...actor import power_distributing
from ...actor._power_managing._base_classes import Report
from .._base_types import Bounds
from .._quantities import Power


# This class is used to expose the generic reports from the PowerManager with specific
# documentation for the battery pool.
class BatteryPoolReport(Report):
    """A status report for a battery pool."""

    target_power: Power | None
    """The currently set power for the batteries."""

    distribution_result: power_distributing.Result | None
    """The result of the last power distribution.

    This is `None` if no power distribution has been performed yet.
    """

    @property
    def bounds(self) -> Bounds[Power] | None:
        """The usable bounds for the batteries.

        These bounds are adjusted to any restrictions placed by actors with higher
        priorities.

        There might be exclusion zones within these bounds. If necessary, the
        [`adjust_to_bounds`][frequenz.sdk.timeseries.battery_pool.BatteryPoolReport.adjust_to_bounds]
        method may be used to check if a desired power value fits the bounds, or to get
        the closest possible power values that do fit the bounds.
        """

    @abc.abstractmethod
    def adjust_to_bounds(self, power: Power) -> tuple[Power | None, Power | None]:
        """Adjust a power value to the bounds.

        This method can be used to adjust a desired power value to the power bounds
        available to the actor.

        If the given power value falls within the usable bounds, it will be returned
        unchanged.

        If it falls outside the usable bounds, the closest possible value on the
        corresponding side will be returned.  For example, if the given power is lower
        than the lowest usable power, only the lowest usable power will be returned, and
        similarly for the highest usable power.

        If the given power falls within an exclusion zone that's contained within the
        usable bounds, the closest possible power values on both sides will be returned.

        !!! note
            It is completely optional to use this method to adjust power values before
            proposing them, because the battery pool will do this automatically.  This
            method is provided for convenience, and for granular control when there are
            two possible power values, both of which fall within the available bounds.

        Example:
            ```python
            from frequenz.sdk import microgrid

            power_status_rx = microgrid.battery_pool(
                priority=5,
            ).power_status.new_receiver()
            power_status = await power_status_rx.receive()
            desired_power = Power.from_watts(1000.0)

            match power_status.adjust_to_bounds(desired_power):
                case (power, _) if power == desired_power:
                    print("Desired power is available.")
                case (None, power) | (power, None) if power:
                    print(f"Closest available power is {power}.")
                case (lower, upper) if lower and upper:
                    print(f"Two options {lower}, {upper} to propose to battery pool.")
                case (None, None):
                    print("No available power")
            ```

        Args:
            power: The power value to adjust.

        Returns:
            A tuple of the closest power values to the desired power that fall within
                the available bounds for the actor.
        """
