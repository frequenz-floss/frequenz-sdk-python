# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Power distribution algorithm to distribute power between batteries."""

import logging
import math
from dataclasses import dataclass
from typing import Dict, List, NamedTuple, Tuple

from ...._internal._math import is_close_to_zero
from ....microgrid.component import BatteryData, InverterData

_logger = logging.getLogger(__name__)


class InvBatPair(NamedTuple):
    """InvBatPair with inverter and adjacent battery data."""

    battery: BatteryData
    """The battery data."""

    inverter: InverterData
    """The inverter data."""


@dataclass
class DistributionResult:
    """Distribution result."""

    distribution: Dict[int, float]
    """The power to be set for each inverter.

    The key is inverter ID, and the value is the power that should be set for
    that inverter.
    """

    remaining_power: float
    """The power which could not be distributed because of bounds."""


class DistributionAlgorithm:
    r"""Distribute power between many components.

    The purpose of this tool is to keep equal SoC level in the batteries.
    It takes total power that should be to be set for some subset of battery-inverter
    pairs. The total power is distributed between given battery-inverter pairs.
    Distribution is calculated based on data below:

    * Battery current SoC.
    * Battery upper and lower SoC bound.
    * Battery capacity.
    * Battery lower and upper power bound.
    * Inverter lower and upper active power bound.

    # Distribution algorithm

    Lets assume that:

    * `N` - number of batteries
    * `power_w` - power to distribute
    * `capacity[i]` - capacity of i'th battery
    * `available_soc[i]` - how much SoC remained to reach:
        * SoC upper bound - if need to distribute power that charges inverters.
        * SoC lower bound - if need to distribute power that discharges inverters.
        * `0` - if SoC is outside SoC bounds.

    * `total_capacity` - `sum(c for c in capacity.values())`
    * `capacity_ratio[i]` - `capacity[i]/total_capacity`


    We would like our distribution to meet the equation:

    ```
    distribution[i] = power_w * capacity_ratio[i] * x[i]
    ```

    where:

    ```
    sum(capacity_ratio[i] * x[i] for i in range(N)) == 1
    ```

    Let `y` be our unknown, the proportion to discharge each battery would be
    (1):

    ```
    x[i] = available_soc[i]*y
    ```

    We can compute `y` from equation above (2):

    ```
    sum(capacity_ratio[i] * x[i] for i in range(N)) == 1
    # =>
    sum(capacity_ratio[i] * available_soc[i] * y for i in range(N)) == 1
    # =>
    y = 1 / sum(capacity_ratio[i] * available_soc[i])
    ```

    Now we know everything and we can compute distribution:

    ```
    distribution[i] = power_w * capacity_ratio[i] * x[i]  # from (1)
    distribution[i] = \
            power_w * capacity_ratio[i] * available_soc[i] * y  # from (2)
    distribution[i] = power_w * capacity_ratio[i] * available_soc[i] * \
            1/sum(capacity_ratio[i] * available_soc[i])
    ```

    Let:

    ```
    battery_availability_ratio[i] = capacity_ratio[i] * available_soc[i]
    total_battery_availability_ratio = sum(battery_availability_ratio)
    ```

    Then:
    ```
    distribution[i] = power_w * battery_availability_ratio[i] \
            / total_battery_availability_ratio
    ```
    """

    def __init__(self, distributor_exponent: float = 1) -> None:
        """Create distribution algorithm instance.

        Args:
            distributor_exponent: How fast the batteries should strive to the
                equal SoC level. Should be float >= 0. Defaults=1.
                For example for distributor_exponent equal:
                    * 1 - means that proportion will be linear from SoC.
                    * 2 - means proportion would be like squared from SoC
                    * 3 - means proportion would be like x^3 from SoC.

        Example:
            Lets say we have two batteries `Bat1` and `Bat2`. All parameters
            except SoC are equal. SoC bounds for each battery is `lower = 20`,
            `upper = 80`.

            # Example 1

            Let:

            * `Bat1.soc = 70` and `Bat2.soc = 50`.
            * `Bat1.available_soc = 10`, `Bat2.available_soc = 30`
            * `Bat1.available_soc / Bat2.available_soc = 3`

            A request power of 8000W will be distributed as follows, for different
            values of `distribution_exponent`:

            | distribution_exponent | Bat1 | Bat2 |
            |-----------------------|------|------|
            | 0                     | 4000 | 4000 |
            | 1                     | 2000 | 6000 |
            | 2                     | 800  | 7200 |
            | 3                     | 285  | 7715 |


            # Example 2

            Let:

            * `Bat1.soc = 50` and `Bat2.soc = 20`.
            * `Bat1.available_soc = 30`, `Bat2.available_soc = 60`
            * `Bat1.available_soc / Bat2.available_soc = 2`

            A request power of 900W will be distributed as follows, for different
            values of `distribution_exponent`.

            | distribution_exponent | Bat1 | Bat2 |
            |-----------------------|------|------|
            | 0                     | 450  | 450  |
            | 1                     | 300  | 600  |
            | 2                     | 180  | 720  |
            | 3                     | 100  | 800  |

            # Example 3

            Let:

            * `Bat1.soc = 44` and `Bat2.soc = 64`.
            * `Bat1.available_soc = 36 (80 - 44)`, `Bat2.available_soc = 16 (80 - 64)`

            A request power of 900W will be distributed as follows, for these values of
            `distribution_exponent`:

            If `distribution_exponent` is:

            | distribution_exponent | Bat1 | Bat2 |
            |-----------------------|------|------|
            | 0                     | 450  | 450  |
            | 0.5                   | 600  | 400  |

        Raises:
            ValueError: If distributor_exponent < 0

        """
        super().__init__()

        if distributor_exponent < 0:
            raise ValueError("Distribution factor should be float >= 0.")
        self._distributor_exponent: float = distributor_exponent

    def _total_capacity(self, components: List[InvBatPair]) -> float:
        """Sum capacity between all batteries in the components list.

        Args:
            components: list of the components

        Raises:
            ValueError: If total capacity is 0.

        Returns:
            Sum of all batteries capacity in the components list.
        """
        total_capacity: float = sum(bat.capacity for bat, _ in components)
        if is_close_to_zero(total_capacity):
            msg = "All batteries have capacity 0."
            _logger.error(msg)
            raise ValueError(msg)

        return total_capacity

    def _compute_battery_availability_ratio(
        self,
        components: List[InvBatPair],
        available_soc: Dict[int, float],
        excl_bounds: Dict[int, float],
    ) -> Tuple[List[Tuple[InvBatPair, float, float]], float]:
        r"""Compute battery ratio and the total sum of all of them.

        battery_availability_ratio = capacity_ratio[i] * available_soc[i]
        Where:
        capacity_ratio[i] = components[i].battery.capacity \
            / sum(battery.capacity for battery, _ in components)

        Args:
            components: list of the components
            available_soc: How much SoC remained to reach
                * SoC upper bound - if need to distribute consumption power
                * SoC lower bound - if need to distribute supply power
            excl_bounds: Exclusion bounds for each inverter

        Returns:
            Tuple where first argument is battery availability ratio for each
                battery-inverter pair. The list is sorted by ratio in
                descending order.  The second element of the tuple is total sum
                of all battery ratios in the list.
        """
        total_capacity = self._total_capacity(components)
        battery_availability_ratio: List[Tuple[InvBatPair, float, float]] = []
        total_battery_availability_ratio: float = 0.0

        for pair in components:
            battery, inverter = pair
            capacity_ratio = battery.capacity / total_capacity
            soc_factor = pow(
                available_soc[battery.component_id], self._distributor_exponent
            )

            ratio = capacity_ratio * soc_factor
            battery_availability_ratio.append(
                (pair, excl_bounds[inverter.component_id], ratio)
            )
            total_battery_availability_ratio += ratio

        battery_availability_ratio.sort(
            key=lambda item: (item[1], item[2]), reverse=True
        )

        return battery_availability_ratio, total_battery_availability_ratio

    def _distribute_power(  # pylint: disable=too-many-arguments
        self,
        components: List[InvBatPair],
        power_w: float,
        available_soc: Dict[int, float],
        incl_bounds: Dict[int, float],
        excl_bounds: Dict[int, float],
    ) -> DistributionResult:
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """Distribute power between given components.

        After this method power should be distributed between batteries
        in a way that equalize SoC between batteries.

        Args:
            components: list of components.
            power_w: power to distribute
            available_soc: how much SoC remained to reach:
                * SoC upper bound - if need to distribute consumption power
                * SoC lower bound - if need to distribute supply power
            incl_bounds: Inclusion bounds for each inverter
            excl_bounds: Exclusion bounds for each inverter

        Returns:
            Distribution result.
        """
        (
            battery_availability_ratio,
            sum_ratio,
        ) = self._compute_battery_availability_ratio(
            components, available_soc, excl_bounds
        )

        distribution: Dict[int, float] = {}
        # sum_ratio == 0 means that all batteries are fully charged / discharged
        if is_close_to_zero(sum_ratio):
            distribution = {inverter.component_id: 0 for _, inverter in components}
            return DistributionResult(distribution, power_w)

        distributed_power: float = 0.0
        reserved_power: float = 0.0
        power_to_distribute: float = power_w
        used_ratio: float = 0.0
        ratio = sum_ratio
        excess_reserved: dict[int, float] = {}
        deficits: dict[int, float] = {}
        for pair, excl_bound, battery_ratio in battery_availability_ratio:
            inverter = pair[1]
            # ratio = 0, means all remaining batteries reach max SoC lvl or have no
            # capacity
            if is_close_to_zero(ratio):
                distribution[inverter.component_id] = 0.0
                continue

            power_to_distribute = power_w - reserved_power
            calculated_power = power_to_distribute * battery_ratio / ratio
            reserved_power += max(calculated_power, excl_bound)
            used_ratio += battery_ratio
            ratio = sum_ratio - used_ratio
            # If the power allocated for that inverter is out of bound,
            # then we need to distribute more power over all remaining batteries.
            incl_bound = incl_bounds[inverter.component_id]
            if calculated_power > incl_bound:
                excess_reserved[inverter.component_id] = incl_bound - excl_bound
                # # Distribute between remaining batteries
            elif calculated_power < excl_bound:
                deficits[inverter.component_id] = calculated_power - excl_bound
            else:
                excess_reserved[inverter.component_id] = calculated_power - excl_bound

            distributed_power += excl_bound
            distribution[inverter.component_id] = excl_bound

        for inverter_id, deficit in deficits.items():
            while not is_close_to_zero(deficit) and deficit < 0.0:
                if not excess_reserved:
                    break
                take_from = max(excess_reserved.items(), key=lambda item: item[1])
                if is_close_to_zero(take_from[1]) or take_from[1] < 0.0:
                    break
                if take_from[1] >= -deficit or math.isclose(take_from[1], -deficit):
                    excess_reserved[take_from[0]] += deficit
                    deficits[inverter_id] = 0.0
                    deficit = 0.0
                else:
                    deficit += excess_reserved[take_from[0]]
                    deficits[inverter_id] = deficit
                    excess_reserved[take_from[0]] = 0.0
            if deficit < -0.1:
                left_over = power_w - distributed_power
                if left_over > -deficit:
                    distributed_power += deficit
                elif left_over > 0.0:
                    distributed_power += left_over

        for inverter_id, excess in excess_reserved.items():
            distribution[inverter_id] += excess
            distributed_power += excess

        left_over = power_w - distributed_power
        dist = DistributionResult(distribution, left_over)

        return self._greedy_distribute_remaining_power(
            dist.distribution, incl_bounds, dist.remaining_power
        )

    def _greedy_distribute_remaining_power(
        self,
        distribution: Dict[int, float],
        upper_bounds: Dict[int, float],
        remaining_power: float,
    ) -> DistributionResult:
        """Add remaining power greedily to the given distribution.

        Distribution for each inverter will not exceed its upper bound.

        Args:
            distribution: distribution
            upper_bounds: upper bounds inverter and adjacent battery in
                distribution.
            remaining_power: power to distribute

        Returns:
            Return the power for each inverter in given distribution.
        """
        if is_close_to_zero(remaining_power):
            return DistributionResult(distribution, remaining_power)

        new_distribution: Dict[int, float] = {}

        for inverter_id, power in distribution.items():
            if is_close_to_zero(remaining_power) or is_close_to_zero(power):
                new_distribution[inverter_id] = power
            else:
                remaining_power_capacity: float = upper_bounds[inverter_id] - power
                to_add = min(remaining_power_capacity, remaining_power)
                new_distribution[inverter_id] = power + to_add
                remaining_power -= to_add

        return DistributionResult(new_distribution, remaining_power)

    def distribute_power_equally(
        self, power: float, inverters: set[int]
    ) -> DistributionResult:
        """Distribute the power equally between the inverters in the set.

        This function is mainly useful to set the power for components that are
        broken or have no metrics available.

        Args:
            power: the power to distribute.
            inverters: the inverters to set the power to.

        Returns:
            the power distribution result.
        """
        power_per_inverter = power / len(inverters)
        return DistributionResult(
            distribution={id: power_per_inverter for id in inverters},
            remaining_power=0.0,
        )

    def distribute_power(
        self, power: float, components: List[InvBatPair]
    ) -> DistributionResult:
        """Distribute given power between given components.

        Args:
            power: Power to distribute
            components: InvBatPaired components data. Each pair should have data
                for battery and adjacent inverter.

        Returns:
            Distribution result
        """
        if power >= 0.0:
            return self._distribute_consume_power(power, components)
        return self._distribute_supply_power(power, components)

    def _distribute_consume_power(
        self, power_w: float, components: List[InvBatPair]
    ) -> DistributionResult:
        """Distribute power between the given components.

        Distribute power in a way that the SoC level between given components will:
            * stay on the same level, equal in all given components
            * will try to align himself to the same level.

        Args:
            power_w: power to distribute
            components: list of components between which the power should be
                distributed.

        Returns:
            Distribution result, batteries with no SoC and capacity won't be used.
        """
        # If SoC exceeded bound then remaining SoC should be 0.
        # Otherwise algorithm would try to supply power from that battery
        # in order to keep equal SoC level.
        available_soc: Dict[int, float] = {}
        for battery, _ in components:
            available_soc[battery.component_id] = max(
                0.0, battery.soc_upper_bound - battery.soc
            )

        incl_bounds: Dict[int, float] = {}
        excl_bounds: Dict[int, float] = {}
        for battery, inverter in components:
            # We can supply/consume with int only
            incl_bounds[inverter.component_id] = min(
                inverter.active_power_inclusion_upper_bound,
                battery.power_inclusion_upper_bound,
            )
            excl_bounds[inverter.component_id] = max(
                inverter.active_power_exclusion_upper_bound,
                battery.power_exclusion_upper_bound,
            )

        return self._distribute_power(
            components, power_w, available_soc, incl_bounds, excl_bounds
        )

    def _distribute_supply_power(
        self, power_w: float, components: List[InvBatPair]
    ) -> DistributionResult:
        """Distribute power between the given components.

        Distribute power in a way that the SoC level between given components will:
            * stay on the same level, equal in all given components
            * will try to align himself to the same level.

        Args:
            power_w: power to distribute
            components: list of components between which the power should be
                distributed.

        Returns:
            Distribution result.
        """
        available_soc: Dict[int, float] = {}
        for battery, _ in components:
            available_soc[battery.component_id] = max(
                0.0, battery.soc - battery.soc_lower_bound
            )

        incl_bounds: Dict[int, float] = {}
        excl_bounds: Dict[int, float] = {}
        for battery, inverter in components:
            incl_bounds[inverter.component_id] = -1 * max(
                inverter.active_power_inclusion_lower_bound,
                battery.power_inclusion_lower_bound,
            )
            excl_bounds[inverter.component_id] = -1 * min(
                inverter.active_power_exclusion_lower_bound,
                battery.power_exclusion_lower_bound,
            )

        result: DistributionResult = self._distribute_power(
            components, -1 * power_w, available_soc, incl_bounds, excl_bounds
        )

        for inverter_id in result.distribution.keys():
            result.distribution[inverter_id] *= -1
        result.remaining_power *= -1

        return result
