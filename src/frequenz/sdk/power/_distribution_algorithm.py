# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Power distribution algorithm to distribute power between batteries."""

import logging
from dataclasses import dataclass
from math import ceil, floor
from typing import Dict, List, NamedTuple, Tuple

from ..microgrid.component import BatteryData, InverterData

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

    distribution: Dict[int, int]
    """The power to be set for each inverter.

    The key is inverter ID, and the value is the power that should be set for
    that inverter.
    """

    remaining_power: int
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

    ``` python
    distribution[i] = power_w * capacity_ratio[i] * x[i]
    ```

    where:

    ``` python
    sum(capacity_ratio[i] * x[i] for i in range(N)) == 1
    ```

    Let `y` be our unknown, the proportion to discharge each battery would be
    (1):

    ``` python
    x[i] = available_soc[i]*y
    ```

    We can compute `y` from equation above (2):

    ``` python
    sum(capacity_ratio[i] * x[i] for i in range(N)) == 1
    # =>
    sum(capacity_ratio[i] * available_soc[i] * y for i in range(N)) == 1
    # =>
    y = 1 / sum(capacity_ratio[i] * available_soc[i])
    ```

    Now we know everything and we can compute distribution:

    ``` python
    distribution[i] = power_w * capacity_ratio[i] * x[i]  # from (1)
    distribution[i] = \
            power_w * capacity_ratio[i] * available_soc[i] * y  # from (2)
    distribution[i] = power_w * capacity_ratio[i] * available_soc[i] * \
            1/sum(capacity_ratio[i] * available_soc[i])
    ```

    Let:

    ``` python
    battery_availability_ratio[i] = capacity_ratio[i] * available_soc[i]
    total_battery_availability_ratio = sum(battery_availability_ratio)
    ```

    Then:
    ``` python
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

            We need to distribute 8000W.

            If `distribution_exponent` is:

            * `0`: distribution for each battery will be the equal.
              ``` python
              Bat1.distribution = 4000; Bat2.distribution = 4000
              ```

            * `1`: then `Bat2` will have 3x more power assigned then `Bat1`.
              ``` python
              10 * x + 30 * x = 8000
              x = 200
              Bat1.distribution = 2000; Bat2.distribution = 6000
              ```

            * `2`: then `Bat2` will have 9x more power assigned then `Bat1`.
              ``` python
              10^2 * x + 30^2 * x = 8000
              x = 80
              Bat1.distribution = 800; Bat2.distribution = 7200
              ```

            * `3`: then `Bat2` will have 27x more power assigned then `Bat1`.
              ``` python
              10^3 * x + 30^3 * x = 8000
              x = 0,285714286
              Bat1.distribution = 285; Bat2.distribution = 7715
              ```

            # Example 2

            Let:

            * `Bat1.soc = 50` and `Bat2.soc = 20`.
            * `Bat1.available_soc = 30`, `Bat2.available_soc = 60`
            * `Bat1.available_soc / Bat2.available_soc = 2`

            We need to distribute 900W.

            If `distribution_exponent` is:

            * `0`: distribution for each battery will be the same.
              ``` python
              Bat1.distribution = 4500; Bat2.distribution = 450
              ```

            * `1`: then `Bat2` will have 2x more power assigned then `Bat1`.
              ``` python
              30 * x + 60 * x = 900
              x = 100
              Bat1.distribution = 300; Bat2.distribution = 600
              ```

            * `2`: then `Bat2` will have 4x more power assigned then `Bat1`.
              ``` python
              30^2 * x + 60^2 * x = 900
              x = 0.2
              Bat1.distribution = 180; Bat2.distribution = 720
              ```

            * `3`: then `Bat2` will have 8x more power assigned then `Bat1`.
              ``` python
              30^3 * x + 60^3 * x = 900
              x = 0,003703704
              Bat1.distribution = 100; Bat2.distribution = 800
              ```

            # Example 3

            Let:

            * `Bat1.soc = 44` and `Bat2.soc = 64`.
            * `Bat1.available_soc = 36 (80 - 44)`, `Bat2.available_soc = 16 (80 - 64)`

            We need to distribute 900W.

            If `distribution_exponent` is:

            * `0`: distribution for each battery will be the equal.
              ``` python
              Bat1.distribution = 450; Bat2.distribution = 450
              ```

            * `0.5`: then `Bat2` will have 6/4x more power assigned then `Bat1`.
              ``` python
              sqrt(36) * x + sqrt(16) * x = 900
              x = 100
              Bat1.distribution = 600; Bat2.distribution = 400
              ```

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
        if total_capacity == 0.0:
            msg = "All batteries have capacity 0."
            _logger.error(msg)
            raise ValueError(msg)

        return total_capacity

    def _compute_battery_availability_ratio(
        self, components: List[InvBatPair], available_soc: Dict[int, float]
    ) -> Tuple[List[Tuple[InvBatPair, float]], float]:
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

        Returns:
            Tuple where first argument is battery availability ratio for each
                battery-inverter pair. The list is sorted by ratio in
                descending order.  The second element of the tuple is total sum
                of all battery ratios in the list.
        """
        total_capacity = self._total_capacity(components)
        battery_availability_ratio: List[Tuple[InvBatPair, float]] = []
        total_battery_availability_ratio: float = 0.0

        for pair in components:
            battery = pair[0]
            capacity_ratio = battery.capacity / total_capacity
            soc_factor = pow(
                available_soc[battery.component_id], self._distributor_exponent
            )

            ratio = capacity_ratio * soc_factor
            battery_availability_ratio.append((pair, ratio))
            total_battery_availability_ratio += ratio

        battery_availability_ratio.sort(key=lambda item: item[1], reverse=True)

        return battery_availability_ratio, total_battery_availability_ratio

    def _distribute_power(
        self,
        components: List[InvBatPair],
        power_w: int,
        available_soc: Dict[int, float],
        upper_bounds: Dict[int, int],
    ) -> DistributionResult:
        # pylint: disable=too-many-locals
        """Distribute power between given components.

        After this method power should be distributed between batteries
        in a way that equalize SoC between batteries.

        Args:
            components: list of components.
            power_w: power to distribute
            available_soc: how much SoC remained to reach:
                * SoC upper bound - if need to distribute consumption power
                * SoC lower bound - if need to distribute supply power
            upper_bounds: Min between upper bound of each pair in the components list:
                * supply upper bound - if need to distribute consumption power
                * consumption lower bound - if need to distribute supply power

        Returns:
            Distribution result.
        """
        (
            battery_availability_ratio,
            sum_ratio,
        ) = self._compute_battery_availability_ratio(components, available_soc)

        distribution: Dict[int, int] = {}

        # sum_ratio == 0 means that all batteries are fully charged / discharged
        if sum_ratio == 0.0:
            distribution = {inverter.component_id: 0 for _, inverter in components}
            return DistributionResult(distribution, power_w)

        distributed_power = 0
        power_to_distribute: int = power_w
        used_ratio: float = 0.0
        ratio = sum_ratio
        for pair, battery_ratio in battery_availability_ratio:
            inverter = pair[1]
            # ratio = 0, means all remaining batteries reach max SoC lvl or have no
            # capacity
            if ratio == 0.0:
                distribution[inverter.component_id] = 0
                continue

            distribution[inverter.component_id] = floor(
                power_to_distribute * battery_ratio / ratio
            )

            used_ratio += battery_ratio

            # If the power allocated for that inverter is out of bound,
            # then we need to distribute more power over all remaining batteries.
            upper_bound = upper_bounds[inverter.component_id]
            if distribution[inverter.component_id] > upper_bound:
                distribution[inverter.component_id] = upper_bound
                distributed_power += upper_bound
                # Distribute only the remaining power.
                power_to_distribute = power_w - distributed_power
                # Distribute between remaining batteries
                ratio = sum_ratio - used_ratio
            else:
                distributed_power += distribution[inverter.component_id]

        return DistributionResult(distribution, power_w - distributed_power)

    def _greedy_distribute_remaining_power(
        self,
        distribution: Dict[int, int],
        upper_bounds: Dict[int, int],
        remaining_power: int,
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
        if remaining_power == 0:
            return DistributionResult(distribution, remaining_power)

        new_distribution: Dict[int, int] = {}

        for inverter_id, power in distribution.items():
            if remaining_power == 0 or power == 0:
                new_distribution[inverter_id] = power
            else:
                remaining_power_capacity: int = upper_bounds[inverter_id] - power
                to_add = min(remaining_power_capacity, remaining_power)
                new_distribution[inverter_id] = power + to_add
                remaining_power -= to_add

        return DistributionResult(new_distribution, remaining_power)

    def distribute_power(
        self, power: int, components: List[InvBatPair]
    ) -> DistributionResult:
        """Distribute given power between given components.

        Args:
            power: Power to distribute
            components: InvBatPaired components data. Each pair should have data
                for battery and adjacent inverter.

        Returns:
            Distribution result
        """
        if power >= 0:
            return self._distribute_consume_power(power, components)
        return self._distribute_supply_power(power, components)

    def _distribute_consume_power(
        self, power_w: int, components: List[InvBatPair]
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

        bounds: Dict[int, int] = {}
        for battery, inverter in components:
            # We can supply/consume with int only
            inverter_bound = inverter.active_power_upper_bound
            battery_bound = battery.power_upper_bound
            bounds[inverter.component_id] = floor(min(inverter_bound, battery_bound))

        result: DistributionResult = self._distribute_power(
            components, power_w, available_soc, bounds
        )

        return self._greedy_distribute_remaining_power(
            result.distribution, bounds, result.remaining_power
        )

    def _distribute_supply_power(
        self, power_w: int, components: List[InvBatPair]
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

        bounds: Dict[int, int] = {}
        for battery, inverter in components:
            # We can consume with int only
            inverter_bound = inverter.active_power_lower_bound
            battery_bound = battery.power_lower_bound
            bounds[inverter.component_id] = -1 * ceil(
                max(inverter_bound, battery_bound)
            )

        result: DistributionResult = self._distribute_power(
            components, -1 * power_w, available_soc, bounds
        )

        result = self._greedy_distribute_remaining_power(
            result.distribution, bounds, result.remaining_power
        )

        for inverter_id in result.distribution.keys():
            result.distribution[inverter_id] *= -1
        result.remaining_power *= -1

        return result
