# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Power distribution algorithm to distribute power between batteries."""

import logging
import math
from dataclasses import dataclass
from typing import NamedTuple, Sequence

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power

from ...._internal._math import is_close_to_zero
from ..._old_component_data import BatteryData, InverterData
from ..result import PowerBounds

_logger = logging.getLogger(__name__)


@dataclass()
class AggregatedBatteryData:
    """Aggregated battery data."""

    component_id: ComponentId
    """The component ID of the first battery.

    This is only used to identify the pair of battery and inverter.
    """

    soc: float
    """The aggregated SoC of the batteries."""

    capacity: float
    """The aggregated capacity of the batteries."""

    soc_upper_bound: float
    """The aggregated upper SoC bound of the batteries."""

    soc_lower_bound: float
    """The aggregated lower SoC bound of the batteries."""

    power_bounds: PowerBounds
    """The aggregated power bounds of the batteries."""

    def __init__(self, batteries: list[BatteryData]) -> None:
        """Create DistBatteryData from BatteryData.

        Aggregates the data of the batteries:

        * Capacity: Sum of capacities of all batteries.
        * SoC: Weighted average of SoCs of all batteries.
        * SoC bounds: Weighted average of SoC bounds of all batteries.
        * Power inclusion bounds: Sum of power inclusion bounds of all batteries.
        * Power exclusion bounds: Largest power exclusion bound multiplied by
            the number of batteries.

        Args:
            batteries: The batteries to aggregate.
        """
        assert len(batteries) > 0, "AggregatedBatteryData: No batteries given."

        # We need only one component ID for DistBatteryData to be able to
        # identify the pair
        self.component_id = batteries[0].component_id

        self.capacity = sum(b.capacity for b in batteries)

        if self.capacity != 0.0:
            self.soc = sum(b.soc * b.capacity for b in batteries) / self.capacity
            self.soc_upper_bound = (
                sum(b.soc_upper_bound * b.capacity for b in batteries) / self.capacity
            )
            self.soc_lower_bound = (
                sum(b.soc_lower_bound * b.capacity for b in batteries) / self.capacity
            )
        else:
            self.soc = math.nan
            self.soc_upper_bound = math.nan
            self.soc_lower_bound = math.nan

        self.power_bounds = _aggregate_battery_power_bounds(
            list(
                map(
                    lambda metrics: PowerBounds(
                        inclusion_upper=Power.from_watts(
                            metrics.power_inclusion_upper_bound
                        ),
                        inclusion_lower=Power.from_watts(
                            metrics.power_inclusion_lower_bound
                        ),
                        exclusion_upper=Power.from_watts(
                            metrics.power_exclusion_upper_bound
                        ),
                        exclusion_lower=Power.from_watts(
                            metrics.power_exclusion_lower_bound
                        ),
                    ),
                    batteries,
                )
            )
        )


def _aggregate_battery_power_bounds(
    battery_metrics: Sequence[PowerBounds],
) -> PowerBounds:
    """Calculate bounds for a set of batteries located behind one set of inverters.

    Args:
        battery_metrics: List of PowerBounds for each battery.

    Returns:
        A PowerBounds object containing the aggregated bounds for all given batteries
    """
    assert len(battery_metrics) > 0, "No batteries given."

    # Calculate the aggregated bounds for the set of batteries
    power_inclusion_upper_bound = sum(
        (bounds.inclusion_upper for bounds in battery_metrics), start=Power.zero()
    )
    power_inclusion_lower_bound = sum(
        (bounds.inclusion_lower for bounds in battery_metrics), start=Power.zero()
    )

    # To satisfy the largest exclusion bounds in the set we need to
    # provide the power defined by the largest bounds multiplied by the
    # number of batteries in the set.
    power_exclusion_upper_bound = Power.from_watts(
        max(bounds.exclusion_upper for bounds in battery_metrics).as_watts()
        * len(battery_metrics)
    )
    power_exclusion_lower_bound = Power.from_watts(
        min(bounds.exclusion_lower for bounds in battery_metrics).as_watts()
        * len(battery_metrics)
    )

    return PowerBounds(
        inclusion_lower=power_inclusion_lower_bound,
        exclusion_lower=power_exclusion_lower_bound,
        exclusion_upper=power_exclusion_upper_bound,
        inclusion_upper=power_inclusion_upper_bound,
    )


class InvBatPair(NamedTuple):
    """InvBatPair with inverter and adjacent battery data."""

    battery: AggregatedBatteryData
    """The battery data."""

    inverter: list[InverterData]
    """The inverter data."""


@dataclass
class AvailabilityRatio:
    """Availability ratio for a battery-inverter pair."""

    battery_id: ComponentId
    """The battery ID."""

    inverter_ids: list[ComponentId]
    """The inverter IDs."""

    ratio: float
    """The availability ratio."""

    min_power: Power
    """The minimum power that can be set for the battery-inverters pair."""


@dataclass
class _Power:
    """Helper class for distribution algorithm."""

    upper_bound: Power
    """The upper bound of the power that can be set for the battery."""

    power: Power
    """The power to be set for the inverter."""


_InverterSet = frozenset[ComponentId]
"""A set of inverter IDs."""


@dataclass
class _Allocation:
    """Helper class for distribution algorithm."""

    inverter_ids: _InverterSet
    """The IDs of the inverters."""

    power: Power
    """The power to be set for the inverters."""


@dataclass
class DistributionResult:
    """Distribution result."""

    distribution: dict[ComponentId, Power]
    """The power to be set for each inverter.

    The key is inverter ID, and the value is the power that should be set for
    that inverter.
    """

    remaining_power: Power
    """The power which could not be distributed because of bounds."""


class BatteryDistributionAlgorithm:
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

    _MINIMUM_DEFICIT_TO_CONSIDER = Power.from_watts(-0.1)

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
        if distributor_exponent < 0:
            raise ValueError("Distribution factor should be float >= 0.")
        self._distributor_exponent: float = distributor_exponent

    def _total_capacity(self, components: list[InvBatPair]) -> float:
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
        components: list[InvBatPair],
        available_soc: dict[ComponentId, float],
        excl_bounds: dict[ComponentId, Power],
    ) -> tuple[list[AvailabilityRatio], float]:
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
        battery_availability_ratio: list[AvailabilityRatio] = []
        total_battery_availability_ratio: float = 0.0

        for pair in components:
            battery, inverters = pair
            capacity_ratio = battery.capacity / total_capacity
            soc_factor: float = pow(
                available_soc[battery.component_id], self._distributor_exponent
            )

            ratio = capacity_ratio * soc_factor

            inverter_ids = [inv.component_id for inv in inverters]
            inverter_ids.sort(key=lambda item: (excl_bounds[item], item), reverse=True)

            battery_availability_ratio.append(
                AvailabilityRatio(
                    battery.component_id,
                    inverter_ids,
                    ratio,
                    # Min power we need to request from the pair.
                    # Note that indvidual inverters may have lower min power
                    # and need to be checked individually.
                    min_power=max(
                        excl_bounds[battery.component_id],
                        min(excl_bounds[inverter_id] for inverter_id in inverter_ids),
                    ),
                )
            )

            total_battery_availability_ratio += ratio

        battery_availability_ratio.sort(
            key=lambda item: (item.min_power, item.ratio), reverse=True
        )

        return battery_availability_ratio, total_battery_availability_ratio

    # pylint: disable-next=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
    def _distribute_power(
        self,
        *,
        components: list[InvBatPair],
        power: Power,
        available_soc: dict[ComponentId, float],
        incl_bounds: dict[ComponentId, Power],
        excl_bounds: dict[ComponentId, Power],
    ) -> DistributionResult:
        """Distribute power between given components.

        After this method power should be distributed between batteries
        in a way that equalize SoC between batteries.

        Args:
            components: list of components.
            power: power to distribute
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

        # sum_ratio == 0 means that all batteries are fully charged / discharged
        if is_close_to_zero(sum_ratio):
            final_distribution = {
                inverter.component_id: Power.zero()
                for _, inverters in components
                for inverter in inverters
            }
            return DistributionResult(final_distribution, power)

        # key: inverter_ids, value: _Power(upper_bound, power)
        distribution: dict[_InverterSet, _Power] = {}
        distributed_power: Power = Power.zero()
        reserved_power: Power = Power.zero()
        power_to_distribute: Power = power
        used_ratio: float = 0.0
        ratio = sum_ratio
        excess_reserved: dict[_InverterSet, Power] = {}
        deficits: dict[_InverterSet, Power] = {}

        for ratio_data in battery_availability_ratio:
            inverter_set = _InverterSet(ratio_data.inverter_ids)
            # ratio = 0, means all remaining batteries reach max SoC lvl or have no
            # capacity
            if is_close_to_zero(ratio):
                distribution[inverter_set] = _Power(
                    upper_bound=Power.zero(),
                    power=Power.zero(),
                )
                continue

            power_to_distribute = power - reserved_power
            calculated_power = power_to_distribute * ratio_data.ratio / ratio
            reserved_power += max(calculated_power, ratio_data.min_power)
            used_ratio += ratio_data.ratio
            ratio = sum_ratio - used_ratio

            # If the power allocated for that inverter set is out of bound,
            # then we need to distribute more power over all remaining batteries.
            incl_bound = min(
                sum(
                    (
                        incl_bounds[inverter_id]
                        for inverter_id in ratio_data.inverter_ids
                    ),
                    start=Power.zero(),
                ),
                incl_bounds[ratio_data.battery_id],
            )
            if calculated_power > incl_bound:
                excess_reserved[inverter_set] = incl_bound - ratio_data.min_power
            # # Distribute between remaining batteries
            elif calculated_power < ratio_data.min_power:
                deficits[inverter_set] = calculated_power - ratio_data.min_power
            else:
                excess_reserved[inverter_set] = calculated_power - ratio_data.min_power

            distributed_power += ratio_data.min_power
            distribution[inverter_set] = _Power(
                upper_bound=incl_bound,
                power=ratio_data.min_power,
            )

        for inverter_ids, deficit in deficits.items():
            while not deficit.isclose(Power.zero()) and deficit < Power.zero():
                if not excess_reserved:
                    break
                largest = _Allocation(
                    *max(excess_reserved.items(), key=lambda item: item[1])
                )

                if largest.power.isclose(Power.zero()) or largest.power < Power.zero():
                    break
                if largest.power >= -deficit or largest.power.isclose(-deficit):
                    excess_reserved[largest.inverter_ids] += deficit
                    deficits[inverter_ids] = Power.zero()
                    deficit = Power.zero()
                else:
                    deficit += excess_reserved[largest.inverter_ids]
                    deficits[inverter_ids] = deficit
                    excess_reserved[largest.inverter_ids] = Power.zero()
            if deficit < self._MINIMUM_DEFICIT_TO_CONSIDER:
                left_over = power - distributed_power
                if left_over > -deficit:
                    distributed_power += deficit
                elif left_over > Power.zero():
                    distributed_power += left_over

        for inverter_ids, excess in excess_reserved.items():
            distributed_power += excess
            battery_power = distribution[inverter_ids]
            battery_power.power += excess
            # Add excess power to the inverter set
            distribution[inverter_ids] = battery_power

        left_over = power - distributed_power

        distribution, left_over = self._greedy_distribute_remaining_power(
            distribution, left_over
        )
        inverter_distribution = self._distribute_multi_inverter_pairs(
            distribution, excl_bounds, incl_bounds
        )

        return DistributionResult(
            distribution=inverter_distribution, remaining_power=left_over
        )

    def _distribute_multi_inverter_pairs(
        self,
        distribution: dict[_InverterSet, _Power],
        excl_bounds: dict[ComponentId, Power],
        incl_bounds: dict[ComponentId, Power],
    ) -> dict[ComponentId, Power]:
        """Distribute power between inverters in a set for a single pair.

        Args:
            distribution: distribution with key: inverter_ids, value: (battery_id, power)
            excl_bounds: exclusion bounds for inverters and batteries
            incl_bounds: inclusion bounds for inverters and batteries

        Returns:
            Return the power for each inverter in given distribution.
        """
        new_distribution: dict[ComponentId, Power] = {}

        for inverter_ids, power in distribution.items():
            if len(inverter_ids) == 1:
                inverter_id = next(iter(inverter_ids))
                new_distribution[inverter_id] = power.power
            else:
                remaining_power = power.power

                # Sort inverters to have the largest exclusion bounds first
                sorted_inverter_ids = sorted(
                    inverter_ids, key=lambda inv_id: excl_bounds[inv_id], reverse=True
                )

                for inverter_id in sorted_inverter_ids:
                    if (
                        not remaining_power.isclose(Power.zero())
                        and excl_bounds[inverter_id] <= remaining_power
                    ):
                        new_power = min(incl_bounds[inverter_id], remaining_power)

                        new_distribution[inverter_id] = new_power
                        remaining_power -= new_power
                    else:
                        new_distribution[inverter_id] = Power.zero()

        return new_distribution

    def _greedy_distribute_remaining_power(
        self, distribution: dict[_InverterSet, _Power], remaining_power: Power
    ) -> tuple[dict[_InverterSet, _Power], Power]:
        """Add remaining power greedily to the given distribution.

        Distribution for each inverter will not exceed its upper bound.

        Args:
            distribution: distribution
            remaining_power: power to distribute

        Returns:
            Return the new distribution and remaining power.
        """
        if remaining_power.isclose(Power.zero()):
            return distribution, remaining_power

        for inverter_ids, power in distribution.items():
            # The power.power == 0 means the inverter shall not be used due to
            # SoC bounds or no capacity
            if remaining_power.isclose(Power.zero()) or power.power.isclose(
                Power.zero()
            ):
                distribution[inverter_ids] = power
            else:
                additional_power = min(power.upper_bound - power.power, remaining_power)
                power.power += additional_power
                remaining_power -= additional_power

        return distribution, remaining_power

    def distribute_power_equally(
        self, power: Power, inverters: set[ComponentId]
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
            remaining_power=Power.zero(),
        )

    def distribute_power(
        self, power: Power, components: list[InvBatPair]
    ) -> DistributionResult:
        """Distribute given power between given components.

        Args:
            power: Power to distribute
            components: InvBatPaired components data. Each pair should have data
                for battery and adjacent inverter.

        Returns:
            Distribution result
        """
        if power.isclose(Power.zero()):
            return DistributionResult(
                distribution={
                    inverter.component_id: Power.zero()
                    for _, inverters in components
                    for inverter in inverters
                },
                remaining_power=Power.zero(),
            )
        if power > Power.zero():
            return self._distribute_consume_power(power, components)
        return self._distribute_supply_power(power, components)

    def _distribute_consume_power(
        self, power: Power, components: list[InvBatPair]
    ) -> DistributionResult:
        """Distribute power between the given components.

        Distribute power in a way that the SoC level between given components will:
            * stay on the same level, equal in all given components
            * will try to align himself to the same level.

        Args:
            power: power to distribute
            components: list of components between which the power should be
                distributed.

        Returns:
            Distribution result, batteries with no SoC and capacity won't be used.
        """
        # If SoC exceeded bound then remaining SoC should be 0.
        # Otherwise algorithm would try to supply power from that battery
        # in order to keep equal SoC level.
        available_soc: dict[ComponentId, float] = {}
        for battery, _ in components:
            available_soc[battery.component_id] = max(
                0.0, battery.soc_upper_bound - battery.soc
            )

        incl_bounds, excl_bounds = self._inclusion_exclusion_bounds(
            components, supply=False
        )

        return self._distribute_power(
            components=components,
            power=power,
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

    def _distribute_supply_power(
        self, power: Power, components: list[InvBatPair]
    ) -> DistributionResult:
        """Distribute power between the given components.

        Distribute power in a way that the SoC level between given components will:
            * stay on the same level, equal in all given components
            * will try to align himself to the same level.

        Args:
            power: power to distribute
            components: list of components between which the power should be
                distributed.

        Returns:
            Distribution result.
        """
        available_soc: dict[ComponentId, float] = {}
        for battery, _ in components:
            available_soc[battery.component_id] = max(
                0.0, battery.soc - battery.soc_lower_bound
            )

        incl_bounds, excl_bounds = self._inclusion_exclusion_bounds(
            components, supply=True
        )

        result: DistributionResult = self._distribute_power(
            components=components,
            power=-power,
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for inverter_id in result.distribution.keys():
            result.distribution[inverter_id] = -result.distribution[inverter_id]
        result.remaining_power = -result.remaining_power

        return result

    def _inclusion_exclusion_bounds(
        self, components: list[InvBatPair], supply: bool = False
    ) -> tuple[dict[ComponentId, Power], dict[ComponentId, Power]]:
        """Calculate inclusion and exclusion bounds for given components.

        Inverter exclusion bounds are _not_ adjusted to battery inclusion
        bounds, as the battery exclusion bounds can be satisfied by multiple
        inverters with lower exclusion bounds.

        Args:
            components: list of components.
            supply: if True then supply bounds will be calculated, otherwise
                consume bounds.

        Returns:
            inclusion and exclusion bounds.
        """
        incl_bounds: dict[ComponentId, Power] = {}
        excl_bounds: dict[ComponentId, Power] = {}
        for battery, inverters in components:
            if supply:
                excl_bounds[battery.component_id] = (
                    -battery.power_bounds.exclusion_lower
                )
                incl_bounds[battery.component_id] = (
                    -battery.power_bounds.inclusion_lower
                )
            else:
                excl_bounds[battery.component_id] = battery.power_bounds.exclusion_upper
                incl_bounds[battery.component_id] = battery.power_bounds.inclusion_upper

            for inverter in inverters:
                if supply:
                    incl_bounds[inverter.component_id] = -max(
                        Power.from_watts(inverter.active_power_inclusion_lower_bound),
                        battery.power_bounds.inclusion_lower,
                    )
                    excl_bounds[inverter.component_id] = Power.from_watts(
                        -inverter.active_power_exclusion_lower_bound
                    )

                else:
                    incl_bounds[inverter.component_id] = min(
                        Power.from_watts(inverter.active_power_inclusion_upper_bound),
                        battery.power_bounds.inclusion_upper,
                    )
                    excl_bounds[inverter.component_id] = Power.from_watts(
                        inverter.active_power_exclusion_upper_bound
                    )
        return incl_bounds, excl_bounds
