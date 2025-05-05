# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

# pylint: disable=too-many-lines
"""Tests for distribution algorithm."""
import math
from collections.abc import Sequence, Set
from dataclasses import dataclass
from datetime import datetime, timezone

from frequenz.client.microgrid import BatteryData, InverterData
from frequenz.quantities import Power
from pytest import approx, raises

from frequenz.sdk.microgrid._power_distributing._distribution_algorithm import (
    AggregatedBatteryData,
    BatteryDistributionAlgorithm,
    DistributionResult,
    InvBatPair,
)
from frequenz.sdk.microgrid._power_distributing.result import PowerBounds

from ...utils.component_data_wrapper import BatteryDataWrapper, InverterDataWrapper


@dataclass
class Bound:
    """Class to create protobuf Bound."""

    lower: float
    upper: float


@dataclass
class Metric:
    """Class to create protobuf Metric."""

    now: float | None
    bound: Bound | None = None


def battery_msg(  # pylint: disable=too-many-arguments
    component_id: int,
    capacity: Metric,
    soc: Metric,
    power: PowerBounds,
    timestamp: datetime = datetime.now(timezone.utc),
) -> BatteryData:
    """Create protobuf battery components with given arguments.

    Args:
        component_id: id of that component
        capacity: capacity
        soc: soc
        power: power bounds
        timestamp: timestamp of the message

    Returns:
        Protobuf battery component with data above
    """
    return BatteryDataWrapper(
        component_id=component_id,
        capacity=capacity.now if capacity.now is not None else math.nan,
        soc=soc.now if soc.now is not None else math.nan,
        soc_lower_bound=soc.bound.lower if soc.bound is not None else math.nan,
        soc_upper_bound=soc.bound.upper if soc.bound is not None else math.nan,
        power_inclusion_lower_bound=power.inclusion_lower.as_watts(),
        power_exclusion_lower_bound=power.exclusion_lower.as_watts(),
        power_exclusion_upper_bound=power.exclusion_upper.as_watts(),
        power_inclusion_upper_bound=power.inclusion_upper.as_watts(),
        timestamp=timestamp,
    )


def inverter_msg(
    component_id: int,
    power: PowerBounds,
    timestamp: datetime = datetime.now(timezone.utc),
) -> InverterData:
    """Create protobuf inverter components with given arguments.

    Args:
        component_id: id of that component
        power: Power bounds
        timestamp: Timestamp from the message

    Returns:
        Protobuf inverter component with data above.
    """
    return InverterDataWrapper(
        component_id=component_id,
        timestamp=timestamp,
        active_power_inclusion_lower_bound=power.inclusion_lower.as_watts(),
        active_power_exclusion_lower_bound=power.exclusion_lower.as_watts(),
        active_power_exclusion_upper_bound=power.exclusion_upper.as_watts(),
        active_power_inclusion_upper_bound=power.inclusion_upper.as_watts(),
    )


def create_components(
    num: int,
    capacity: list[Metric],
    soc: list[Metric],
    power_bounds: list[PowerBounds],
) -> list[InvBatPair]:
    """Create components with given arguments.

    Args:
        num: Number of components
        capacity: Capacity for each battery
        soc: SoC for each battery
        power_bounds: Power bounds for each battery and inverter

    Returns:
        List of the components
    """
    components: list[InvBatPair] = []
    for i in range(0, num):
        battery = battery_msg(2 * i, capacity[i], soc[i], power_bounds[2 * i])
        inverter = inverter_msg(2 * i + 1, power_bounds[2 * i + 1])
        components.append(InvBatPair(AggregatedBatteryData([battery]), [inverter]))
    return components


class TestDistributionAlgorithm:  # pylint: disable=too-many-public-methods
    """Test whether the algorithm works as expected."""

    # pylint: disable=protected-access

    def create_components_with_capacity(
        self,
        num: int,
        capacity: list[float],
        share_inverter: bool = False,
        start_id: int = 0,
    ) -> list[InvBatPair]:
        """Create components with given capacity."""
        components: list[InvBatPair] = []

        shared_inverter: InverterDataWrapper | None = None

        if share_inverter:
            shared_inverter = InverterDataWrapper(
                component_id=start_id + 1, timestamp=datetime.now(tz=timezone.utc)
            )

        for i in range(start_id, num):
            battery_data = BatteryDataWrapper(
                component_id=2 * i,
                timestamp=datetime.now(tz=timezone.utc),
                capacity=capacity[i],
            )

            if shared_inverter:
                inverter_data = shared_inverter
            else:
                inverter_data = InverterDataWrapper(
                    component_id=2 * i + 1, timestamp=datetime.now(tz=timezone.utc)
                )

            components.append(
                InvBatPair(AggregatedBatteryData([battery_data]), [inverter_data])
            )
        return components

    def test_total_capacity_all_0(self) -> None:
        """Raise error if all batteries have no capacity."""
        capacity = [0.0] * 4
        components = self.create_components_with_capacity(4, capacity)
        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        with raises(ValueError):
            algorithm._total_capacity(components)  # pylint: disable=protected-access

    def test_total_capacity(self) -> None:
        """Test if capacity is computed properly."""
        capacity: list[float] = list(range(4))
        components = self.create_components_with_capacity(4, capacity)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._total_capacity(components)
        assert result == approx(sum(list(range(4))))

    def test_distribute_power_one_battery(self) -> None:
        """Distribute power between one battery."""
        capacity: list[float] = [98000]
        components = self.create_components_with_capacity(1, capacity)

        available_soc: dict[int, float] = {0: 40}
        incl_bounds: dict[int, Power] = {
            0: Power.from_watts(500),
            1: Power.from_watts(500),
        }
        excl_bounds: dict[int, Power] = {0: Power.zero(), 1: Power.zero()}

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._distribute_power(  # pylint: disable=protected-access
            components=components,
            power=Power.from_watts(650),
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for key, value in {1: 500}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(150.0)

    def test_distribute_power_two_batteries_1(self) -> None:
        """Test when the batteries has different SoC.

        First battery has two times more SoC to use, so first battery should have more
        power assigned.
        """
        capacity: list[float] = [98000, 98000]
        components = self.create_components_with_capacity(2, capacity)

        available_soc: dict[int, float] = {0: 40, 2: 20}
        incl_bounds: dict[int, Power] = {
            0: Power.from_watts(500),
            2: Power.from_watts(500),
            1: Power.from_watts(500),
            3: Power.from_watts(500),
        }
        excl_bounds: dict[int, Power] = {
            0: Power.zero(),
            2: Power.zero(),
            1: Power.zero(),
            3: Power.zero(),
        }

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._distribute_power(  # pylint: disable=protected-access
            components=components,
            power=Power.from_watts(600),
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for key, value in {1: 400, 3: 200}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_distribute_power_two_batteries_2(self) -> None:
        """Test when the batteries has different SoC.

        First battery has two times less capacity to use, so first
        battery should be have two times less power.
        """
        capacity: list[float] = [49000, 98000]
        components = self.create_components_with_capacity(2, capacity)

        available_soc: dict[int, float] = {0: 20, 2: 20}
        incl_bounds: dict[int, Power] = {
            0: Power.from_watts(500),
            2: Power.from_watts(500),
            1: Power.from_watts(500),
            3: Power.from_watts(500),
        }
        excl_bounds: dict[int, Power] = {
            0: Power.zero(),
            2: Power.zero(),
            1: Power.zero(),
            3: Power.zero(),
        }

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._distribute_power(  # pylint: disable=protected-access
            components=components,
            power=Power.from_watts(600),
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for key, value in {1: 200, 3: 400}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_distribute_power_two_batteries_one_inverter(self) -> None:
        """Test when the batteries has different SoC and only one inverter.

        No matter the differences, the power should be distributed equally,
        because there is only one inverter.
        """
        capacity: list[float] = [49000, 98000]
        components = self.create_components_with_capacity(
            2, capacity, share_inverter=True
        )

        available_soc: dict[int, float] = {0: 20, 2: 30}
        incl_bounds: dict[int, Power] = {
            0: Power.from_watts(500),
            2: Power.from_watts(500),
            1: Power.from_watts(500),
        }
        excl_bounds: dict[int, Power] = {
            0: Power.zero(),
            2: Power.zero(),
            1: Power.zero(),
        }

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._distribute_power(  # pylint: disable=protected-access
            components=components,
            power=Power.from_watts(600),
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for key, value in {1: 500}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(100.0)

    def test_distribute_power_two_batteries_bounds(self) -> None:
        """Test two batteries.

        First battery has two times less capacity, but
        two times more SoC. So the distributed power should be equal
        for each battery.
        """
        capacity: list[float] = [49000, 98000]
        components = self.create_components_with_capacity(2, capacity)

        available_soc: dict[int, float] = {0: 40, 2: 20}
        incl_bounds: dict[int, Power] = {
            0: Power.from_watts(250),
            2: Power.from_watts(330),
            1: Power.from_watts(250),
            3: Power.from_watts(330),
        }
        excl_bounds: dict[int, Power] = {
            0: Power.zero(),
            2: Power.zero(),
            1: Power.zero(),
            3: Power.zero(),
        }

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._distribute_power(  # pylint: disable=protected-access
            components=components,
            power=Power.from_watts(600),
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for key, value in {1: 250, 3: 330}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(20.0)

    def test_distribute_power_three_batteries(self) -> None:
        """Test whether the distribution works ok for more batteries."""
        capacity: list[float] = [49000, 98000, 49000]
        components = self.create_components_with_capacity(3, capacity)

        available_soc: dict[int, float] = {0: 40, 2: 20, 4: 20}
        incl_bounds: dict[int, Power] = {
            0: Power.from_watts(1000),
            2: Power.from_watts(1000),
            4: Power.from_watts(1000),
            1: Power.from_watts(1000),
            3: Power.from_watts(3400),
            5: Power.from_watts(3550),
        }
        excl_bounds: dict[int, Power] = {
            0: Power.zero(),
            2: Power.zero(),
            4: Power.zero(),
            1: Power.zero(),
            3: Power.zero(),
            5: Power.zero(),
        }

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._distribute_power(  # pylint: disable=protected-access
            components=components,
            power=Power.from_watts(1000),
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for key, value in {1: 400, 3: 400, 5: 200}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_distribute_power_three_batteries_2(self) -> None:
        """Test whether the power which couldn't be distributed is correct."""
        capacity: list[float] = [98000, 49000, 49000]
        components = self.create_components_with_capacity(3, capacity)

        available_soc: dict[int, float] = {0: 80, 2: 10, 4: 20}
        incl_bounds: dict[int, Power] = {
            0: Power.from_watts(1000),
            2: Power.from_watts(1000),
            4: Power.from_watts(1000),
            1: Power.from_watts(400),
            3: Power.from_watts(3400),
            5: Power.from_watts(300),
        }
        excl_bounds: dict[int, Power] = {
            0: Power.zero(),
            2: Power.zero(),
            4: Power.zero(),
            1: Power.zero(),
            3: Power.zero(),
            5: Power.zero(),
        }

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._distribute_power(  # pylint: disable=protected-access
            components=components,
            power=Power.from_watts(1000),
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for key, value in {1: 400, 3: 300, 5: 300}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_distribute_power_three_batteries_3(self) -> None:
        """Test with batteries with no capacity."""
        capacity: list[float] = [0, 49000, 0]
        components = self.create_components_with_capacity(3, capacity)

        available_soc: dict[int, float] = {0: 80, 2: 10, 4: 20}
        incl_bounds: dict[int, Power] = {
            0: Power.from_watts(1000),
            2: Power.from_watts(1000),
            4: Power.from_watts(1000),
            1: Power.from_watts(500),
            3: Power.from_watts(300),
            5: Power.from_watts(300),
        }
        excl_bounds: dict[int, Power] = {
            0: Power.zero(),
            2: Power.zero(),
            4: Power.zero(),
            1: Power.zero(),
            3: Power.zero(),
            5: Power.zero(),
        }

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm._distribute_power(  # pylint: disable=protected-access
            components=components,
            power=Power.from_watts(1000),
            available_soc=available_soc,
            incl_bounds=incl_bounds,
            excl_bounds=excl_bounds,
        )

        for key, value in {1: 0, 3: 300, 5: 0}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(700.0)

    # Test distribute supply power
    def test_supply_three_batteries_1(self) -> None:
        """Test distribute supply power for batteries with different SoC."""
        capacity: list[Metric] = [Metric(49000), Metric(49000), Metric(49000)]

        soc: list[Metric] = [
            Metric(20.0, Bound(0, 60)),
            Metric(60.0, Bound(20, 80)),
            Metric(80.0, Bound(20, 80)),
        ]

        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-800), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-700), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-1200), components)

        for key, value in {1: -200, 3: -400, 5: -600}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_supply_three_batteries_2(self) -> None:
        """Test distribute supply power."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(20.0, Bound(0, 50)),
            Metric(60.0, Bound(20, 80)),
            Metric(80.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-800), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-700), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-1400), components)

        for key, value in {1: -400, 3: -400, 5: -600}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_supply_three_batteries_3(self) -> None:
        """Distribute supply power with small upper bounds."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(20.0, Bound(0, 50)),
            Metric(60.0, Bound(20, 80)),
            Metric(80.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        supply_bounds = [
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-100), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-800), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(3, capacity, soc, supply_bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-1400), components)

        for key, value in {1: -500, 3: -100, 5: -800}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_supply_three_batteries_4(self) -> None:
        """Distribute supply power with small upper bounds."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(20.0, Bound(0, 50)),
            Metric(60.0, Bound(20, 80)),
            Metric(80.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-100), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-800), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-1700), components)

        for key, value in {1: -600, 3: -100, 5: -800}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(-200.0)

    def test_supply_three_batteries_5(self) -> None:
        """Test no capacity."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(0.0)]
        soc: list[Metric] = [
            Metric(20.0, Bound(40, 90)),
            Metric(60.0, Bound(20, 80)),
            Metric(80.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        supply_bounds = [
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-100), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-800), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-900), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(3, capacity, soc, supply_bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-1700), components)

        for key, value in {1: 0, 3: -100, 5: 0}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(-1600.0)

    def test_supply_two_batteries_1(self) -> None:
        """Distribute supply power between two batteries."""
        capacity: list[Metric] = [Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(25.0, Bound(0, 80)),
            Metric(25.0, Bound(20, 80)),
        ]

        # consume bounds == 0 makes sure they are not used in supply algorithm
        supply_bounds = [
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(2, capacity, soc, supply_bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-600), components)

        for key, value in {1: -500, 3: -100}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_supply_two_batteries_2(self) -> None:
        """Distribute supply power between two batteries."""
        capacity: list[Metric] = [Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(75.0, Bound(0, 80)),
            Metric(75.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        supply_bounds = [
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-600), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-1000), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(2, capacity, soc, supply_bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-600), components)

        for key, value in {1: -346.1538, 3: -253.8461}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    # Test consumption power distribution
    def test_consumption_three_batteries_1(self) -> None:
        """Distribute consume power with three batteries with the same capacity."""
        capacity: list[Metric] = [Metric(49000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(80.0, Bound(0, 100)),
            Metric(40.0, Bound(20, 80)),
            Metric(20.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(800)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(700)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(1200), components)

        for key, value in {1: 200, 3: 400, 5: 600}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_consumption_three_batteries_2(self) -> None:
        """Distribute consume power with three batteries but one with double the capacity."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(80.0, Bound(0, 100)),
            Metric(40.0, Bound(20, 80)),
            Metric(20.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(800)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(700)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(1400), components)

        for key, value in {1: 400, 3: 400, 5: 600}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_consumption_three_batteries_3(self) -> None:
        """Distribute consume power with small bounds."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(80.0, Bound(0, 100)),
            Metric(40.0, Bound(20, 80)),
            Metric(20.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(100)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(800)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(1400), components)

        for key, value in {1: 500, 3: 100, 5: 800}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_consumption_three_batteries_4(self) -> None:
        """Distribute consume power with small upper bounds."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(80.0, Bound(0, 100)),
            Metric(40.0, Bound(20, 80)),
            Metric(20.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(100)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(800)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(1700), components)

        for key, value in {1: 600, 3: 100, 5: 800}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(200.0)

    def test_consumption_three_batteries_5(self) -> None:
        """Test what if some batteries has invalid SoC and capacity."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(0.0)]
        soc: list[Metric] = [
            Metric(80.0, Bound(0, 50)),
            Metric(40.0, Bound(20, 80)),
            Metric(20.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(100)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(800)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(1700), components)

        for key, value in {1: 0, 3: 100, 5: 0}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(1600.0)

    def test_consumption_three_batteries_6(self) -> None:
        """Distribute consume power."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(50.0, Bound(0, 50)),
            Metric(40.0, Bound(20, 80)),
            Metric(20.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(100)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(800)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(1700), components)

        for key, value in {1: 0, 3: 100, 5: 800}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(800.0)

    def test_consumption_three_batteries_7(self) -> None:
        """Distribute consume power."""
        capacity: list[Metric] = [Metric(98000), Metric(49000), Metric(49000)]
        soc: list[Metric] = [
            Metric(20.0, Bound(0, 80)),
            Metric(79.6, Bound(20, 80)),
            Metric(80.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(500)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(700)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(800)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(900)
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(500), components)

        for key, value in {1: 498.3388, 3: 1.661129, 5: 0}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_consumption_two_batteries_1(self) -> None:
        """Distribute consume power."""
        capacity: list[Metric] = [Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(75.0, Bound(20, 80)),
            Metric(75.0, Bound(0, 100)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(600)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(1000)
            ),
        ]
        components = create_components(2, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(600), components)

        for key, value in {1: 100, 3: 500}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_consumption_two_batteries_distribution_exponent(self) -> None:
        """Distribute consume power."""
        capacity: list[Metric] = [Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(70.0, Bound(20, 80)),
            Metric(50.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
        ]
        components = create_components(2, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(8000), components)

        for key, value in {1: 2000.0, 3: 6000.0}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

        algorithm2 = BatteryDistributionAlgorithm(distributor_exponent=2)
        result2 = algorithm2.distribute_power(Power.from_watts(8000), components)

        for key, value in {1: 800.0, 3: 7200.0}.items():
            assert result2.distribution[key].as_watts() == approx(value)

        assert result2.remaining_power.as_watts() == approx(0.0)

        algorithm3 = BatteryDistributionAlgorithm(distributor_exponent=3)
        result3 = algorithm3.distribute_power(Power.from_watts(8000), components)

        for key, value in {1: 285.7142, 3: 7714.2857}.items():
            assert result3.distribution[key].as_watts() == approx(value)

        assert result3.remaining_power.as_watts() == approx(0.0)

    def test_consumption_two_batteries_distribution_exponent_1(self) -> None:
        """Distribute consume power."""
        capacity: list[Metric] = [Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(50.0, Bound(20, 80)),
            Metric(20.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
        ]
        components = create_components(2, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(900), components)

        for key, value in {1: 300.0, 3: 600.0}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(8000), components)

        for key, value in {1: 2666.6666, 3: 5333.3333}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

        algorithm2 = BatteryDistributionAlgorithm(distributor_exponent=2)
        result2 = algorithm2.distribute_power(Power.from_watts(900), components)

        for key, value in {1: 180, 3: 720}.items():
            assert result2.distribution[key].as_watts() == approx(value)

        assert result2.remaining_power.as_watts() == approx(0.0)

        algorithm2 = BatteryDistributionAlgorithm(distributor_exponent=2)
        result2 = algorithm2.distribute_power(Power.from_watts(8000), components)

        for key, value in {1: 1600, 3: 6400}.items():
            assert result2.distribution[key].as_watts() == approx(value)

        assert result2.remaining_power.as_watts() == approx(0.0)

        algorithm2 = BatteryDistributionAlgorithm(distributor_exponent=3)
        result2 = algorithm2.distribute_power(Power.from_watts(900), components)

        for key, value in {1: 100, 3: 800}.items():
            assert result2.distribution[key].as_watts() == approx(value)

        assert result2.remaining_power.as_watts() == approx(0.0)

        algorithm3 = BatteryDistributionAlgorithm(distributor_exponent=3)
        result3 = algorithm3.distribute_power(Power.from_watts(8000), components)

        for key, value in {1: 888.8888, 3: 7111.1111}.items():
            assert result3.distribution[key].as_watts() == approx(value)

        assert result3.remaining_power.as_watts() == approx(0.0)

    def test_supply_two_batteries_distribution_exponent(self) -> None:
        """Distribute power."""
        capacity: list[Metric] = [Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(30.0, Bound(20, 80)),
            Metric(50.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(2, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -2000.0, 3: -6000.0}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

        algorithm2 = BatteryDistributionAlgorithm(distributor_exponent=2)
        result2 = algorithm2.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -800, 3: -7200}.items():
            assert result2.distribution[key].as_watts() == approx(value)

        assert result2.remaining_power.as_watts() == approx(0.0)

        algorithm3 = BatteryDistributionAlgorithm(distributor_exponent=3)
        result3 = algorithm3.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -285.7142, 3: -7714.2857}.items():
            assert result3.distribution[key].as_watts() == approx(value)

        assert result3.remaining_power.as_watts() == approx(0.0)

    def test_supply_two_batteries_distribution_exponent_1(self) -> None:
        """Distribute power."""
        capacity: list[Metric] = [Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(50.0, Bound(20, 80)),
            Metric(80.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        supply_bounds = [
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(2, capacity, soc, supply_bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -2666.6666, 3: -5333.3333}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

        algorithm2 = BatteryDistributionAlgorithm(distributor_exponent=2)
        result2 = algorithm2.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -1600, 3: -6400}.items():
            assert result2.distribution[key].as_watts() == approx(value)

        assert result2.remaining_power.as_watts() == approx(0.0)

        algorithm3 = BatteryDistributionAlgorithm(distributor_exponent=3)
        result3 = algorithm3.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -888.8888, 3: -7111.1111}.items():
            assert result3.distribution[key].as_watts() == approx(value)

        assert result3.remaining_power.as_watts() == approx(0.0)

    def test_supply_three_batteries_distribution_exponent_2(self) -> None:
        """Distribute power."""
        capacity: list[Metric] = [Metric(98000), Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(50.0, Bound(20, 80)),
            Metric(65.0, Bound(20, 80)),
            Metric(80.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(3, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=1)
        result = algorithm.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -1777.7777, 3: -2666.6666, 5: -3555.5555}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

        algorithm2 = BatteryDistributionAlgorithm(distributor_exponent=2)
        result2 = algorithm2.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -1103.4482, 3: -2482.7586, 5: -4413.7931}.items():
            assert result2.distribution[key].as_watts() == approx(value)

        assert result2.remaining_power.as_watts() == approx(0.0)

        algorithm3 = BatteryDistributionAlgorithm(distributor_exponent=3)
        result3 = algorithm3.distribute_power(Power.from_watts(-8000), components)

        for key, value in {1: -646.4646, 3: -2181.8181, 5: -5171.7171}.items():
            assert result3.distribution[key].as_watts() == approx(value)

        assert result3.remaining_power.as_watts() == approx(0.0)

    def test_supply_three_batteries_distribution_exponent_3(self) -> None:
        """Distribute power."""
        capacity: list[Metric] = [Metric(98000), Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(56.0, Bound(20, 80)),  # available SoC 36
            Metric(36.0, Bound(20, 80)),  # available SoC 16
            Metric(29.0, Bound(20, 80)),  # available SoC 9
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        supply_bounds = [
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
            PowerBounds(
                Power.from_watts(-9000), Power.zero(), Power.zero(), Power.zero()
            ),
        ]
        components = create_components(3, capacity, soc, supply_bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=0.5)
        result = algorithm.distribute_power(Power.from_watts(-1300), components)

        for key, value in {1: -600, 3: -400, 5: -300}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=0)
        result = algorithm.distribute_power(Power.from_watts(-1200), components)

        for key, value in {1: -400, 3: -400, 5: -400}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

    def test_supply_two_batteries_distribution_exponent_less_then_1(self) -> None:
        """Distribute power."""
        capacity: list[Metric] = [Metric(98000), Metric(98000)]
        soc: list[Metric] = [
            Metric(44.0, Bound(20, 80)),
            Metric(64.0, Bound(20, 80)),
        ]
        # consume bounds == 0 makes sure they are not used in supply algorithm
        bounds = [
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
            PowerBounds(
                Power.zero(), Power.zero(), Power.zero(), Power.from_watts(9000)
            ),
        ]
        components = create_components(2, capacity, soc, bounds)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=0.5)
        result = algorithm.distribute_power(Power.from_watts(1000), components)

        for key, value in {1: 600, 3: 400}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)

        algorithm = BatteryDistributionAlgorithm(distributor_exponent=0)
        result = algorithm.distribute_power(Power.from_watts(1000), components)

        for key, value in {1: 500, 3: 500}.items():
            assert result.distribution[key].as_watts() == approx(value)

        assert result.remaining_power.as_watts() == approx(0.0)


class TestDistWithExclBounds:
    """Test the distribution algorithm with exclusive bounds."""

    @staticmethod
    def assert_result(result: DistributionResult, expected: DistributionResult) -> None:
        """Assert the result is as expected."""
        for key, value in expected.distribution.items():
            assert result.distribution[key].as_watts() == approx(
                value.as_watts(), abs=0.01
            )
        assert result.remaining_power.as_watts() == approx(
            expected.remaining_power.as_watts(), abs=0.01
        )

    @staticmethod
    def assert_any_result(
        result: DistributionResult,
        expected_distribution_ids: Set[int],
        expected_distribution_powers: Sequence[Power],
        expected_remaining_power: Power,
    ) -> None:
        """Assert the result is as expected, disregarding which power goes to which component."""
        assert result.remaining_power == expected_remaining_power
        assert {*result.distribution.keys()} == expected_distribution_ids
        assert list(sorted(result.distribution.values())) == list(
            sorted(expected_distribution_powers)
        )

    def test_scenario_1(self) -> None:
        """Test scenario 1.

        Set params for 3 batteries:
            capacities: 10000, 10000, 10000
            socs: 30, 50, 70

            individual soc bounds: 10-90
            individual bounds: -1000, -100, 100, 1000

            battery pool bounds: -3000, -300, 300, 1000

        Expected result:

        | request |                        |    excess |
        |   power | distribution           | remaining |
        |---------+------------------------+-----------|
        |    -300 | -100, -100, -100       |         0 |
        |     300 | 100, 100, 100          |         0 |
        |    -600 | -100, -200, -300       |         0 |
        |     900 | 466.66, 300, 133.33    |         0 |
        |    -900 | -133.33, -300, -466.66 |         0 |
        |    2200 | 1000, 850, 350         |         0 |
        |   -2200 | -350, -850, -1000      |         0 |
        |    2800 | 1000, 1000, 800        |         0 |
        |   -2800 | -800, -1000, -1000     |         0 |
        |    3800 | 1000, 1000, 1000       |       800 |
        |   -3200 | -1000, -1000, -1000    |      -200 |

        """
        capacities: list[Metric] = [Metric(10000), Metric(10000), Metric(10000)]
        soc: list[Metric] = [
            Metric(30.0, Bound(10, 90)),
            Metric(50.0, Bound(10, 90)),
            Metric(70.0, Bound(10, 90)),
        ]
        bounds = [
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-100),
                Power.from_watts(20),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-90),
                Power.from_watts(100),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-50),
                Power.from_watts(100),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-100),
                Power.from_watts(90),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-20),
                Power.from_watts(100),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-100),
                Power.from_watts(80),
                Power.from_watts(1000),
            ),
        ]
        components = create_components(3, capacities, soc, bounds)

        algorithm = BatteryDistributionAlgorithm()

        self.assert_result(
            algorithm.distribute_power(Power.zero(), components),
            DistributionResult(
                {1: Power.zero(), 3: Power.zero(), 5: Power.zero()},
                remaining_power=Power.zero(),
            ),
        )

        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-300), components),
            DistributionResult(
                {
                    1: Power.from_watts(-100),
                    3: Power.from_watts(-100),
                    5: Power.from_watts(-100),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(300), components),
            DistributionResult(
                {
                    1: Power.from_watts(100),
                    3: Power.from_watts(100),
                    5: Power.from_watts(100),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-600), components),
            DistributionResult(
                {
                    1: Power.from_watts(-100),
                    3: Power.from_watts(-200),
                    5: Power.from_watts(-300),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(900), components),
            DistributionResult(
                {
                    1: Power.from_watts(450),
                    3: Power.from_watts(300),
                    5: Power.from_watts(150),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-900), components),
            DistributionResult(
                {
                    1: Power.from_watts(-150),
                    3: Power.from_watts(-300),
                    5: Power.from_watts(-450),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(2200), components),
            DistributionResult(
                {
                    1: Power.from_watts(1000),
                    3: Power.from_watts(833.33),
                    5: Power.from_watts(366.66),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-2200), components),
            DistributionResult(
                {
                    1: Power.from_watts(-366.66),
                    3: Power.from_watts(-833.33),
                    5: Power.from_watts(-1000),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(2800), components),
            DistributionResult(
                {
                    1: Power.from_watts(1000),
                    3: Power.from_watts(1000),
                    5: Power.from_watts(800),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-2800), components),
            DistributionResult(
                {
                    1: Power.from_watts(-800),
                    3: Power.from_watts(-1000),
                    5: Power.from_watts(-1000),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(3800), components),
            DistributionResult(
                {
                    1: Power.from_watts(1000),
                    3: Power.from_watts(1000),
                    5: Power.from_watts(1000),
                },
                remaining_power=Power.from_watts(800.0),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-3200), components),
            DistributionResult(
                {
                    1: Power.from_watts(-1000),
                    3: Power.from_watts(-1000),
                    5: Power.from_watts(-1000),
                },
                remaining_power=Power.from_watts(-200.0),
            ),
        )

    def test_scenario_2(self) -> None:
        """Test scenario 2.

        Set params for 3 batteries:
            capacities: 10000, 10000, 10000
            socs: 50, 50, 70

            individual soc bounds: 10-90
            individual bounds: -1000, -100, 100, 1000

            battery pool bounds: -3000, -300, 300, 1000

        Expected result:

        | request |                           |    excess |
        |   power | distribution              | remaining |
        |---------+---------------------------+-----------|
        |    -300 | -100, -100, -100          |         0 |
        |     300 | 100, 100, 100             |         0 |
        |    -530 | -151.42, -151.42, -227.14 |         0 |
        |     530 | 212, 212, 106             |         0 |
        |    2000 | 800, 800, 400             |         0 |
        |   -2000 | -571.42, -571.42, -857.14 |         0 |
        |    2500 | 1000, 1000, 500           |         0 |
        |   -2500 | -785.71, -714.28, -1000.0 |         0 |
        |    3000 | 1000, 1000, 1000          |         0 |
        |   -3000 | -1000, -1000, -1000       |         0 |
        |    3500 | 1000, 1000, 1000          |       500 |
        |   -3500 | -1000, -1000, -1000       |      -500 |
        """
        capacities: list[Metric] = [Metric(10000), Metric(10000), Metric(10000)]
        soc: list[Metric] = [
            Metric(50.0, Bound(10, 90)),
            Metric(50.0, Bound(10, 90)),
            Metric(70.0, Bound(10, 90)),
        ]
        bounds = [
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-100),
                Power.from_watts(20),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-90),
                Power.from_watts(100),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-50),
                Power.from_watts(100),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-100),
                Power.from_watts(90),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-20),
                Power.from_watts(100),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-100),
                Power.from_watts(80),
                Power.from_watts(1000),
            ),
        ]
        components = create_components(3, capacities, soc, bounds)

        algorithm = BatteryDistributionAlgorithm()

        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-300), components),
            DistributionResult(
                {
                    1: Power.from_watts(-100),
                    3: Power.from_watts(-100),
                    5: Power.from_watts(-100),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(300), components),
            DistributionResult(
                {
                    1: Power.from_watts(100),
                    3: Power.from_watts(100),
                    5: Power.from_watts(100),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-530), components),
            DistributionResult(
                {
                    1: Power.from_watts(-151.42),
                    3: Power.from_watts(-151.42),
                    5: Power.from_watts(-227.14),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(530), components),
            DistributionResult(
                {
                    1: Power.from_watts(212),
                    3: Power.from_watts(212),
                    5: Power.from_watts(106),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(2000), components),
            DistributionResult(
                {
                    1: Power.from_watts(800),
                    3: Power.from_watts(800),
                    5: Power.from_watts(400),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-2000), components),
            DistributionResult(
                {
                    1: Power.from_watts(-571.42),
                    3: Power.from_watts(-571.42),
                    5: Power.from_watts(-857.14),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(2500), components),
            DistributionResult(
                {
                    1: Power.from_watts(1000),
                    3: Power.from_watts(1000),
                    5: Power.from_watts(500),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-2500), components),
            DistributionResult(
                {
                    1: Power.from_watts(-785.71),
                    3: Power.from_watts(-714.28),
                    5: Power.from_watts(-1000.0),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(3000), components),
            DistributionResult(
                {
                    1: Power.from_watts(1000),
                    3: Power.from_watts(1000),
                    5: Power.from_watts(1000),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-3000), components),
            DistributionResult(
                {
                    1: Power.from_watts(-1000),
                    3: Power.from_watts(-1000),
                    5: Power.from_watts(-1000),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(3500), components),
            DistributionResult(
                {
                    1: Power.from_watts(1000),
                    3: Power.from_watts(1000),
                    5: Power.from_watts(1000),
                },
                remaining_power=Power.from_watts(500.0),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-3500), components),
            DistributionResult(
                {
                    1: Power.from_watts(-1000),
                    3: Power.from_watts(-1000),
                    5: Power.from_watts(-1000),
                },
                remaining_power=Power.from_watts(-500.0),
            ),
        )

    def test_scenario_3(self) -> None:
        """Test scenario 3.

        Set params for 3 batteries:
            capacities: 10000, 10000, 10000
            socs: 50, 50, 70

            individual soc bounds: 10-90
            individual bounds 1: -1000, 0, 0, 1000
            individual bounds 2: -1000, -100, 100, 1000
            individual bounds 3: -1000, 0, 0, 1000

            battery pool bounds: -3000, -100, 100, 1000

        Expected result:

        | request |                           |    excess |
        |   power | distribution              | remaining |
        |---------+---------------------------+-----------|
        |    -320 | -88, -108.57, -123.43     |         0 |
        |     320 | 128, 128, 64              |         0 |
        |   -1800 | -514.28, -514.28, -771.42 |         0 |
        |    1800 | 720, 720, 360             |         0 |
        |   -2800 | -800, -1000, -1000        |         0 |
        |    2800 | 1000, 1000, 800           |         0 |
        |   -3500 | -1000, -1000, -1000       |      -500 |
        |    3500 | 1000, 1000, 1000          |       500 |
        """
        capacities: list[Metric] = [Metric(10000), Metric(10000), Metric(10000)]
        soc: list[Metric] = [
            Metric(50.0, Bound(10, 90)),
            Metric(50.0, Bound(10, 90)),
            Metric(70.0, Bound(10, 90)),
        ]
        bounds = [
            PowerBounds(
                Power.from_watts(-1000),
                Power.zero(),
                Power.zero(),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.zero(),
                Power.zero(),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-100),
                Power.from_watts(100),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.from_watts(-100),
                Power.from_watts(100),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.zero(),
                Power.zero(),
                Power.from_watts(1000),
            ),
            PowerBounds(
                Power.from_watts(-1000),
                Power.zero(),
                Power.zero(),
                Power.from_watts(1000),
            ),
        ]
        components = create_components(3, capacities, soc, bounds)

        algorithm = BatteryDistributionAlgorithm()

        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-320), components),
            DistributionResult(
                {
                    1: Power.from_watts(-88),
                    3: Power.from_watts(-108.57),
                    5: Power.from_watts(-123.43),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(320), components),
            DistributionResult(
                {
                    1: Power.from_watts(128),
                    3: Power.from_watts(128),
                    5: Power.from_watts(64),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-1800), components),
            DistributionResult(
                {
                    1: Power.from_watts(-514.28),
                    3: Power.from_watts(-514.28),
                    5: Power.from_watts(-771.42),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(1800), components),
            DistributionResult(
                {
                    1: Power.from_watts(720),
                    3: Power.from_watts(720),
                    5: Power.from_watts(360),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-2800), components),
            DistributionResult(
                {
                    1: Power.from_watts(-800),
                    3: Power.from_watts(-1000),
                    5: Power.from_watts(-1000),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(2800), components),
            DistributionResult(
                {
                    1: Power.from_watts(1000),
                    3: Power.from_watts(1000),
                    5: Power.from_watts(800),
                },
                remaining_power=Power.zero(),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(-3500), components),
            DistributionResult(
                {
                    1: Power.from_watts(-1000),
                    3: Power.from_watts(-1000),
                    5: Power.from_watts(-1000),
                },
                remaining_power=Power.from_watts(-500.0),
            ),
        )
        self.assert_result(
            algorithm.distribute_power(Power.from_watts(3500), components),
            DistributionResult(
                {
                    1: Power.from_watts(1000),
                    3: Power.from_watts(1000),
                    5: Power.from_watts(1000),
                },
                remaining_power=Power.from_watts(500.0),
            ),
        )

    def test_scenario_4(self) -> None:
        """Test scenario 4.

        Set params for 1 battery connected to two inverters:
            capacities: 10000
            socs: 50

            individual soc bounds: 10-90
            battery bounds 1: -1500, -200, 200, 1500
            inverter 1 bounds: -1000, -100, 100, 1000
            inverter 2 bounds: -1000, -100, 100, 1000

            battery pool bounds: -1500, -200, 200, 1500

        Expected result:

        | request |                           |    excess |
        |   power | distribution              | remaining |
        |---------+---------------------------+-----------|
        |    -300 | -300, -0                  |         0 |
        |     300 | 300, 0                    |         0 |
        |   -1800 | -1000, -500               |       300 |
        """
        components = [
            InvBatPair(
                AggregatedBatteryData(
                    [
                        battery_msg(
                            component_id=1,
                            capacity=Metric(10000),
                            soc=Metric(50, Bound(10, 90)),
                            power=PowerBounds(
                                Power.from_watts(-1500),
                                Power.from_watts(-200),
                                Power.from_watts(200),
                                Power.from_watts(1500),
                            ),
                        )
                    ]
                ),
                [
                    inverter_msg(
                        2,
                        PowerBounds(
                            Power.from_watts(-1000),
                            Power.from_watts(-100),
                            Power.from_watts(100),
                            Power.from_watts(1000),
                        ),
                    ),
                    inverter_msg(
                        3,
                        PowerBounds(
                            Power.from_watts(-1000),
                            Power.from_watts(-100),
                            Power.from_watts(100),
                            Power.from_watts(1000),
                        ),
                    ),
                ],
            )
        ]

        algorithm = BatteryDistributionAlgorithm()

        # The assignment of power to each inverter can be swapped, as they both have the
        # same bounds, none will be preferred
        self.assert_any_result(
            algorithm.distribute_power(Power.from_watts(-300), components),
            expected_distribution_ids={2, 3},
            expected_distribution_powers=[Power.from_watts(-300), Power.zero()],
            expected_remaining_power=Power.zero(),
        )

        self.assert_any_result(
            algorithm.distribute_power(Power.from_watts(300), components),
            expected_distribution_ids={2, 3},
            expected_distribution_powers=[Power.from_watts(300), Power.zero()],
            expected_remaining_power=Power.zero(),
        )

        self.assert_any_result(
            algorithm.distribute_power(Power.from_watts(-1800), components),
            expected_distribution_ids={2, 3},
            expected_distribution_powers=[
                Power.from_watts(-1000),
                Power.from_watts(-500),
            ],
            expected_remaining_power=Power.from_watts(-300),
        )

    def test_scenario_5(self) -> None:
        """Test scenario 5.

        Set params for 2 batteries each connected to two inverters:
            capacities: 10000, 10000
            socs: 20, 60

            individual soc bounds: 10-90
            battery bounds 1: -1500, -200, 200, 1500
            inverter 1 bounds: -1000, -100, 100, 1000
            inverter 2 bounds: -1000, -100, 100, 1000

            battery bounds 2: -1500, 0, 0, 1500
            inverter 3 bounds: -1000, -100, 100, 1000
            inverter 4 bounds: -1000, -100, 100, 1000

            battery pool bounds: -3000, -200, 200, 3000

        Expected result:

        | request |                           |    excess |
        |   power | distribution              | remaining |
        |---------+---------------------------+-----------|
        |    -300 | -200, -100, 0, 0          |         0 |
        |     300 | 200, 100, 0, 0            |         0 |
        |   -1800 | -300, -1000, -500         |         0 |
        |    3000 | 1000, 500, 1000, 500      |         0 |
        |    3500 | 1000, 500, 1000, 500      |       500 |

        """
        components = [
            InvBatPair(
                AggregatedBatteryData(
                    [
                        battery_msg(
                            component_id=1,
                            capacity=Metric(10000),
                            soc=Metric(20, Bound(10, 90)),
                            power=PowerBounds(
                                Power.from_watts(-1500),
                                Power.from_watts(-200),
                                Power.from_watts(200),
                                Power.from_watts(1500),
                            ),
                        )
                    ]
                ),
                [
                    inverter_msg(
                        10,
                        PowerBounds(
                            Power.from_watts(-1000),
                            Power.from_watts(-100),
                            Power.from_watts(100),
                            Power.from_watts(1000),
                        ),
                    ),
                    inverter_msg(
                        11,
                        PowerBounds(
                            Power.from_watts(-1000),
                            Power.from_watts(-100),
                            Power.from_watts(100),
                            Power.from_watts(1000),
                        ),
                    ),
                ],
            ),
            InvBatPair(
                AggregatedBatteryData(
                    [
                        battery_msg(
                            component_id=2,
                            capacity=Metric(10000),
                            soc=Metric(60, Bound(10, 90)),
                            power=PowerBounds(
                                Power.from_watts(-1500),
                                Power.zero(),
                                Power.zero(),
                                Power.from_watts(1500),
                            ),
                        )
                    ]
                ),
                [
                    inverter_msg(
                        20,
                        PowerBounds(
                            Power.from_watts(-1000),
                            Power.from_watts(-100),
                            Power.from_watts(100),
                            Power.from_watts(1000),
                        ),
                    ),
                    inverter_msg(
                        21,
                        PowerBounds(
                            Power.from_watts(-1000),
                            Power.from_watts(-100),
                            Power.from_watts(100),
                            Power.from_watts(1000),
                        ),
                    ),
                ],
            ),
        ]

        algorithm = BatteryDistributionAlgorithm()

        self.assert_any_result(
            algorithm.distribute_power(Power.from_watts(-300), components),
            expected_distribution_ids={10, 11, 20, 21},
            expected_distribution_powers=[
                Power.from_watts(-200),
                Power.zero(),
                Power.from_watts(-100),
                Power.zero(),
            ],
            expected_remaining_power=Power.zero(),
        )

        self.assert_any_result(
            algorithm.distribute_power(Power.from_watts(300), components),
            expected_distribution_ids={10, 11, 20, 21},
            expected_distribution_powers=[
                Power.from_watts(200),
                Power.zero(),
                Power.from_watts(100),
                Power.zero(),
            ],
            expected_remaining_power=Power.zero(),
        )

        self.assert_any_result(
            algorithm.distribute_power(Power.from_watts(-1800), components),
            expected_distribution_ids={10, 11, 20, 21},
            expected_distribution_powers=[
                Power.from_watts(-300),
                Power.zero(),
                Power.from_watts(-1000),
                Power.from_watts(-500),
            ],
            expected_remaining_power=Power.zero(),
        )

        self.assert_any_result(
            algorithm.distribute_power(Power.from_watts(3000), components),
            expected_distribution_ids={10, 11, 20, 21},
            expected_distribution_powers=[
                Power.from_watts(1000),
                Power.from_watts(500),
                Power.from_watts(1000),
                Power.from_watts(500),
            ],
            expected_remaining_power=Power.zero(),
        )

        self.assert_any_result(
            algorithm.distribute_power(Power.from_watts(3500), components),
            expected_distribution_ids={10, 11, 20, 21},
            expected_distribution_powers=[
                Power.from_watts(1000),
                Power.from_watts(500),
                Power.from_watts(1000),
                Power.from_watts(500),
            ],
            expected_remaining_power=Power.from_watts(500.0),
        )
