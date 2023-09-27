# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""An external interface for the BatteryPool.

Allows for actors interested in operating on the same set of batteries to share
underlying formula engine and metric calculator instances, but without having to specify
their individual priorities with each request.
"""

import asyncio
import uuid
from collections import abc
from datetime import timedelta

from ..._internal._channels import ReceiverFetcher
from ...actor import _power_managing
from ...timeseries import Energy, Percentage, Power, Sample, Temperature
from .._formula_engine import FormulaEngine
from .._formula_engine._formula_generators import (
    BatteryPowerFormula,
    FormulaGeneratorConfig,
    FormulaType,
)
from ._methods import SendOnUpdate
from ._metric_calculator import (
    CapacityCalculator,
    PowerBoundsCalculator,
    SoCCalculator,
    TemperatureCalculator,
)
from ._result_types import PowerMetrics
from .battery_pool import BatteryPool

# pylint: disable=protected-access


class BatteryPoolWrapper:
    """The BatteryPoolWrapper is the external interface for the BatteryPool.

    BatteryPool instances are unique to a set of batteries.  The BatteryPoolWrapper
    provides an abstraction over actor priorities when multiple actors want to use the
    same set of batteries.
    """

    def __init__(self, battery_pool: BatteryPool, source_id: str | None, priority: int):
        """Create a BatteryPoolWrapper instance.

        Args:
            battery_pool: The battery pool to wrap.
            source_id: The source ID to use for the requests made by this wrapper.
            priority: The priority of the actor using this wrapper.
        """
        self._battery_pool = battery_pool
        unique_id = str(uuid.uuid4())
        self._source_id = unique_id if source_id is None else f"{source_id}-{unique_id}"
        self._priority = priority

    async def set_power(
        self,
        preferred_power: Power | None,
        *,
        request_timeout: timedelta = timedelta(seconds=5.0),
        include_broken_batteries: bool = False,
        _bounds: tuple[Power, Power] | None = None,
    ) -> None:
        """Set the given power for the batteries in the pool.

        Power values need to follow the Passive Sign Convention (PSC). That is, positive
        values indicate charge power and negative values indicate discharge power.

        When not using the Passive Sign Convention, the `charge` and `discharge` methods
        might be more convenient.

        The result of the request can be accessed using the receiver returned from the
        `power_distribution_results` method.

        Args:
            preferred_power: The power to set for the batteries in the pool.
            request_timeout: The timeout for the request.
            include_broken_batteries: if True, the power will be set for all batteries
                in the pool, including the broken ones. If False, then the power will be
                set only for the working batteries.  This is not a guarantee that the
                power will be set for all working batteries, as the microgrid API may
                still reject the request.
            _bounds: The power bounds for the request.  These bounds will apply to actors
                with a lower priority, and can be overridden by bounds from actors with
                a higher priority.  If None, the power bounds will be set to the maximum
                power of the batteries in the pool.  This is currently and experimental
                feature.
        """
        await self._battery_pool._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._source_id,
                preferred_power=preferred_power,
                bounds=_bounds,
                battery_ids=self._battery_pool._batteries,
                priority=self._priority,
                request_timeout=request_timeout,
                include_broken_batteries=include_broken_batteries,
            )
        )

    async def charge(
        self,
        power: Power | None,
        *,
        request_timeout: timedelta = timedelta(seconds=5.0),
        include_broken_batteries: bool = False,
    ) -> None:
        """Set the given charge power for the batteries in the pool.

        Power values need to be positive values, indicating charge power.

        When using the Passive Sign Convention, the `set_power` method might be more
        convenient.

        The result of the request can be accessed using the receiver returned from
        the `power_distribution_results` method.

        Args:
            power: Unsigned charge power to set for the batteries in the pool.
            request_timeout: The timeout for the request.
            include_broken_batteries: if True, the power will be set for all batteries
                in the pool, including the broken ones. If False, then the power will be
                set only for the working batteries.  This is not a guarantee that the
                power will be set for all working batteries, as the microgrid API may
                still reject the request.

        Raises:
            ValueError: If the given power is negative.
        """
        if power and power < Power.zero():
            raise ValueError("Charge power must be positive.")
        await self._battery_pool._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._source_id,
                preferred_power=power,
                bounds=None,
                battery_ids=self._battery_pool._batteries,
                priority=0,
                request_timeout=request_timeout,
                include_broken_batteries=include_broken_batteries,
            )
        )

    async def discharge(
        self,
        power: Power | None,
        *,
        request_timeout: timedelta = timedelta(seconds=5.0),
        include_broken_batteries: bool = False,
    ) -> None:
        """Set the given discharge power for the batteries in the pool.

        Power values need to be positive values, indicating discharge power.

        When using the Passive Sign Convention, the `set_power` method might be more
        convenient.

        The result of the request can be accessed using the receiver returned from
        the `power_distribution_results` method.

        Args:
            power: Unsigned discharge power to set for the batteries in the pool.
            request_timeout: The timeout for the request.
            include_broken_batteries: if True, the power will be set for all batteries
                in the pool, including the broken ones. If False, then the power will be
                set only for the working batteries.  This is not a guarantee that the
                power will be set for all working batteries, as the microgrid API may
                still reject the request.

        Raises:
            ValueError: If the given power is negative.
        """
        if power and power < Power.zero():
            raise ValueError("Discharge power must be positive.")
        await self._battery_pool._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._source_id,
                preferred_power=power,
                bounds=None,
                battery_ids=self._battery_pool._batteries,
                priority=0,
                request_timeout=request_timeout,
                include_broken_batteries=include_broken_batteries,
            )
        )

    @property
    def battery_ids(self) -> abc.Set[int]:
        """Return ids of the batteries in the pool.

        Returns:
            Ids of the batteries in the pool
        """
        return self._battery_pool._batteries

    @property
    def power(self) -> FormulaEngine[Power]:
        """Fetch the total power of the batteries in the pool.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate this metric is not already running, it will be
        started.

        A receiver from the formula engine can be obtained by calling the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the total power of all
                batteries in the pool.
        """
        engine = self._battery_pool._formula_pool.from_power_formula_generator(
            "battery_pool_power",
            BatteryPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._battery_pool._batteries,
                formula_type=FormulaType.PASSIVE_SIGN_CONVENTION,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def production_power(self) -> FormulaEngine[Power]:
        """Fetch the total production power of the batteries in the pool.

        This formula produces positive values when producing power and 0 otherwise.

        If a formula engine to calculate this metric is not already running, it will be
        started.

        A receiver from the formula engine can be obtained by calling the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the total production power of
                all batteries in the pool.
        """
        engine = self._battery_pool._formula_pool.from_power_formula_generator(
            "battery_pool_production_power",
            BatteryPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._battery_pool._batteries,
                formula_type=FormulaType.PRODUCTION,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def consumption_power(self) -> FormulaEngine[Power]:
        """Fetch the total consumption power of the batteries in the pool.

        This formula produces positive values when consuming power and 0 otherwise.

        If a formula engine to calculate this metric is not already running, it will be
        started.

        A receiver from the formula engine can be obtained by calling the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the total consumption
                power of all batteries in the pool.
        """
        engine = self._battery_pool._formula_pool.from_power_formula_generator(
            "battery_pool_consumption_power",
            BatteryPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._battery_pool._batteries,
                formula_type=FormulaType.CONSUMPTION,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def soc(self) -> ReceiverFetcher[Sample[Percentage]]:
        """Fetch the normalized average weighted-by-capacity SoC values for the pool.

        The SoC values are normalized to the 0-100% range and clamped if they are out
        of bounds. Only values from working batteries with operational inverters are
        considered in the calculation.

        Average SoC is calculated using the formula:
        ```
        working_batteries: Set[BatteryData] # working batteries from the battery pool

        soc_scaled = min(max(
            0,
            (soc - soc_lower_bound) / (soc_upper_bound - soc_lower_bound) * 100,
        ), 100)
        used_capacity = sum(
            battery.usable_capacity * battery.soc_scaled
            for battery in working_batteries
        )
        total_capacity = sum(battery.usable_capacity for battery in working_batteries)
        average_soc = used_capacity/total_capacity
        ```

        `None` values will be sent if there are no working batteries with operational
        inverters to calculate the metric with.

        A receiver from the MetricAggregator can be obtained by calling the
        `new_receiver` method.

        Returns:
            A MetricAggregator that will calculate and stream the aggregate SoC of all
                batteries in the pool, considering only working batteries with
                operational inverters.
        """
        method_name = SendOnUpdate.name() + "_" + SoCCalculator.name()

        if method_name not in self._battery_pool._active_methods:
            calculator = SoCCalculator(self._battery_pool._batteries)
            self._battery_pool._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._battery_pool._working_batteries,
                min_update_interval=self._battery_pool._min_update_interval,
            )

        return self._battery_pool._active_methods[method_name]

    @property
    def temperature(self) -> ReceiverFetcher[Sample[Temperature]]:
        """Fetch the average temperature of the batteries in the pool.

        Returns:
            A MetricAggregator that will calculate and stream the average temperature
                of all batteries in the pool.
        """
        method_name = SendOnUpdate.name() + "_" + TemperatureCalculator.name()
        if method_name not in self._battery_pool._active_methods:
            calculator = TemperatureCalculator(self._battery_pool._batteries)
            self._battery_pool._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._battery_pool._working_batteries,
                min_update_interval=self._battery_pool._min_update_interval,
            )
        return self._battery_pool._active_methods[method_name]

    @property
    def capacity(self) -> ReceiverFetcher[Sample[Energy]]:
        """Get a receiver to receive new capacity metrics when they change.

        The reported capacity values consider only working batteries with operational
        inverters.

        Calculated with the formula:
        ```
        working_batteries: Set[BatteryData] # working batteries from the battery pool
        total_capacity = sum(
            battery.capacity * (soc_upper_bound - soc_lower_bound) / 100
            for battery in working_batteries
        )
        ```

        `None` will be sent if there are no working batteries with operational
        inverters to calculate metrics.

        A receiver from the MetricAggregator can be obtained by calling the
        `new_receiver` method.

        Returns:
            A MetricAggregator that will calculate and stream the capacity of all
                batteries in the pool, considering only working batteries with
                operational inverters.
        """
        method_name = SendOnUpdate.name() + "_" + CapacityCalculator.name()

        if method_name not in self._battery_pool._active_methods:
            calculator = CapacityCalculator(self._battery_pool._batteries)
            self._battery_pool._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._battery_pool._working_batteries,
                min_update_interval=self._battery_pool._min_update_interval,
            )

        return self._battery_pool._active_methods[method_name]

    @property
    def power_bounds(self) -> ReceiverFetcher[_power_managing.Report]:
        """Get a receiver to receive new power bounds when they change.

        These bounds are the bounds specified the power manager for actors with the
        given priority.

        Returns:
            A receiver that will receive the power bounds for the given priority.
        """
        sub = _power_managing.ReportRequest(
            source_id=self._source_id,
            priority=self._priority,
            battery_ids=self._battery_pool._batteries,
        )
        self._battery_pool._power_bounds_subs[
            sub.get_channel_name()
        ] = asyncio.create_task(
            self._battery_pool._power_manager_bounds_subscription_sender.send(sub)
        )
        return self._battery_pool._channel_registry.new_receiver_fetcher(
            sub.get_channel_name()
        )

    @property
    def _system_power_bounds(self) -> ReceiverFetcher[PowerMetrics]:
        """Get receiver to receive new power bounds when they change.

        Power bounds refer to the min and max power that a battery can
        discharge or charge at and is also denoted as SoP.

        Power bounds formulas are described in the receiver return type.
        None will be send if there is no component to calculate metrics.

        A receiver from the MetricAggregator can be obtained by calling the
        `new_receiver` method.

        Returns:
            A MetricAggregator that will calculate and stream the power bounds
                of all batteries in the pool.
        """
        method_name = SendOnUpdate.name() + "_" + PowerBoundsCalculator.name()

        if method_name not in self._battery_pool._active_methods:
            calculator = PowerBoundsCalculator(self._battery_pool._batteries)
            self._battery_pool._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._battery_pool._working_batteries,
                min_update_interval=self._battery_pool._min_update_interval,
            )

        return self._battery_pool._active_methods[method_name]
