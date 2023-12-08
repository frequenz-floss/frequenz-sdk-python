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
from typing import cast

from ... import timeseries
from ..._internal._channels import ReceiverFetcher
from ...actor import _power_managing
from ...timeseries import Energy, Percentage, Power, Sample, Temperature
from .._base_types import SystemBounds
from ..formula_engine import FormulaEngine
from ..formula_engine._formula_generators import (
    BatteryPowerFormula,
    FormulaGeneratorConfig,
)
from ._battery_pool_reference_store import BatteryPoolReferenceStore
from ._methods import SendOnUpdate
from ._metric_calculator import (
    CapacityCalculator,
    PowerBoundsCalculator,
    SoCCalculator,
    TemperatureCalculator,
)
from ._result_types import BatteryPoolReport

# pylint: disable=protected-access


class BatteryPool:
    """An interface for interaction with pools of batteries.

    !!! note
        `BatteryPool` instances are not meant to be created directly by users.  Use the
        [`microgrid.battery_pool`][frequenz.sdk.microgrid.battery_pool] method for
        creating `BatteryPool` instances.

    Provides:
      - properties for fetching reporting streams of instantaneous
        [power][frequenz.sdk.timeseries.battery_pool.BatteryPool.power],
        [soc][frequenz.sdk.timeseries.battery_pool.BatteryPool.soc],
        [capacity][frequenz.sdk.timeseries.battery_pool.BatteryPool.capacity] values and
        available power bounds and other status through
        [power_status][frequenz.sdk.timeseries.battery_pool.BatteryPool.power_status].
      - control methods for proposing power values, namely:
        [propose_power][frequenz.sdk.timeseries.battery_pool.BatteryPool.propose_power],
        [propose_charge][frequenz.sdk.timeseries.battery_pool.BatteryPool.propose_charge] and
        [propose_discharge][frequenz.sdk.timeseries.battery_pool.BatteryPool.propose_discharge].
    """

    def __init__(
        self,
        battery_pool_ref: BatteryPoolReferenceStore,
        name: str | None,
        priority: int,
    ):
        """Create a BatteryPool instance.

        !!! note
            `BatteryPool` instances are not meant to be created directly by users.  Use
            the [`microgrid.battery_pool`][frequenz.sdk.microgrid.battery_pool] method
            for creating `BatteryPool` instances.

        Args:
            battery_pool_ref: The battery pool reference store instance.
            name: An optional name used to identify this instance of the pool or a
                corresponding actor in the logs.
            priority: The priority of the actor using this wrapper.
        """
        self._battery_pool = battery_pool_ref
        unique_id = str(uuid.uuid4())
        self._source_id = unique_id if name is None else f"{name}-{unique_id}"
        self._priority = priority

    async def propose_power(
        self,
        power: Power | None,
        *,
        request_timeout: timedelta = timedelta(seconds=5.0),
        bounds: timeseries.Bounds[Power | None] = timeseries.Bounds(None, None),
    ) -> None:
        """Send a proposal to the power manager for the pool's set of batteries.

        Power values need to follow the Passive Sign Convention (PSC). That is, positive
        values indicate charge power and negative values indicate discharge power.

        If the same batteries are shared by multiple actors, the power manager will
        consider the priority of the actors, the bounds they set, and their preferred
        power, when calculating the target power for the batteries.

        The preferred power of lower priority actors will take precedence as long as
        they respect the bounds set by higher priority actors.  If lower priority actors
        request power values outside of the bounds set by higher priority actors, the
        target power will be the closest value to the preferred power that is within the
        bounds.

        When there are no other actors trying to use the same batteries, the actor's
        preferred power would be set as the target power, as long as it falls within the
        system power bounds for the batteries.

        The result of the request can be accessed using the receiver returned from the
        [`power_status`][frequenz.sdk.timeseries.battery_pool.BatteryPool.power_status]
        method, which also streams the bounds that an actor should comply with, based on
        its priority.

        Args:
            power: The power to propose for the batteries in the pool.  If `None`, this
                proposal will not have any effect on the target power, unless bounds are
                specified.  If both are `None`, it is equivalent to not having a
                proposal or withdrawing a previous one.
            request_timeout: The timeout for the request.
            bounds: The power bounds for the proposal.  These bounds will apply to
                actors with a lower priority, and can be overridden by bounds from
                actors with a higher priority.  If None, the power bounds will be set
                to the maximum power of the batteries in the pool.  This is currently
                and experimental feature.
        """
        await self._battery_pool._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._source_id,
                preferred_power=power,
                bounds=bounds,
                component_ids=self._battery_pool._batteries,
                priority=self._priority,
                request_timeout=request_timeout,
            )
        )

    async def propose_charge(
        self,
        power: Power | None,
        *,
        request_timeout: timedelta = timedelta(seconds=5.0),
    ) -> None:
        """Set the given charge power for the batteries in the pool.

        Power values need to be positive values, indicating charge power.

        When using the Passive Sign Convention, the
        [`propose_power`][frequenz.sdk.timeseries.battery_pool.BatteryPool.propose_power]
        method might be more convenient.

        If the same batteries are shared by multiple actors, the behaviour is the same
        as that of the `propose_power` method.  The bounds for lower priority actors
        can't be specified with this method.  If that's required, use the
        `propose_power` method instead.

        The result of the request can be accessed using the receiver returned from the
        [`power_status`][frequenz.sdk.timeseries.battery_pool.BatteryPool.power_status]
        method.

        Args:
            power: The unsigned charge power to propose for the batteries in the pool.
                If None, the proposed power of higher priority actors will take
                precedence as the target power.
            request_timeout: The timeout for the request.

        Raises:
            ValueError: If the given power is negative.
        """
        if power and power < Power.zero():
            raise ValueError("Charge power must be positive.")
        await self._battery_pool._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._source_id,
                preferred_power=power,
                bounds=timeseries.Bounds(None, None),
                component_ids=self._battery_pool._batteries,
                priority=self._priority,
                request_timeout=request_timeout,
            )
        )

    async def propose_discharge(
        self,
        power: Power | None,
        *,
        request_timeout: timedelta = timedelta(seconds=5.0),
    ) -> None:
        """Set the given discharge power for the batteries in the pool.

        Power values need to be positive values, indicating discharge power.

        When using the Passive Sign Convention, the
        [`propose_power`][frequenz.sdk.timeseries.battery_pool.BatteryPool.propose_power]
        method might be more convenient.

        If the same batteries are shared by multiple actors, the behaviour is the same
        as that of the `propose_power` method.  The bounds for lower priority actors
        can't be specified with this method.  If that's required, use the
        `propose_power` method instead.

        The result of the request can be accessed using the receiver returned from the
        [`power_status`][frequenz.sdk.timeseries.battery_pool.BatteryPool.power_status]
        method.

        Args:
            power: The unsigned discharge power to propose for the batteries in the
                pool.  If None, the proposed power of higher priority actors will take
                precedence as the target power.
            request_timeout: The timeout for the request.

        Raises:
            ValueError: If the given power is negative.
        """
        if power and power < Power.zero():
            raise ValueError("Discharge power must be positive.")
        await self._battery_pool._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._source_id,
                preferred_power=power,
                bounds=timeseries.Bounds(None, None),
                component_ids=self._battery_pool._batteries,
                priority=self._priority,
                request_timeout=request_timeout,
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
    def power_status(self) -> ReceiverFetcher[BatteryPoolReport]:
        """Get a receiver to receive new power status reports when they change.

        These include
          - the current inclusion/exclusion bounds available for the pool's priority,
          - the current target power for the pool's set of batteries,
          - the result of the last distribution request for the pool's set of batteries.

        Returns:
            A receiver that will stream power status reports for the pool's priority.
        """
        sub = _power_managing.ReportRequest(
            source_id=self._source_id,
            priority=self._priority,
            component_ids=self._battery_pool._batteries,
        )
        self._battery_pool._power_bounds_subs[
            sub.get_channel_name()
        ] = asyncio.create_task(
            self._battery_pool._power_manager_bounds_subscription_sender.send(sub)
        )
        self._battery_pool._channel_registry.set_resend_latest(
            sub.get_channel_name(), True
        )
        return cast(
            ReceiverFetcher[BatteryPoolReport],
            self._battery_pool._channel_registry.new_receiver_fetcher(
                sub.get_channel_name()
            ),
        )

    @property
    def _system_power_bounds(self) -> ReceiverFetcher[SystemBounds]:
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
