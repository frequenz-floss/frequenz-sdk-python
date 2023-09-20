# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""User interface for requesting aggregated battery-inverter data."""


import asyncio
import uuid
from collections.abc import Awaitable, Set
from datetime import timedelta
from typing import Any

from frequenz.channels import Receiver, Sender

from ..._internal._asyncio import cancel_and_await
from ..._internal._channels import ReceiverFetcher
from ...actor import ComponentMetricRequest, _channel_registry, _power_managing
from ...actor.power_distributing._battery_pool_status import BatteryStatus
from ...microgrid import connection_manager
from ...microgrid.component import ComponentCategory
from ...timeseries import Sample
from .._formula_engine import FormulaEngine, FormulaEnginePool
from .._formula_engine._formula_generators import (
    BatteryPowerFormula,
    FormulaGeneratorConfig,
    FormulaType,
)
from .._quantities import Energy, Percentage, Power, Temperature
from ._methods import MetricAggregator, SendOnUpdate
from ._metric_calculator import (
    CapacityCalculator,
    PowerBoundsCalculator,
    SoCCalculator,
    TemperatureCalculator,
)
from ._result_types import PowerMetrics


class BatteryPool:  # pylint: disable=too-many-instance-attributes
    """Calculate high level metrics for a pool of the batteries.

    BatterPool accepts subset of the battery ids and provides methods methods for
    fetching high level metrics for this subset.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        channel_registry: _channel_registry.ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        batteries_status_receiver: Receiver[BatteryStatus],
        power_manager_requests_sender: Sender[_power_managing.Proposal],
        power_manager_bounds_subscription_sender: Sender[_power_managing.ReportRequest],
        min_update_interval: timedelta,
        batteries_id: Set[int] | None = None,
    ) -> None:
        """Create the class instance.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
            batteries_status_receiver: Receiver to receive status of the batteries.
                Receivers should has maxsize = 1 to fetch only the latest status.
                Battery status channel should has resend_latest = True.
                It should send information when any battery changed status.
                Battery status should include status of the inverter adjacent to this
                battery.
            power_manager_requests_sender: A Channel sender for sending power
                requests to the power managing actor.
            power_manager_bounds_subscription_sender: A Channel sender for sending
                power bounds requests to the power managing actor.
            min_update_interval: Some metrics in BatteryPool are send only when they
                change. For these metrics min_update_interval is the minimum time
                interval between the following messages.
                Note that this argument is similar to the resampling period
                argument in the ComponentMetricsResamplingActor. But as opposed to
                ResamplingActor, timestamp returned in the resulting message will be
                the timestamp of the last received component data.
                It is currently impossible to use resampling actor for these metrics,
                because we can't specify resampling function for them.
            batteries_id: Subset of the batteries that should be included in the
                battery pool. If None or empty, then all batteries from the microgrid
                will be used.
        """
        if batteries_id:
            self._batteries: frozenset[int] = frozenset(batteries_id)
        else:
            self._batteries = self._get_all_batteries()

        self._working_batteries: set[int] = set()

        self._update_battery_status_task: asyncio.Task[None] | None = None
        if self._batteries:
            self._update_battery_status_task = asyncio.create_task(
                self._update_battery_status(batteries_status_receiver)
            )

        self._min_update_interval = min_update_interval

        self._power_manager_requests_sender = power_manager_requests_sender
        self._power_manager_bounds_subscription_sender = (
            power_manager_bounds_subscription_sender
        )

        self._active_methods: dict[str, MetricAggregator[Any]] = {}
        self._power_bounds_subs: dict[str, asyncio.Task[None]] = {}
        self._namespace: str = f"battery-pool-{self._batteries}-{uuid.uuid4()}"
        self._power_distributing_namespace: str = f"power-distributor-{self._namespace}"
        self._channel_registry = channel_registry
        self._formula_pool: FormulaEnginePool = FormulaEnginePool(
            self._namespace,
            self._channel_registry,
            resampler_subscription_sender,
        )

    async def set_power(
        self,
        preferred_power: Power,
        *,
        request_timeout: timedelta = timedelta(seconds=5.0),
        include_broken_batteries: bool = False,
        _bounds: tuple[Power, Power] | None = None,
        _priority: int = 0,
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
            _priority: The priority of the actor making the request.
        """
        await self._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._namespace,
                preferred_power=preferred_power,
                bounds=_bounds,
                battery_ids=self._batteries,
                priority=_priority,
                request_timeout=request_timeout,
                include_broken_batteries=include_broken_batteries,
            )
        )

    async def charge(
        self,
        power: Power,
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
        if power < Power.zero():
            raise ValueError("Charge power must be positive.")
        await self._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._namespace,
                preferred_power=power,
                bounds=None,
                battery_ids=self._batteries,
                priority=0,
                request_timeout=request_timeout,
                include_broken_batteries=include_broken_batteries,
            )
        )

    async def discharge(
        self,
        power: Power,
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
        if power < Power.zero():
            raise ValueError("Discharge power must be positive.")
        await self._power_manager_requests_sender.send(
            _power_managing.Proposal(
                source_id=self._namespace,
                preferred_power=power,
                bounds=None,
                battery_ids=self._batteries,
                priority=0,
                request_timeout=request_timeout,
                include_broken_batteries=include_broken_batteries,
            )
        )

    @property
    def battery_ids(self) -> Set[int]:
        """Return ids of the batteries in the pool.

        Returns:
            Ids of the batteries in the pool
        """
        return self._batteries

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
        engine = self._formula_pool.from_power_formula_generator(
            "battery_pool_power",
            BatteryPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._batteries,
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
        engine = self._formula_pool.from_power_formula_generator(
            "battery_pool_production_power",
            BatteryPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._batteries,
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
        engine = self._formula_pool.from_power_formula_generator(
            "battery_pool_consumption_power",
            BatteryPowerFormula,
            FormulaGeneratorConfig(
                component_ids=self._batteries,
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

        if method_name not in self._active_methods:
            calculator = SoCCalculator(self._batteries)
            self._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._working_batteries,
                min_update_interval=self._min_update_interval,
            )

        return self._active_methods[method_name]

    @property
    def temperature(self) -> ReceiverFetcher[Sample[Temperature]]:
        """Fetch the average temperature of the batteries in the pool.

        Returns:
            A MetricAggregator that will calculate and stream the average temperature
                of all batteries in the pool.
        """
        method_name = SendOnUpdate.name() + "_" + TemperatureCalculator.name()
        if method_name not in self._active_methods:
            calculator = TemperatureCalculator(self._batteries)
            self._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._working_batteries,
                min_update_interval=self._min_update_interval,
            )
        return self._active_methods[method_name]

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

        if method_name not in self._active_methods:
            calculator = CapacityCalculator(self._batteries)
            self._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._working_batteries,
                min_update_interval=self._min_update_interval,
            )

        return self._active_methods[method_name]

    def power_bounds(
        self, priority: int = 0
    ) -> ReceiverFetcher[_power_managing.Report]:
        """Get a receiver to receive new power bounds when they change.

        These bounds are the bounds specified the power manager for actors with the
        given priority.

        Args:
            priority: The priority of the actor to get the power bounds for.

        Returns:
            A receiver that will receive the power bounds for the given priority.
        """
        sub = _power_managing.ReportRequest(
            source_id=self._namespace, priority=priority, battery_ids=self._batteries
        )
        self._power_bounds_subs[sub.get_channel_name()] = asyncio.create_task(
            self._power_manager_bounds_subscription_sender.send(sub)
        )
        return self._channel_registry.new_receiver_fetcher(sub.get_channel_name())

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

        if method_name not in self._active_methods:
            calculator = PowerBoundsCalculator(self._batteries)
            self._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._working_batteries,
                min_update_interval=self._min_update_interval,
            )

        return self._active_methods[method_name]

    async def stop(self) -> None:
        """Stop all pending async tasks."""
        tasks_to_stop: list[Awaitable[Any]] = [
            method.stop() for method in self._active_methods.values()
        ]
        tasks_to_stop.append(self._formula_pool.stop())
        if self._update_battery_status_task:
            tasks_to_stop.append(cancel_and_await(self._update_battery_status_task))
        await asyncio.gather(*tasks_to_stop)

    def _get_all_batteries(self) -> frozenset[int]:
        """Get all batteries from the microgrid.

        Returns:
            All batteries in the microgrid.
        """
        graph = connection_manager.get().component_graph
        return frozenset(
            {
                battery.component_id
                for battery in graph.components(
                    component_category={ComponentCategory.BATTERY}
                )
            }
        )

    async def _update_battery_status(self, receiver: Receiver[BatteryStatus]) -> None:
        async for status in receiver:
            self._working_batteries = status.get_working_batteries(self._batteries)
            for item in self._active_methods.values():
                item.update_working_batteries(self._working_batteries)
