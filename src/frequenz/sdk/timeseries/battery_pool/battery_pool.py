# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""User interface for requesting aggregated battery-inverter data."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Set
from datetime import timedelta
from typing import Any

from frequenz.channels import Receiver, Sender

from ..._internal._constants import RECEIVER_MAX_SIZE
from ..._internal.asyncio import cancel_and_await
from ...actor import ChannelRegistry, ComponentMetricRequest
from ...actor.power_distributing._battery_pool_status import BatteryStatus
from ...microgrid import connection_manager
from ...microgrid.component import ComponentCategory
from .._formula_engine import FormulaEngine, FormulaEnginePool
from .._formula_engine._formula_generators import (
    BatteryPowerFormula,
    FormulaGeneratorConfig,
)
from ._methods import AggregateMethod, SendOnUpdate
from ._metric_calculator import CapacityCalculator, PowerBoundsCalculator, SoCCalculator
from ._result_types import CapacityMetrics, PowerMetrics, SoCMetrics


class BatteryPool:
    """Calculate high level metrics for a pool of the batteries.

    BatterPool accepts subset of the battery ids and provides methods methods for
    fetching high level metrics for this subset.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        batteries_status_receiver: Receiver[BatteryStatus],
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
            self._batteries: Set[int] = batteries_id
        else:
            self._batteries = self._get_all_batteries()

        self._working_batteries: set[int] = set()

        if self._batteries:
            self._update_battery_status_task = asyncio.create_task(
                self._update_battery_status(batteries_status_receiver)
            )

        self._min_update_interval = min_update_interval
        self._active_methods: dict[str, AggregateMethod[Any]] = {}

        self._namespace: str = f"battery-pool-{self._batteries}-{uuid.uuid4()}"
        self._formula_pool: FormulaEnginePool = FormulaEnginePool(
            self._namespace,
            channel_registry,
            resampler_subscription_sender,
        )

    @property
    def battery_ids(self) -> Set[int]:
        """Return ids of the batteries in the pool.

        Returns:
            Ids of the batteries in the pool
        """
        return self._batteries

    @property
    def power(self) -> FormulaEngine:
        """Fetch the total power of the batteries in the pool.

        If a formula engine to calculate this metric is not already running, it will be
        started.

        A receiver from the formula engine can be obtained by calling the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream the total power of all
                batteries in the pool.
        """
        return self._formula_pool.from_generator(
            "battery_pool_power",
            BatteryPowerFormula,
            FormulaGeneratorConfig(component_ids=self._batteries),
        )  # type: ignore[return-value]

    async def soc(
        self, maxsize: int | None = RECEIVER_MAX_SIZE
    ) -> Receiver[SoCMetrics | None]:
        """Get receiver to receive new soc metrics when they change.

        Soc formulas are described in the receiver return type.
        None will be send if there is no component to calculate metric.

        Args:
            maxsize: Maxsize of the receiver channel.

        Returns:
            Receiver for this metric.
        """
        method_name = SendOnUpdate.name() + "_" + SoCCalculator.name()

        if method_name not in self._active_methods:
            calculator = SoCCalculator(self._batteries)
            self._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._working_batteries,
                min_update_interval=self._min_update_interval,
            )

        running_method = self._active_methods[method_name]
        return running_method.new_receiver(maxsize)

    async def capacity(
        self, maxsize: int | None = RECEIVER_MAX_SIZE
    ) -> Receiver[CapacityMetrics | None]:
        """Get receiver to receive new capacity metrics when they change.

        Capacity formulas are described in the receiver return type.
        None will be send if there is no component to calculate metrics.

        Args:
            maxsize: Maxsize of the receiver channel.

        Returns:
            Receiver for this metric.
        """
        method_name = SendOnUpdate.name() + "_" + CapacityCalculator.name()

        if method_name not in self._active_methods:
            calculator = CapacityCalculator(self._batteries)
            self._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._working_batteries,
                min_update_interval=self._min_update_interval,
            )

        running_method = self._active_methods[method_name]
        return running_method.new_receiver(maxsize)

    async def power_bounds(
        self, maxsize: int | None = RECEIVER_MAX_SIZE
    ) -> Receiver[PowerMetrics | None]:
        """Get receiver to receive new power bounds when they change.

        Power bounds formulas are described in the receiver return type.
        None will be send if there is no component to calculate metrics.

        Args:
            maxsize: Maxsize of the receivers channel.

        Returns:
            Receiver for this metric.
        """
        method_name = SendOnUpdate.name() + "_" + PowerBoundsCalculator.name()

        if method_name not in self._active_methods:
            calculator = PowerBoundsCalculator(self._batteries)
            self._active_methods[method_name] = SendOnUpdate(
                metric_calculator=calculator,
                working_batteries=self._working_batteries,
                min_update_interval=self._min_update_interval,
            )

        running_method = self._active_methods[method_name]
        return running_method.new_receiver(maxsize)

    async def stop(self) -> None:
        """Stop all pending async tasks."""
        await asyncio.gather(
            *[method.stop() for method in self._active_methods.values()],
            cancel_and_await(self._update_battery_status_task),
        )

    def _get_all_batteries(self) -> Set[int]:
        """Get all batteries from the microgrid.

        Returns:
            All batteries in the microgrid.
        """
        graph = connection_manager.get().component_graph
        return {
            battery.component_id
            for battery in graph.components(
                component_category={ComponentCategory.BATTERY}
            )
        }

    async def _update_battery_status(self, receiver: Receiver[BatteryStatus]) -> None:
        async for status in receiver:
            self._working_batteries = status.get_working_batteries(
                self._batteries  # type: ignore[arg-type]
            )
            for item in self._active_methods.values():
                item.update_working_batteries(self._working_batteries)
