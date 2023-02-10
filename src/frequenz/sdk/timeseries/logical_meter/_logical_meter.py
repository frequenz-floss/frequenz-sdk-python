# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A logical meter for calculating high level metrics for a microgrid."""

from __future__ import annotations

import logging
import uuid

from frequenz.channels import Sender

from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid import ComponentGraph
from ...microgrid.component import ComponentMetricId
from .._formula_engine import FormulaEnginePool, FormulaReceiver, FormulaReceiver3Phase
from .._formula_engine._formula_generators import (
    BatteryPowerFormula,
    BatterySoCFormula,
    GridCurrentFormula,
    GridPowerFormula,
    PVPowerFormula,
)

logger = logging.getLogger(__name__)


class LogicalMeter:
    """A logical meter for calculating high level metrics in a microgrid.

    LogicalMeter provides methods for fetching power values from different points in the
    microgrid.  These methods return `FormulaReceiver` objects, which can be used like
    normal `Receiver`s, but can also be composed to form higher-order formula streams.

    Example:
        ``` python
        channel_registry = ChannelRegistry(name="data-registry")

        # Create a channels for sending/receiving subscription requests
        data_source_request_channel = Broadcast[ComponentMetricRequest]("data-source")
        data_source_request_sender = data_source_request_channel.new_sender()
        data_source_request_receiver = data_source_request_channel.new_receiver()

        resampling_request_channel = Broadcast[ComponentMetricRequest]("resample")
        resampling_request_sender = resampling_request_channel.new_sender()
        resampling_request_receiver = resampling_request_channel.new_receiver()

        # Instantiate a data sourcing actor
        _data_sourcing_actor = DataSourcingActor(
            request_receiver=data_source_request_receiver, registry=channel_registry
        )

        # Instantiate a resampling actor
        _resampling_actor = ComponentMetricsResamplingActor(
            channel_registry=channel_registry,
            data_sourcing_request_sender=data_source_request_sender,
            resampling_request_receiver=resampling_request_receiver,
            config=ResamplerConfig(resampling_period_s=1),
        )

        # Create a logical meter instance
        logical_meter = LogicalMeter(
            channel_registry,
            resampling_request_sender,
            microgrid.get().component_graph,
        )

        # Get a receiver for a builtin formula
        grid_power_recv = logical_meter.grid_power()
        for grid_power_sample in grid_power_recv:
            print(grid_power_sample)

        # or compose formula receivers to create a new formula
        net_power_recv = (
            (
                logical_meter.grid_power()
                - logical_meter.battery_power()
                - logical_meter.pv_power()
            )
            .build("net_power")
            .new_receiver()
        )
        for net_power_sample in net_power_recv:
            print(net_power_sample)
        ```
    """

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        component_graph: ComponentGraph,
    ) -> None:
        """Create a `LogicalMeter instance`.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
            component_graph: The component graph representing the microgrid.
        """
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender

        # Use a randomly generated uuid to create a unique namespace name for the local
        # meter to use when communicating with the resampling actor.
        self._namespace = f"logical-meter-{uuid.uuid4()}"
        self._component_graph = component_graph
        self._formula_pool = FormulaEnginePool(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
        )

    async def start_formula(
        self,
        formula: str,
        component_metric_id: ComponentMetricId,
        nones_are_zeros: bool = False,
    ) -> FormulaReceiver:
        """Start execution of the given formula.

        Formulas can have Component IDs that are preceeded by a pound symbol("#"), and
        these operators: +, -, *, /, (, ).

        For example, the input string: "#20 + #5" is a formula for adding metrics from
        two components with ids 20 and 5.

        Args:
            formula: formula to execute.
            component_metric_id: The metric ID to use when fetching receivers from the
                resampling actor.
            nones_are_zeros: Whether to treat None values from the stream as 0s.  If
                False, the returned value will be a None.

        Returns:
            A FormulaReceiver that streams values with the formulas applied.
        """
        return await self._formula_pool.from_string(
            formula, component_metric_id, nones_are_zeros
        )

    async def grid_power(self) -> FormulaReceiver:
        """Fetch the grid power for the microgrid.

        If a formula engine to calculate grid power is not already running, it
        will be started.  Else, we'll just get a new receiver to the already
        existing data stream.

        Returns:
            A *new* receiver that will stream grid_power values.

        """
        return await self._formula_pool.from_generator("grid_power", GridPowerFormula)

    async def grid_current(self) -> FormulaReceiver3Phase:
        """Fetch the grid power for the microgrid.

        If a formula engine to calculate grid current is not already running, it
        will be started.  Else, we'll just get a new receiver to the already
        existing data stream.

        Returns:
            A *new* receiver that will stream grid_current values.

        """
        return await self._formula_pool.from_generator(
            "grid_current", GridCurrentFormula
        )

    async def battery_power(self) -> FormulaReceiver:
        """Fetch the cumulative battery power in the microgrid.

        If a formula engine to calculate cumulative battery power is not
        already running, it will be started.  Else, we'll just get a new
        receiver to the already existing data stream.

        Returns:
            A *new* receiver that will stream battery_power values.

        """
        return await self._formula_pool.from_generator(
            "battery_power", BatteryPowerFormula
        )

    async def pv_power(self) -> FormulaReceiver:
        """Fetch the PV power production in the microgrid.

        If a formula engine to calculate PV power production is not
        already running, it will be started.  Else, we'll just get a new
        receiver to the already existing data stream.

        Returns:
            A *new* receiver that will stream PV power production values.
        """
        return await self._formula_pool.from_generator("pv_power", PVPowerFormula)

    async def _soc(self) -> FormulaReceiver:
        """Fetch the SoC of the active batteries in the microgrid.

        NOTE: This method is part of the logical meter only temporarily, and will get
        moved to the `BatteryPool` within the next few releases.

        Returns:
            A *new* receiver that will stream average SoC of active batteries.
        """
        return await self._formula_pool.from_generator("soc", BatterySoCFormula)
