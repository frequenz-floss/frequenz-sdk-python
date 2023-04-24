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
from .._formula_engine import FormulaEngine, FormulaEngine3Phase, FormulaEnginePool
from .._formula_engine._formula_generators import (
    GridCurrentFormula,
    GridPowerFormula,
    PVPowerFormula,
)

_logger = logging.getLogger(__name__)


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

    def start_formula(
        self,
        formula: str,
        component_metric_id: ComponentMetricId,
        nones_are_zeros: bool = False,
    ) -> FormulaEngine:
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
            A FormulaEngine that applies the formula and streams values.
        """
        return self._formula_pool.from_string(
            formula, component_metric_id, nones_are_zeros
        )

    @property
    def grid_power(self) -> FormulaEngine:
        """Fetch the grid power for the microgrid.

        If a formula engine to calculate grid power is not already running, it will be
        started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream grid power.
        """
        return self._formula_pool.from_generator(
            "grid_power",
            GridPowerFormula,
        )  # type: ignore[return-value]

    @property
    def grid_current(self) -> FormulaEngine3Phase:
        """Fetch the grid power for the microgrid.

        If a formula engine to calculate grid current is not already running, it will be
        started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream grid current.
        """
        return self._formula_pool.from_generator(
            "grid_current",
            GridCurrentFormula,
        )  # type: ignore[return-value]

    @property
    def pv_power(self) -> FormulaEngine:
        """Fetch the PV power production in the microgrid.

        If a formula engine to calculate PV power production is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream PV power production.
        """
        return self._formula_pool.from_generator(
            "pv_power",
            PVPowerFormula,
        )  # type: ignore[return-value]
