# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A logical meter for calculating high level metrics for a microgrid."""

from __future__ import annotations

import uuid

from frequenz.channels import Sender

from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid.component import ComponentMetricId
from .._formula_engine import FormulaEngine, FormulaEngine3Phase, FormulaEnginePool
from .._formula_engine._formula_generators import (
    CHPPowerFormula,
    ConsumerPowerFormula,
    FormulaGeneratorConfig,
    FormulaType,
    GridCurrentFormula,
    GridPowerFormula,
    ProducerPowerFormula,
    PVPowerFormula,
)
from .._quantities import Current, Power, Quantity


class LogicalMeter:
    """A logical meter for calculating high level metrics in a microgrid.

    LogicalMeter provides methods for fetching active_power values from different points in the
    microgrid.  These methods return `FormulaReceiver` objects, which can be used like
    normal `Receiver`s, but can also be composed to form higher-order formula streams.

    Example:
        ```python
        from frequenz.channels import Sender, Broadcast
        from frequenz.sdk.actor import DataSourcingActor, ComponentMetricsResamplingActor
        from frequenz.sdk.timeseries import ResamplerConfig
        from frequenz.sdk.microgrid import initialize
        from datetime import timedelta

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
            config=ResamplerConfig(resampling_period=timedelta(seconds=1)),
        )

        await initialize(
            "127.0.0.1",
            50051,
            ResamplerConfig(resampling_period=timedelta(seconds=1))
        )

        # Create a logical meter instance
        logical_meter = LogicalMeter(
            channel_registry,
            resampling_request_sender,
        )

        # Get a receiver for a builtin formula
        grid_active_power_recv = logical_meter.grid_active_power.new_receiver()
        for grid_active_power_sample in grid_active_power_recv:
            print(grid_active_power_sample)

        # or compose formula receivers to create a new formula
        net_active_power_recv = (
            (
                logical_meter.grid_active_power
                - logical_meter.pv_active_power
            )
            .build("net_active_power")
            .new_receiver()
        )
        for net_active_power_sample in net_active_power_recv:
            print(net_active_power_sample)
        ```
    """

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
    ) -> None:
        """Create a `LogicalMeter instance`.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
        """
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender

        # Use a randomly generated uuid to create a unique namespace name for the local
        # meter to use when communicating with the resampling actor.
        self._namespace = f"logical-meter-{uuid.uuid4()}"
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
    ) -> FormulaEngine[Quantity]:
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
    def grid_active_power(self) -> FormulaEngine[Power]:
        """Fetch the grid active_power for the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate grid active_power is not already running, it will be
        started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream grid active_power.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "grid_active_power",
            GridPowerFormula,
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def grid_consumption_active_power(self) -> FormulaEngine[Power]:
        """Fetch the grid consumption active_power for the microgrid.

        This formula produces positive values when consuming active_power and 0 otherwise.

        If a formula engine to calculate grid consumption active_power is not already running,
        it will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream grid consumption active_power.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "grid_consumption_active_power",
            GridPowerFormula,
            FormulaGeneratorConfig(formula_type=FormulaType.CONSUMPTION),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def grid_production_active_power(self) -> FormulaEngine[Power]:
        """Fetch the grid production active_power for the microgrid.

        This formula produces positive values when producing active_power and 0 otherwise.

        If a formula engine to calculate grid production active_power is not already running,
        it will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream grid production active_power.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "grid_production_active_power",
            GridPowerFormula,
            FormulaGeneratorConfig(formula_type=FormulaType.PRODUCTION),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def grid_current(self) -> FormulaEngine3Phase[Current]:
        """Fetch the grid active_power for the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate grid current is not already running, it will be
        started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream grid current.
        """
        engine = self._formula_pool.from_3_phase_current_formula_generator(
            "grid_current",
            GridCurrentFormula,
        )
        assert isinstance(engine, FormulaEngine3Phase)
        return engine

    @property
    def consumer_active_power(self) -> FormulaEngine[Power]:
        """Fetch the consumer active_power for the microgrid.

        Under normal circumstances this is expected to correspond to the gross
        consumption of the site excluding active parts and battery.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate consumer active_power is not already running, it will
        be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream consumer active_power.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "consumer_active_power",
            ConsumerPowerFormula,
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def producer_active_power(self) -> FormulaEngine[Power]:
        """Fetch the producer active_power for the microgrid.

        Under normal circumstances this is expected to correspond to the production
        of the sites active parts excluding ev chargers and batteries.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate producer active_power is not already running, it will
        be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream producer active_power.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "producer_active_power",
            ProducerPowerFormula,
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def pv_active_power(self) -> FormulaEngine[Power]:
        """Fetch the PV active_power in the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate PV active_power is not already running, it will be
        started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream PV total active_power.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "pv_active_power",
            PVPowerFormula,
            FormulaGeneratorConfig(formula_type=FormulaType.PASSIVE_SIGN_CONVENTION),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def pv_production_active_power(self) -> FormulaEngine[Power]:
        """Fetch the PV active_power production in the microgrid.

        This formula produces positive values when producing active_power and 0 otherwise.

        If a formula engine to calculate PV active_power production is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream PV active_power production.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "pv_production_active_power",
            PVPowerFormula,
            FormulaGeneratorConfig(formula_type=FormulaType.PRODUCTION),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def pv_consumption_active_power(self) -> FormulaEngine[Power]:
        """Fetch the PV active_power consumption in the microgrid.

        This formula produces positive values when consuming active_power and 0 otherwise.

        If a formula engine to calculate PV active_power consumption is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream PV active_power consumption.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "pv_consumption_active_power",
            PVPowerFormula,
            FormulaGeneratorConfig(formula_type=FormulaType.CONSUMPTION),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def chp_active_power(self) -> FormulaEngine[Power]:
        """Fetch the CHP active_power production in the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate CHP active_power production is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream CHP active_power production.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "chp_active_power",
            CHPPowerFormula,
            FormulaGeneratorConfig(formula_type=FormulaType.PASSIVE_SIGN_CONVENTION),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def chp_production_active_power(self) -> FormulaEngine[Power]:
        """Fetch the CHP active_power production in the microgrid.

        This formula produces positive values when producing active_power and 0 otherwise.

        If a formula engine to calculate CHP active_power production is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream CHP active_power production.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "chp_production_active_power",
            CHPPowerFormula,
            FormulaGeneratorConfig(
                formula_type=FormulaType.PRODUCTION,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    @property
    def chp_consumption_active_power(self) -> FormulaEngine[Power]:
        """Fetch the CHP active_power consumption in the microgrid.

        This formula produces positive values when consuming active_power and 0 otherwise.

        If a formula engine to calculate CHP active_power consumption is not already running,
        it will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream CHP active_power consumption.
        """
        engine = self._formula_pool.from_active_power_formula_generator(
            "chp_consumption_active_power",
            CHPPowerFormula,
            FormulaGeneratorConfig(
                formula_type=FormulaType.CONSUMPTION,
            ),
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    async def stop(self) -> None:
        """Stop all formula engines."""
        await self._formula_pool.stop()
