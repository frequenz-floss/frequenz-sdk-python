# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A logical meter for calculating high level metrics for a microgrid."""


import uuid

from frequenz.channels import Sender
from frequenz.quantities import Power, Quantity

from ..._internal._channels import ChannelRegistry
from ...microgrid._data_sourcing import ComponentMetricId, ComponentMetricRequest
from ..formula_engine import FormulaEngine
from ..formula_engine._formula_engine_pool import FormulaEnginePool
from ..formula_engine._formula_generators import CHPPowerFormula


class LogicalMeter:
    """A logical meter for calculating high level metrics in a microgrid.

    LogicalMeter provides methods for fetching power values from different points in the
    microgrid.  These methods return `FormulaReceiver` objects, which can be used like
    normal `Receiver`s, but can also be composed to form higher-order formula streams.

    !!! note
        `LogicalMeter` instances are not meant to be created directly by users.  Use the
        [`microgrid.logical_meter`][frequenz.sdk.microgrid.logical_meter] method for
        creating `LogicalMeter` instances.

    Example:
        ```python
        from datetime import timedelta

        from frequenz.sdk import microgrid
        from frequenz.sdk.actor import ResamplerConfig
        from frequenz.client.microgrid import ComponentMetricId


        await microgrid.initialize(
            "grpc://microgrid.sandbox.api.frequenz.io:62060",
            ResamplerConfig(resampling_period=timedelta(seconds=1)),
        )

        logical_meter = (
            microgrid.logical_meter()
            .start_formula("#1001 + #1002", ComponentMetricId.ACTIVE_POWER)
            .new_receiver()
        )

        async for power in logical_meter:
            print(power.value)
        ```
    """

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
    ) -> None:
        """Create a `LogicalMeter` instance.

        !!! note
            `LogicalMeter` instances are not meant to be created directly by users.  Use
            the [`microgrid.logical_meter`][frequenz.sdk.microgrid.logical_meter] method
            for creating `LogicalMeter` instances.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
        """
        self._channel_registry: ChannelRegistry = channel_registry
        self._resampler_subscription_sender: Sender[ComponentMetricRequest] = (
            resampler_subscription_sender
        )

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
        *,
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
            formula, component_metric_id, nones_are_zeros=nones_are_zeros
        )

    @property
    def chp_power(self) -> FormulaEngine[Power]:
        """Fetch the CHP power production in the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula engine to calculate CHP power production is not already running, it
        will be started.

        A receiver from the formula engine can be created using the `new_receiver`
        method.

        Returns:
            A FormulaEngine that will calculate and stream CHP power production.
        """
        engine = self._formula_pool.from_power_formula_generator(
            "chp_power",
            CHPPowerFormula,
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    async def stop(self) -> None:
        """Stop all formula engines."""
        await self._formula_pool.stop()
