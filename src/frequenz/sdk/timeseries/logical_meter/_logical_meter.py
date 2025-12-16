# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A logical meter for calculating high level metrics for a microgrid."""


import uuid

from frequenz.channels import Sender
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Power, Quantity

from frequenz.sdk.microgrid import connection_manager

from ..._internal._channels import ChannelRegistry
from ...microgrid._data_sourcing import ComponentMetricRequest
from ..formulas._formula import Formula
from ..formulas._formula_pool import FormulaPool


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
        from frequenz.sdk.timeseries import ResamplerConfig2
        from frequenz.client.microgrid.metrics import Metric


        await microgrid.initialize(
            "grpc://microgrid.sandbox.api.frequenz.io:62060",
            ResamplerConfig2(resampling_period=timedelta(seconds=1)),
        )

        logical_meter = (
            microgrid.logical_meter()
            .start_formula("#1001 + #1002", Metric.AC_ACTIVE_POWER)
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
        self._namespace: str = f"logical-meter-{uuid.uuid4()}"
        self._formula_pool: FormulaPool = FormulaPool(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
        )

    def start_formula(
        self,
        formula: str,
        metric: Metric,
    ) -> Formula[Quantity]:
        """Start execution of the given formula.

        Formulas can have Component IDs that are preceded by a pound symbol(`#`),
        constant values and these operators: `+`, `-`, `*`, `/`, `(`, `)`.

        These functions are also supported: `COALESCE`, `MAX`, `MIN`.

        For example, the input string: `#20 + #5` is a formula for adding metrics from
        two components with ids 20 and 5.

        A more detailed description of the formula syntax with examples can be found
        [here](https://github.com/frequenz-floss/frequenz-microgrid-formula-engine-rs?tab=readme-ov-file#formula-syntax-overview).

        Args:
            formula: formula to execute.
            metric: The metric to use when fetching receivers from the resampling actor.

        Returns:
            A Formula that applies the formula and streams values.
        """
        return self._formula_pool.from_string(formula, metric)

    @property
    def chp_power(self) -> Formula[Power]:
        """Fetch the CHP power production in the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        If a formula to calculate CHP power production is not already running, it
        will be started.

        A receiver from the formula can be created using the `new_receiver`
        method.

        Returns:
            A Formula that will calculate and stream CHP power production.
        """
        return self._formula_pool.from_power_formula(
            channel_key="chp_power",
            formula_str=connection_manager.get().component_graph.chp_formula(None),
        )

    async def stop(self) -> None:
        """Stop all formulas."""
        await self._formula_pool.stop()
