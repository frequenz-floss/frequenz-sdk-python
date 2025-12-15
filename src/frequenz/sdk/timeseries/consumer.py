# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""The logical component for calculating high level consumer metrics for a microgrid."""

import uuid

from frequenz.channels import Sender
from frequenz.quantities import Power

from .._internal._channels import ChannelRegistry
from ..microgrid import connection_manager
from ..microgrid._data_sourcing import ComponentMetricRequest
from .formulas._formula import Formula
from .formulas._formula_pool import FormulaPool


class Consumer:
    """Calculate high level consumer metrics in a microgrid.

    Under normal circumstances this is expected to correspond to the gross
    consumption of the site excluding active parts and battery.

    Consumer provides methods for fetching power values from different points
    in the microgrid. These methods return `FormulaReceiver` objects, which can
    be used like normal `Receiver`s, but can also be composed to form
    higher-order formula streams.

    !!! note
        `Consumer` instances are not meant to be created directly by users.
        Use the [`microgrid.consumer`][frequenz.sdk.microgrid.consumer] method
        for creating `Consumer` instances.

    Example:
        ```python
        from datetime import timedelta

        from frequenz.sdk import microgrid
        from frequenz.sdk.timeseries import ResamplerConfig2

        await microgrid.initialize(
            "grpc://127.0.0.1:50051",
            ResamplerConfig2(resampling_period=timedelta(seconds=1.0))
        )

        consumer = microgrid.consumer()

        # Get a receiver for a builtin formula
        consumer_power_recv = consumer.power.new_receiver()
        async for consumer_power_sample in consumer_power_recv:
            print(consumer_power_sample)
        ```
    """

    _formula_pool: FormulaPool
    """The formula pool to generate consumer metrics."""

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
    ) -> None:
        """Initialize the consumer formula generator.

        Args:
            channel_registry: The channel registry to use for the consumer.
            resampler_subscription_sender: The sender to use for resampler subscriptions.
        """
        namespace = f"consumer-{uuid.uuid4()}"
        self._formula_pool = FormulaPool(
            namespace,
            channel_registry,
            resampler_subscription_sender,
        )

    @property
    def power(self) -> Formula[Power]:
        """Fetch the consumer power for the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        It will start the formula to calculate consumer power if it is
        not already running.

        A receiver from the formula can be created using the
        `new_receiver` method.

        Returns:
            A Formula that will calculate and stream consumer power.
        """
        return self._formula_pool.from_power_formula(
            "consumer_power",
            connection_manager.get().component_graph.consumer_formula(),
        )

    async def stop(self) -> None:
        """Stop all formulas."""
        await self._formula_pool.stop()
