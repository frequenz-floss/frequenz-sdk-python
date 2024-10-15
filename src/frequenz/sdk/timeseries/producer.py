# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""The logical component for calculating high level producer metrics for a microgrid."""

import uuid

from frequenz.channels import Sender
from frequenz.quantities import Power

from .._internal._channels import ChannelRegistry
from ..microgrid._data_sourcing import ComponentMetricRequest
from .formula_engine import FormulaEngine
from .formula_engine._formula_engine_pool import FormulaEnginePool
from .formula_engine._formula_generators import ProducerPowerFormula


class Producer:
    """Calculate high level producer metrics in a microgrid.

    Under normal circumstances this is expected to correspond to the gross
    production of the sites active parts excluding EV chargers and batteries.

    Producer provides methods for fetching power values from different points
    in the microgrid. These methods return `FormulaReceiver` objects, which can
    be used like normal `Receiver`s, but can also be composed to form
    higher-order formula streams.

    !!! note
        `Producer` instances are not meant to be created directly by users.
        Use the [`microgrid.producer`][frequenz.sdk.microgrid.producer] method
        for creating `Producer` instances.

    Example:
        ```python
        from datetime import timedelta

        from frequenz.sdk import microgrid
        from frequenz.sdk.timeseries import ResamplerConfig

        await microgrid.initialize(
            "grpc://127.0.0.1:50051",
            ResamplerConfig(resampling_period=timedelta(seconds=1.0))
        )

        producer = microgrid.producer()

        # Get a receiver for a builtin formula
        producer_power_recv = producer.power.new_receiver()
        async for producer_power_sample in producer_power_recv:
            print(producer_power_sample)
        ```
    """

    _formula_pool: FormulaEnginePool
    """The formula engine pool to generate producer metrics."""

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
    ) -> None:
        """Initialize the producer formula generator.

        Args:
            channel_registry: The channel registry to use for the producer.
            resampler_subscription_sender: The sender to use for resampler subscriptions.
        """
        namespace = f"producer-{uuid.uuid4()}"
        self._formula_pool = FormulaEnginePool(
            namespace,
            channel_registry,
            resampler_subscription_sender,
        )

    @property
    def power(self) -> FormulaEngine[Power]:
        """Fetch the producer power for the microgrid.

        This formula produces values that are in the Passive Sign Convention (PSC).

        It will start the formula engine to calculate producer power if it is
        not already running.

        A receiver from the formula engine can be created using the
        `new_receiver` method.

        Returns:
            A FormulaEngine that will calculate and stream producer power.
        """
        engine = self._formula_pool.from_power_formula_generator(
            "producer_power",
            ProducerPowerFormula,
        )
        assert isinstance(engine, FormulaEngine)
        return engine

    async def stop(self) -> None:
        """Stop all formula engines."""
        await self._formula_pool.stop()
