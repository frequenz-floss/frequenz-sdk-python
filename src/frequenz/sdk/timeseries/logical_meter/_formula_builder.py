# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A builder for creating formula engines that operate on resampled component metrics."""


from frequenz.channels import Receiver, Sender

from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid.component import ComponentMetricId
from .._sample import Sample
from ._formula_engine import FormulaEngine
from ._tokenizer import Tokenizer, TokenType


class FormulaBuilder:
    """Provides a way to build a FormulaEngine from resampled data streams."""

    def __init__(
        self,
        namespace: str,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        metric_id: ComponentMetricId,
    ) -> None:
        """Create a `FormulaBuilder` instance.

        Args:
            namespace: The unique namespace to allow reuse of streams in the data
                pipeline.
            channel_registry: The channel registry instance shared with the resampling
                and the data sourcing actors.
            resampler_subscription_sender: A sender to send metric requests to the
                resampling actor.
            metric_id: A metric ID to fetch for all components in this formula.
        """
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender
        self._namespace = namespace
        self._formula = FormulaEngine()
        self._metric_id = metric_id

    async def _get_resampled_receiver(self, component_id: int) -> Receiver[Sample]:
        """Get a receiver with the resampled data for the given component id.

        This receiver would contain data for the `metric_id` specified when creating the
        `FormulaBuilder` instance.

        Args:
            component_id: The component id for which to get a resampled data receiver.

        Returns:
            A receiver to stream resampled data for the given component id.
        """
        request = ComponentMetricRequest(
            self._namespace, component_id, self._metric_id, None
        )
        await self._resampler_subscription_sender.send(request)
        return self._channel_registry.new_receiver(request.get_channel_name())

    async def push_component_metric(self, component_id: int) -> None:
        """Push a resampled component metric stream to the formula engine.

        Args:
            component_id: The component id for which to push a metric fetcher.
        """
        receiver = await self._get_resampled_receiver(component_id)
        self._formula.push_metric(f"#{component_id}", receiver)

    async def from_string(self, formula: str) -> FormulaEngine:
        """Construct a `FormulaEngine` from the given formula string.

        Formulas can have Component IDs that are preceeded by a pound symbol("#"), and
        these operators: +, -, *, /, (, ).

        For example, the input string: "#20 + #5" is a formula for adding metrics from
        two components with ids 20 and 5.

        Args:
            formula: A string formula.

        Returns:
            A FormulaEngine instance corresponding to the given formula.

        Raises:
            ValueError: when there is an unknown token type.
        """
        tokenizer = Tokenizer(formula)

        for token in tokenizer:
            if token.type == TokenType.COMPONENT_METRIC:
                await self.push_component_metric(int(token.value))
            elif token.type == TokenType.OPER:
                self._formula.push_oper(token.value)
            else:
                raise ValueError(f"Unknown token type: {token}")

        self._formula.finalize()
        return self._formula
