# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A builder for creating formula engines that operate on resampled component metrics."""


from frequenz.channels import Receiver, Sender

from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid.component import ComponentMetricId
from .. import Sample
from ._formula_engine import FormulaBuilder, FormulaEngine
from ._tokenizer import Tokenizer, TokenType


class ResampledFormulaBuilder(FormulaBuilder):
    """Provides a way to build a FormulaEngine from resampled data streams."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        namespace: str,
        formula_name: str,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
        metric_id: ComponentMetricId,
    ) -> None:
        """Create a `ResampledFormulaBuilder` instance.

        Args:
            namespace: The unique namespace to allow reuse of streams in the data
                pipeline.
            formula_name: A name for the formula.
            channel_registry: The channel registry instance shared with the resampling
                and the data sourcing actors.
            resampler_subscription_sender: A sender to send metric requests to the
                resampling actor.
            metric_id: A metric ID to fetch for all components in this formula.
        """
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender
        self._namespace = namespace
        self._metric_id = metric_id
        super().__init__(formula_name)

    async def _get_resampled_receiver(
        self, component_id: int, metric_id: ComponentMetricId
    ) -> Receiver[Sample]:
        """Get a receiver with the resampled data for the given component id.

        Args:
            component_id: The component id for which to get a resampled data receiver.
            metric_id: A metric ID to fetch for all components in this formula.

        Returns:
            A receiver to stream resampled data for the given component id.
        """
        request = ComponentMetricRequest(self._namespace, component_id, metric_id, None)
        await self._resampler_subscription_sender.send(request)
        return self._channel_registry.new_receiver(request.get_channel_name())

    async def push_component_metric(
        self, component_id: int, *, nones_are_zeros: bool
    ) -> None:
        """Push a resampled component metric stream to the formula engine.

        Args:
            component_id: The component id for which to push a metric fetcher.
            nones_are_zeros: Whether to treat None values from the stream as 0s.  If
                False, the returned value will be a None.
        """
        receiver = await self._get_resampled_receiver(component_id, self._metric_id)
        self.push_metric(f"#{component_id}", receiver, nones_are_zeros)

    async def from_string(
        self,
        formula: str,
        nones_are_zeros: bool,
    ) -> FormulaEngine:
        """Construct a `FormulaEngine` from the given formula string.

        Formulas can have Component IDs that are preceeded by a pound symbol("#"), and
        these operators: +, -, *, /, (, ).

        For example, the input string: "#20 + #5" is a formula for adding metrics from
        two components with ids 20 and 5.

        Args:
            formula: A string formula.
            nones_are_zeros: Whether to treat None values from the stream as 0s.  If
                False, the returned value will be a None.

        Returns:
            A FormulaEngine instance corresponding to the given formula.

        Raises:
            ValueError: when there is an unknown token type.
        """
        tokenizer = Tokenizer(formula)

        for token in tokenizer:
            if token.type == TokenType.COMPONENT_METRIC:
                await self.push_component_metric(
                    int(token.value), nones_are_zeros=nones_are_zeros
                )
            elif token.type == TokenType.OPER:
                self.push_oper(token.value)
            else:
                raise ValueError(f"Unknown token type: {token}")

        return self.build()
