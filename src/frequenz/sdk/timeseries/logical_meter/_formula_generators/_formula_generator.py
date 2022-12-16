# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Base class for formula generators that use the component graphs."""

from abc import ABC, abstractmethod

from frequenz.channels import Sender

from ....actor import ChannelRegistry, ComponentMetricRequest
from ....microgrid.component import ComponentMetricId
from .._formula_engine import FormulaEngine
from .._resampled_formula_builder import ResampledFormulaBuilder


class FormulaGenerationError(Exception):
    """An error encountered during formula generation from the component graph."""


class ComponentNotFound(FormulaGenerationError):
    """Indicates that a component required for generating a formula is not found."""


class FormulaGenerator(ABC):
    """A class for generating formulas from the component graph."""

    def __init__(
        self,
        namespace: str,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
    ) -> None:
        """Create a `FormulaGenerator` instance.

        Args:
            namespace: A namespace to use with the data-pipeline.
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
        """
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender
        self._namespace = namespace

    def _get_builder(
        self, component_metric_id: ComponentMetricId
    ) -> ResampledFormulaBuilder:
        builder = ResampledFormulaBuilder(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
            component_metric_id,
        )
        return builder

    @abstractmethod
    async def generate(self) -> FormulaEngine:
        """Generate a formula engine, based on the component graph."""
