# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A formula pool for helping with tracking running formula engines."""

from __future__ import annotations

from typing import TYPE_CHECKING, Type

from frequenz.channels import Sender

from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid.component import ComponentMetricId
from ._formula_generators._formula_generator import (
    FormulaGenerator,
    FormulaGeneratorConfig,
)
from ._resampled_formula_builder import ResampledFormulaBuilder

if TYPE_CHECKING:
    # Break circular import by enclosing these type hints in a `TYPE_CHECKING` block.
    from .._formula_engine import FormulaEngine, FormulaEngine3Phase


class FormulaEnginePool:
    """Creates and owns formula engines from string formulas, or formula generators.

    If an engine already exists with a given name, it is reused instead.
    """

    def __init__(
        self,
        namespace: str,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
    ) -> None:
        """Create a new instance.

        Args:
            namespace: namespace to use with the data pipeline.
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
        """
        self._namespace = namespace
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender
        self._engines: dict[str, "FormulaEngine|FormulaEngine3Phase"] = {}

    def from_string(
        self,
        formula: str,
        component_metric_id: ComponentMetricId,
        nones_are_zeros: bool = False,
    ) -> FormulaEngine:
        """Get a receiver for a manual formula.

        Args:
            formula: formula to execute.
            component_metric_id: The metric ID to use when fetching receivers from the
                resampling actor.
            nones_are_zeros: Whether to treat None values from the stream as 0s.  If
                False, the returned value will be a None.

        Returns:
            A FormulaReceiver that streams values with the formulas applied.
        """
        channel_key = formula + component_metric_id.value
        if channel_key in self._engines:
            return self._engines[channel_key]  # type: ignore

        builder = ResampledFormulaBuilder(
            self._namespace,
            formula,
            self._channel_registry,
            self._resampler_subscription_sender,
            component_metric_id,
        )
        formula_engine = builder.from_string(formula, nones_are_zeros)
        self._engines[channel_key] = formula_engine

        return formula_engine

    def from_generator(
        self,
        channel_key: str,
        generator: "Type[FormulaGenerator]",
        config: FormulaGeneratorConfig = FormulaGeneratorConfig(),
    ) -> FormulaEngine | FormulaEngine3Phase:
        """Get a receiver for a formula from a generator.

        Args:
            channel_key: A string to uniquely identify the formula.
            generator: A formula generator.
            config: config to initialize the formula generator with.

        Returns:
            A FormulaReceiver or a FormulaReceiver3Phase instance based on what the
                FormulaGenerator returns.
        """
        if channel_key in self._engines:
            return self._engines[channel_key]

        engine = generator(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
            config,
        ).generate()
        self._engines[channel_key] = engine
        return engine
