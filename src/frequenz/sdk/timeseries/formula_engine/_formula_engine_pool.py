# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A formula pool for helping with tracking running formula engines."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from frequenz.channels import Sender

from ...microgrid.component import ComponentMetricId
from .._base_types import Sample
from .._quantities import Current, Power, Quantity, QuantityT
from ._formula_generators._formula_generator import (
    FormulaGenerator,
    FormulaGeneratorConfig,
)
from ._resampled_formula_builder import ResampledFormulaBuilder

if TYPE_CHECKING:
    # Break circular import
    from ...actor import ChannelRegistry, ComponentMetricRequest
    from ..formula_engine import FormulaEngine, FormulaEngine3Phase


class QuantitySampleChannelRegistry:
    """A channel registry for a specific quantity."""

    def __init__(
        self,
        *,
        name: str,
        power: ChannelRegistry[Sample[Power]] | None = None,
        current: ChannelRegistry[Sample[Current]] | None = None,
        quantity: ChannelRegistry[Sample[Quantity]] | None = None,
    ) -> None:
        """Initialize this registry.

        Args:
            name: The name of the registry.
            power: The power channel registry.
            current: The current channel registry.
            quantity: The quantity channel registry.
        """
        self._name = name
        self._power = power or ChannelRegistry[Sample[Power]](name=f"{name}-power")
        self._current = current or ChannelRegistry[Sample[Current]](
            name=f"{name}-current"
        )
        self._quantity = quantity or ChannelRegistry[Sample[Quantity]](
            name=f"{name}-quantity"
        )

    @property
    def name(self) -> str:
        """The name of the registry."""
        return self._name

    @property
    def power(self) -> ChannelRegistry[Sample[Power]]:
        """The power channel registry."""
        return self._power

    @property
    def current(self) -> ChannelRegistry[Sample[Current]]:
        """The current channel registry."""
        return self._current

    @property
    def quantity(self) -> ChannelRegistry[Sample[Quantity]]:
        """The quantity channel registry."""
        return self._quantity

    def __repr__(self) -> str:
        """Get a string representation of the instance."""
        return f"{type(self).__name__}(name={self._name!r})"


class FormulaEnginePool:
    """Creates and owns formula engines from string formulas, or formula generators.

    If an engine already exists with a given name, it is reused instead.
    """

    def __init__(
        self,
        namespace: str,
        channel_registry: QuantitySampleChannelRegistry,
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
        self._namespace: str = namespace
        self._channel_registry: QuantitySampleChannelRegistry = channel_registry
        self._resampler_subscription_sender: Sender[
            ComponentMetricRequest
        ] = resampler_subscription_sender
        self._string_engines: dict[str, FormulaEngine[Quantity]] = {}
        self._power_engines: dict[str, FormulaEngine[Power]] = {}
        self._current_engines: dict[str, FormulaEngine3Phase[Current]] = {}

    def from_string(
        self,
        formula: str,
        component_metric_id: ComponentMetricId,
        *,
        nones_are_zeros: bool = False,
    ) -> FormulaEngine[Quantity]:
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
        if channel_key in self._string_engines:
            return self._string_engines[channel_key]

        builder = ResampledFormulaBuilder(
            self._namespace,
            formula,
            self._channel_registry.quantity,
            self._resampler_subscription_sender,
            component_metric_id,
            Quantity,
        )
        formula_engine = builder.from_string(formula, nones_are_zeros=nones_are_zeros)
        self._string_engines[channel_key] = formula_engine

        return formula_engine

    def from_power_formula_generator(
        self,
        channel_key: str,
        generator: type[FormulaGenerator[Power]],
        config: FormulaGeneratorConfig = FormulaGeneratorConfig(),
    ) -> FormulaEngine[Power]:
        """Get a receiver for a formula from a generator.

        Args:
            channel_key: A string to uniquely identify the formula.
            generator: A formula generator.
            config: config to initialize the formula generator with.

        Returns:
            A FormulaReceiver or a FormulaReceiver3Phase instance based on what the
                FormulaGenerator returns.
        """
        from ._formula_engine import (  # pylint: disable=import-outside-toplevel
            FormulaEngine,
        )

        if channel_key in self._power_engines:
            return self._power_engines[channel_key]

        engine = generator(
            self._namespace,
            self._channel_registry.power,
            self._resampler_subscription_sender,
            config,
        ).generate()
        assert isinstance(engine, FormulaEngine)
        self._power_engines[channel_key] = engine
        return engine

    def from_3_phase_current_formula_generator(
        self,
        channel_key: str,
        generator: type[FormulaGenerator[Current]],
        config: FormulaGeneratorConfig = FormulaGeneratorConfig(),
    ) -> FormulaEngine3Phase[Current]:
        """Get a receiver for a formula from a generator.

        Args:
            channel_key: A string to uniquely identify the formula.
            generator: A formula generator.
            config: config to initialize the formula generator with.

        Returns:
            A FormulaReceiver or a FormulaReceiver3Phase instance based on what the
                FormulaGenerator returns.
        """
        from ._formula_engine import (  # pylint: disable=import-outside-toplevel
            FormulaEngine3Phase,
        )

        if channel_key in self._current_engines:
            return self._current_engines[channel_key]

        engine = generator(
            self._namespace,
            self._channel_registry.current,
            self._resampler_subscription_sender,
            config,
        ).generate()
        assert isinstance(engine, FormulaEngine3Phase)
        self._current_engines[channel_key] = engine
        return engine

    async def stop(self) -> None:
        """Stop all formula engines in the pool."""
        for string_engine in self._string_engines.values():
            await string_engine._stop()  # pylint: disable=protected-access
        for power_engine in self._power_engines.values():
            await power_engine._stop()  # pylint: disable=protected-access
        for current_engine in self._current_engines.values():
            await current_engine._stop()  # pylint: disable=protected-access
