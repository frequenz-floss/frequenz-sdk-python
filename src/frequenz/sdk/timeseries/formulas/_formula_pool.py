# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""A formula pool for helping with tracking running formulas."""


import logging
import sys

from frequenz.channels import Sender
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Current, Power, Quantity, ReactivePower

from frequenz.sdk.timeseries.formulas._resampled_stream_fetcher import (
    ResampledStreamFetcher,
)

from ..._internal._channels import ChannelRegistry
from ...microgrid._data_sourcing import ComponentMetricRequest
from ._formula import Formula
from ._formula_3_phase import Formula3Phase
from ._parser import parse

_logger = logging.getLogger(__name__)


NON_EXISTING_COMPONENT_ID = sys.maxsize
"""The component ID for non-existent components in the components graph.

The non-existing component ID is commonly used in scenarios where a formula
requires a component ID but there are no available components in the graph to
associate with it. Thus, the non-existing component ID is subscribed instead so
that the formula can send `None` or `0` values at the same frequency as the
other streams.
"""


class FormulaPool:
    """Creates and owns formulas from string formulas.

    If a formula already exists with a given name, it is reused instead.
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
        self._namespace: str = namespace
        self._channel_registry: ChannelRegistry = channel_registry
        self._resampler_subscription_sender: Sender[ComponentMetricRequest] = (
            resampler_subscription_sender
        )

        self._string_formulas: dict[str, Formula[Quantity]] = {}
        self.power_formulas: dict[str, Formula[Power]] = {}
        self._reactive_power_formulas: dict[str, Formula[ReactivePower]] = {}
        self._current_formulas: dict[str, Formula3Phase[Current]] = {}

        self._power_3_phase_formulas: dict[str, Formula3Phase[Power]] = {}
        self._current_3_phase_formulas: dict[str, Formula3Phase[Current]] = {}

    def from_string(
        self,
        formula_str: str,
        metric: Metric,
    ) -> Formula[Quantity]:
        """Get a receiver for a manual formula.

        Args:
            formula_str: formula to execute.
            metric: The metric to use when fetching receivers from the resampling
                actor.

        Returns:
            A Formula that streams values with the formulas applied.
        """
        channel_key = formula_str + str(metric.value)
        if channel_key in self._string_formulas:
            return self._string_formulas[channel_key]
        formula = parse(
            name=channel_key,
            formula=formula_str,
            telemetry_fetcher=self._telemetry_fetcher(metric),
            create_method=Quantity,
        )
        self._string_formulas[channel_key] = formula
        return formula

    def from_power_formula(self, channel_key: str, formula_str: str) -> Formula[Power]:
        """Get a receiver from the formula represented by the given strings.

        Args:
            channel_key: A string to uniquely identify the formula.  This
                usually includes the formula itself and the metric.
            formula_str: The formula string.

        Returns:
            A formula that evaluates the given formula.
        """
        if channel_key in self.power_formulas:
            return self.power_formulas[channel_key]

        if formula_str == "0.0":
            formula_str = f"coalesce(#{NON_EXISTING_COMPONENT_ID}, 0.0)"

        formula = parse(
            name=channel_key,
            formula=formula_str,
            telemetry_fetcher=self._telemetry_fetcher(Metric.AC_POWER_ACTIVE),
            create_method=Power.from_watts,
        )
        self.power_formulas[channel_key] = formula

        return formula

    def from_reactive_power_formula(
        self, channel_key: str, formula_str: str
    ) -> Formula[ReactivePower]:
        """Get a receiver from the formula represented by the given strings.

        Args:
            channel_key: A string to uniquely identify the formula.  This
                usually includes the formula itself and the metric.
            formula_str: The formula string.

        Returns:
            A formula that evaluates the given formula.
        """
        if channel_key in self.power_formulas:
            return self._reactive_power_formulas[channel_key]

        if formula_str == "0.0":
            formula_str = f"coalesce(#{NON_EXISTING_COMPONENT_ID}, 0.0)"

        formula = parse(
            name=channel_key,
            formula=formula_str,
            telemetry_fetcher=self._telemetry_fetcher(Metric.AC_POWER_REACTIVE),
            create_method=ReactivePower.from_volt_amperes_reactive,
        )
        self._reactive_power_formulas[channel_key] = formula

        return formula

    def from_power_3_phase_formula(
        self, channel_key: str, formula_str: str
    ) -> Formula3Phase[Power]:
        """Get a receiver from the 3-phase power formula represented by the given strings.

        Args:
            channel_key: A string to uniquely identify the formula.  This
                usually includes the formula itself.
            formula_str: The formula string.

        Returns:
            A formula that evaluates the given formula.
        """
        if channel_key in self._power_3_phase_formulas:
            return self._power_3_phase_formulas[channel_key]

        if formula_str == "0.0":
            formula_str = f"coalesce(#{NON_EXISTING_COMPONENT_ID}, 0.0)"

        formula = Formula3Phase(
            name=channel_key,
            phase_1=parse(
                name=channel_key + "_phase_1",
                formula=formula_str,
                telemetry_fetcher=self._telemetry_fetcher(
                    Metric.AC_POWER_ACTIVE_PHASE_1
                ),
                create_method=Power.from_watts,
            ),
            phase_2=parse(
                name=channel_key + "_phase_2",
                formula=formula_str,
                telemetry_fetcher=self._telemetry_fetcher(
                    Metric.AC_POWER_ACTIVE_PHASE_2
                ),
                create_method=Power.from_watts,
            ),
            phase_3=parse(
                name=channel_key + "_phase_3",
                formula=formula_str,
                telemetry_fetcher=self._telemetry_fetcher(
                    Metric.AC_POWER_ACTIVE_PHASE_3
                ),
                create_method=Power.from_watts,
            ),
        )
        self._power_3_phase_formulas[channel_key] = formula

        return formula

    def from_current_3_phase_formula(
        self, channel_key: str, formula_str: str
    ) -> Formula3Phase[Current]:
        """Get a receiver from the 3-phase current formula represented by the given strings.

        Args:
            channel_key: A string to uniquely identify the formula.  This
                usually includes the formula itself.
            formula_str: The formula string.

        Returns:
            A formula that evaluates the given formula.
        """
        if channel_key in self._current_3_phase_formulas:
            return self._current_3_phase_formulas[channel_key]

        if formula_str == "0.0":
            formula_str = f"coalesce(#{NON_EXISTING_COMPONENT_ID}, 0.0)"

        formula = Formula3Phase(
            name=channel_key,
            phase_1=parse(
                name=channel_key + "_phase_1",
                formula=formula_str,
                telemetry_fetcher=self._telemetry_fetcher(Metric.AC_CURRENT_PHASE_1),
                create_method=Current.from_amperes,
            ),
            phase_2=parse(
                name=channel_key + "_phase_2",
                formula=formula_str,
                telemetry_fetcher=self._telemetry_fetcher(Metric.AC_CURRENT_PHASE_2),
                create_method=Current.from_amperes,
            ),
            phase_3=parse(
                name=channel_key + "_phase_3",
                formula=formula_str,
                telemetry_fetcher=self._telemetry_fetcher(Metric.AC_CURRENT_PHASE_3),
                create_method=Current.from_amperes,
            ),
        )
        self._current_3_phase_formulas[channel_key] = formula

        return formula

    async def stop(self) -> None:
        """Stop all formulas."""
        for pf in self.power_formulas.values():
            await pf.stop()
        self.power_formulas.clear()

        for rpf in self._reactive_power_formulas.values():
            await rpf.stop()
        self._reactive_power_formulas.clear()

        for p3pf in self._power_3_phase_formulas.values():
            await p3pf.stop()
        self._power_3_phase_formulas.clear()

        for c3pf in self._current_3_phase_formulas.values():
            await c3pf.stop()
        self._current_3_phase_formulas.clear()

    def _telemetry_fetcher(self, metric: Metric) -> ResampledStreamFetcher:
        """Create a ResampledStreamFetcher for the given metric.

        Args:
            metric: The metric to fetch.

        Returns:
            A ResampledStreamFetcher for the given metric.
        """
        return ResampledStreamFetcher(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
            metric,
        )
