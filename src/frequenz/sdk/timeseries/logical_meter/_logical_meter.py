# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A logical meter for calculating high level metrics for a microgrid."""

import asyncio
import logging
import uuid
from typing import Dict, List

from frequenz.channels import Broadcast, Receiver, Sender

from ...actor import ChannelRegistry, ComponentMetricRequest
from ...microgrid.component import ComponentMetricId
from .. import Sample
from ._formula_engine import FormulaEngine
from ._resampled_formula_builder import ResampledFormulaBuilder

logger = logging.Logger(__name__)


class LogicalMeter:
    """A logical meter for calculating high level metrics in a microgrid.

    LogicalMeter can be used to run formulas on resampled component metric streams.

    Formulas can have Component IDs that are preceeded by a pound symbol("#"), and these
    operators: +, -, *, /, (, ).

    For example, the input string: "#20 + #5" is a formula for adding metrics from two
    components with ids 20 and 5.
    """

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        resampler_subscription_sender: Sender[ComponentMetricRequest],
    ) -> None:
        """Create a `LogicalMeter instance`.

        Args:
            channel_registry: A channel registry instance shared with the resampling
                actor.
            resampler_subscription_sender: A sender for sending metric requests to the
                resampling actor.
        """
        self._channel_registry = channel_registry
        self._resampler_subscription_sender = resampler_subscription_sender

        # Use a randomly generated uuid to create a unique namespace name for the local
        # meter to use when communicating with the resampling actor.
        self._namespace = f"logical-meter-{uuid.uuid4()}"

        self._output_channels: Dict[str, Broadcast[Sample]] = {}
        self._tasks: List[asyncio.Task[None]] = []

    async def _engine_from_formula_string(
        self, formula: str, metric_id: ComponentMetricId, nones_are_zeros: bool
    ) -> FormulaEngine:
        builder = ResampledFormulaBuilder(
            self._namespace,
            self._channel_registry,
            self._resampler_subscription_sender,
            metric_id,
        )
        return await builder.from_string(formula, nones_are_zeros)

    async def _run_formula(
        self, formula: FormulaEngine, sender: Sender[Sample]
    ) -> None:
        """Run the formula repeatedly and send the results to a channel.

        Args:
            formula: The formula to run.
            sender: A sender for sending the formula results to.
        """
        while True:
            try:
                msg = await formula.apply()
            except asyncio.CancelledError:
                logger.exception("LogicalMeter task cancelled")
                break
            except Exception as err:  # pylint: disable=broad-except
                logger.warning("Formula application failed: %s", err)
            else:
                await sender.send(msg)

    async def start_formula(
        self,
        formula: str,
        component_metric_id: ComponentMetricId,
        nones_are_zeros: bool = False,
    ) -> Receiver[Sample]:
        """Start execution of the given formula name.

        Args:
            formula: formula to execute.
            component_metric_id: The metric ID to use when fetching receivers from the
                resampling actor.
            nones_are_zeros: Whether to treat None values from the stream as 0s.  If
                False, the returned value will be a None.

        Returns:
            A Receiver that streams values with the formulas applied.
        """
        channel_key = formula + component_metric_id.value
        if channel_key in self._output_channels:
            return self._output_channels[channel_key].new_receiver()

        formula_engine = await self._engine_from_formula_string(
            formula, component_metric_id, nones_are_zeros
        )
        out_chan = Broadcast[Sample](channel_key)
        self._output_channels[channel_key] = out_chan
        self._tasks.append(
            asyncio.create_task(
                self._run_formula(formula_engine, out_chan.new_sender())
            )
        )
        return out_chan.new_receiver()
