# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""An evaluator for three-phase formulas."""

import logging
from typing import Generic

from frequenz.channels import Broadcast, ReceiverStoppedError, Sender
from typing_extensions import override

from ...actor import Actor
from .._base_types import QuantityT, Sample, Sample3Phase
from . import _ast
from ._base_ast_node import AstNode, NodeSynchronizer
from ._formula import Formula, metric_fetcher

_logger = logging.getLogger(__name__)


class Formula3PhaseEvaluatingActor(Generic[QuantityT], Actor):
    """An evaluator for three-phase formulas."""

    def __init__(
        self,
        phase_1: Formula[QuantityT],
        phase_2: Formula[QuantityT],
        phase_3: Formula[QuantityT],
        output_channel: Broadcast[Sample3Phase[QuantityT]],
    ) -> None:
        """Initialize this instance.

        Args:
            phase_1: The formula for phase 1.
            phase_2: The formula for phase 2.
            phase_3: The formula for phase 3.
            output_channel: The channel to send evaluated samples to.
        """
        super().__init__()

        self._phase_1_formula: Formula[QuantityT] = phase_1
        self._phase_2_formula: Formula[QuantityT] = phase_2
        self._phase_3_formula: Formula[QuantityT] = phase_3
        self._components: list[AstNode[QuantityT]] = [
            _ast.TelemetryStream(
                source="phase_1",
                metric_fetcher=metric_fetcher(phase_1),
                create_method=phase_1._create_method,  # pylint: disable=protected-access
            ),
            _ast.TelemetryStream(
                source="phase_2",
                metric_fetcher=metric_fetcher(phase_2),
                create_method=phase_2._create_method,  # pylint: disable=protected-access
            ),
            _ast.TelemetryStream(
                source="phase_3",
                metric_fetcher=metric_fetcher(phase_3),
                create_method=phase_3._create_method,  # pylint: disable=protected-access
            ),
        ]
        self._output_channel: Broadcast[Sample3Phase[QuantityT]] = output_channel
        self._output_sender: Sender[Sample3Phase[QuantityT]] = (
            self._output_channel.new_sender()
        )
        self._synchronizer: NodeSynchronizer[QuantityT] = NodeSynchronizer()

    @override
    async def _run(self) -> None:
        """Run the three-phase formula evaluator actor."""
        while True:
            try:
                phase_1_sample, phase_2_sample, phase_3_sample = (
                    await self._synchronizer.evaluate(self._components)
                )
            except (StopAsyncIteration, ReceiverStoppedError):
                _logger.debug(
                    "input streams closed; stopping three-phase formula evaluator."
                )
                await self._output_channel.close()
                return

            if (
                phase_1_sample is None
                or phase_2_sample is None
                or phase_3_sample is None
            ):
                _logger.debug(
                    "One of the three phase samples is None, stopping the evaluator."
                )
                await self._output_channel.close()
                return

            if not (
                isinstance(phase_1_sample, Sample)
                and isinstance(phase_2_sample, Sample)
                and isinstance(phase_3_sample, Sample)
            ):
                # This should never happen because the components are Formula
                # instances
                raise RuntimeError("Expected all phase samples to be of type Sample")

            sample_3phase = Sample3Phase(
                timestamp=phase_1_sample.timestamp,
                value_p1=phase_1_sample.value,
                value_p2=phase_2_sample.value,
                value_p3=phase_3_sample.value,
            )

            await self._output_sender.send(sample_3phase)
