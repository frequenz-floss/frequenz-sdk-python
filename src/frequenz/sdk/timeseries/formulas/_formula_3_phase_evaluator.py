# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""An evaluator for three-phase formulas."""

import asyncio
import logging
from typing import Generic

from frequenz.channels import Broadcast, ReceiverStoppedError, Sender
from typing_extensions import override

from ...actor import Actor
from .._base_types import QuantityT, Sample3Phase
from . import _ast
from ._formula import Formula
from ._formula_evaluator import synchronize_receivers

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
            phase_1: The formula for phase 1
            phase_2: The formula for phase 2.
            phase_3: The formula for phase 3.
            output_channel: The channel to send evaluated samples to.
        """
        super().__init__()

        self._phase_1_formula: Formula[QuantityT] = phase_1
        self._phase_2_formula: Formula[QuantityT] = phase_2
        self._phase_3_formula: Formula[QuantityT] = phase_3
        self._components: list[_ast.TelemetryStream[QuantityT]] = [
            _ast.TelemetryStream(
                None,
                "phase_1",
                phase_1.new_receiver(),
            ),
            _ast.TelemetryStream(
                None,
                "phase_2",
                phase_2.new_receiver(),
            ),
            _ast.TelemetryStream(
                None,
                "phase_3",
                phase_3.new_receiver(),
            ),
        ]
        self._output_channel: Broadcast[Sample3Phase[QuantityT]] = output_channel
        self._output_sender: Sender[Sample3Phase[QuantityT]] = (
            self._output_channel.new_sender()
        )

    @override
    async def _run(self) -> None:
        """Run the three-phase formula evaluator actor."""
        await synchronize_receivers(self._components)

        while True:
            phase_1_sample = self._components[0].latest_sample
            phase_2_sample = self._components[1].latest_sample
            phase_3_sample = self._components[2].latest_sample

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

            sample_3phase = Sample3Phase(
                timestamp=phase_1_sample.timestamp,
                value_p1=phase_1_sample.value,
                value_p2=phase_2_sample.value,
                value_p3=phase_3_sample.value,
            )

            await self._output_sender.send(sample_3phase)

            fetch_results = await asyncio.gather(
                *(comp.fetch_next() for comp in self._components),
                return_exceptions=True,
            )
            if e := next((e for e in fetch_results if isinstance(e, Exception)), None):
                if isinstance(e, (StopAsyncIteration, ReceiverStoppedError)):
                    _logger.debug(
                        "input streams closed; stopping three-phase formula evaluator."
                    )
                    await self._output_channel.close()
                    return
                raise e
