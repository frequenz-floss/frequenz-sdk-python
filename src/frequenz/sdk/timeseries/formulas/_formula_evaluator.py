# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""An evaluator for a formula represented as an AST."""


import asyncio
import logging
from datetime import datetime
from typing import Generic

from frequenz.channels import Broadcast, ReceiverStoppedError, Sender
from typing_extensions import override

from ...actor import Actor
from .._base_types import QuantityT, Sample
from . import _ast
from ._base_ast_node import AstNode
from ._resampled_stream_fetcher import ResampledStreamFetcher

_logger = logging.getLogger(__name__)


class FormulaEvaluatingActor(Generic[QuantityT], Actor):
    """An evaluator for a formula represented as an AST."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        root: AstNode[QuantityT],
        components: list[_ast.TelemetryStream[QuantityT]],
        output_channel: Broadcast[Sample[QuantityT]],
        metric_fetcher: ResampledStreamFetcher | None = None,
    ) -> None:
        """Create a `FormulaEvaluatingActor` instance.

        Args:
            root: The root node of the formula AST.
            components: The telemetry streams that the formula depends on.
            output_channel: The channel to send evaluated samples to.
            metric_fetcher: An optional metric fetcher that needs to be started
                before the formula can be evaluated.
        """
        super().__init__()

        self._root: AstNode[QuantityT] = root
        self._components: list[_ast.TelemetryStream[QuantityT]] = components
        self._metric_fetcher: ResampledStreamFetcher | None = metric_fetcher
        self._output_channel: Broadcast[Sample[QuantityT]] = output_channel

        self._output_sender: Sender[Sample[QuantityT]] = output_channel.new_sender()

    @override
    async def _run(self) -> None:
        """Run the formula evaluator actor."""
        await synchronize_receivers(self._components)

        while True:
            try:
                timestamp = next(
                    comp.latest_sample.timestamp
                    for comp in self._components
                    if comp.latest_sample is not None
                )

                res = await self._root.evaluate()
                next_sample = res if isinstance(res, Sample) else Sample(timestamp, res)
                await self._output_sender.send(next_sample)
            except (StopAsyncIteration, StopIteration):
                _logger.debug(
                    "No more input samples available; stopping formula evaluator. (%s)",
                    self._root,
                )
                await self._output_channel.close()
                return
            except Exception as e:  # pylint: disable=broad-except
                _logger.error(
                    "Error evaluating formula %s: %s", self._root, e, exc_info=True
                )
                await self._output_channel.close()
                return

            fetch_results = await asyncio.gather(
                *(comp.fetch_next() for comp in self._components),
                return_exceptions=True,
            )
            if ex := next((e for e in fetch_results if isinstance(e, Exception)), None):
                if isinstance(ex, (StopAsyncIteration, ReceiverStoppedError)):
                    _logger.debug(
                        "input streams closed; stopping formula evaluator. (%s)",
                        self._root,
                    )
                    await self._output_channel.close()
                    return
                raise ex


async def synchronize_receivers(
    components: list[_ast.TelemetryStream[QuantityT]],
) -> None:
    """Synchronize the given telemetry stream receivers."""
    _ = await asyncio.gather(
        *(comp.fetch_next() for comp in components),
    )
    latest_ts: datetime | None = None
    for comp in components:
        if comp.latest_sample is not None and (
            latest_ts is None or comp.latest_sample.timestamp > latest_ts
        ):
            latest_ts = comp.latest_sample.timestamp
    if latest_ts is None:
        _logger.debug("No samples available to synchronize receivers.")
        return
    for comp in components:
        if comp.latest_sample is None:
            raise RuntimeError("Can't synchronize receivers.")
        ctr = 0
        while ctr < 10 and comp.latest_sample.timestamp < latest_ts:
            await comp.fetch_next()
            ctr += 1
