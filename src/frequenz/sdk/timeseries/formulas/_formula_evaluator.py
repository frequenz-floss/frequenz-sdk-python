# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""An evaluator for a formula represented as an AST."""

import logging
from typing import Generic

from frequenz.channels import Broadcast, ReceiverStoppedError, Sender
from typing_extensions import override

from ...actor import Actor
from .._base_types import QuantityT, Sample
from ._base_ast_node import AstNode
from ._resampled_stream_fetcher import ResampledStreamFetcher

_logger = logging.getLogger(__name__)


class FormulaEvaluatingActor(Generic[QuantityT], Actor):
    """An evaluator for a formula represented as an AST."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        root: AstNode[QuantityT],
        output_channel: Broadcast[Sample[QuantityT]],
        metric_fetcher: ResampledStreamFetcher | None = None,
    ) -> None:
        """Create a `FormulaEvaluatingActor` instance.

        Args:
            root: The root node of the formula AST.
            output_channel: The channel to send evaluated samples to.
            metric_fetcher: An optional metric fetcher that needs to be started
                before the formula can be evaluated.
        """
        super().__init__()

        self._root: AstNode[QuantityT] = root
        self._metric_fetcher: ResampledStreamFetcher | None = metric_fetcher
        self._output_channel: Broadcast[Sample[QuantityT]] = output_channel

        self._output_sender: Sender[Sample[QuantityT]] = output_channel.new_sender()

    @override
    async def _run(self) -> None:
        """Run the formula evaluator actor."""
        while True:
            try:
                res = await self._root.evaluate()
                if isinstance(res, Sample):
                    next_sample = res
                else:
                    _logger.debug(
                        "No input samples available; stopping formula evaluator. (%s)",
                        self._root,
                    )
                    return
                await self._output_sender.send(next_sample)
            except (StopAsyncIteration, ReceiverStoppedError):
                _logger.debug(
                    "No more input samples available; stopping formula evaluator. (%s)",
                    self._root,
                )
                await self._output_channel.close()
                return
            except Exception:  # pylint: disable=broad-except
                _logger.exception("Error evaluating formula %s", self._root)
                await self._output_channel.close()
                return
