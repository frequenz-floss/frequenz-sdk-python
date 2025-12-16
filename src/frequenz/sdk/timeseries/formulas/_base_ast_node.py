# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Formula AST node base class."""

import abc
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Generic

from typing_extensions import override

from ...timeseries import Sample
from ...timeseries._base_types import QuantityT

_logger = logging.getLogger(__name__)

_MAX_SYNC_RETRIES = 10


@dataclass(kw_only=True)
class AstNode(abc.ABC, Generic[QuantityT]):
    """An abstract syntax tree node representing a formula expression."""

    span: tuple[int, int] | None = None
    """The span (start, end) of the expression in the input string."""

    @abc.abstractmethod
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the expression and return its numerical value."""

    @abc.abstractmethod
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the node."""

    @override
    def __str__(self) -> str:
        """Return the string representation of the node."""
        return self.format()

    @abc.abstractmethod
    async def subscribe(self) -> None:
        """Subscribe to any data streams needed by this node."""

    @abc.abstractmethod
    async def unsubscribe(self) -> None:
        """Unsubscribe from any data streams used by this node."""


class NodeSynchronizer(Generic[QuantityT]):
    """A helper class to synchronize multiple AST nodes."""

    def __init__(self) -> None:
        """Initialize this instance."""
        self._synchronized_nodes: int = 0
        self._latest_values: dict[int, Sample[QuantityT] | QuantityT | None] = {}

    async def evaluate(
        self,
        nodes: list[AstNode[QuantityT]],
        sync_to_first_node: bool = False,
    ) -> list[Sample[QuantityT] | QuantityT | None]:
        """Synchronize and evaluate multiple AST nodes.

        Args:
            nodes: The AST nodes to synchronize and evaluate.
            sync_to_first_node: If True, synchronize all nodes to the timestamp
                of the first node. If False, synchronize to the latest timestamp
                among all nodes.

        Returns:
            A list containing the evaluated values of the nodes.
        """
        if self._synchronized_nodes != len(nodes) or self._latest_values:
            _logger.debug(
                "Synchronizing %d AST nodes (sync_to_first_node=%s).",
                len(nodes),
                sync_to_first_node,
            )
            _ = await asyncio.gather(*(node.subscribe() for node in nodes))
            values: list[Sample[QuantityT] | QuantityT | None] = []
            for node in nodes:
                value = self._latest_values.pop(id(node), None)
                if value is None:
                    value = await node.evaluate()
                values.append(value)

            if sync_to_first_node:
                target_timestamp = None
                for value in values:
                    if isinstance(value, Sample):
                        target_timestamp = value.timestamp
                        break
            else:
                target_timestamp = max(
                    (value.timestamp for value in values if isinstance(value, Sample)),
                    default=None,
                )

            if target_timestamp is None:
                self._synchronized_nodes = len(nodes)
                return values

            return await self._synchronize_to_timestamp(values, nodes, target_timestamp)

        return await asyncio.gather(*(node.evaluate() for node in nodes))

    async def _synchronize_to_timestamp(
        self,
        values: list[Sample[QuantityT] | QuantityT | None],
        nodes: list[AstNode[QuantityT]],
        target_timestamp: datetime,
    ) -> list[Sample[QuantityT] | QuantityT | None]:
        for i, value in enumerate(values):
            if isinstance(value, Sample):
                ctr = 0
                while ctr < _MAX_SYNC_RETRIES and value.timestamp < target_timestamp:
                    value = await nodes[i].evaluate()
                    if not isinstance(value, Sample):
                        raise RuntimeError(
                            "Subsequent AST node evaluation did not return a Sample"
                        )
                    values[i] = value
                    ctr += 1
                if ctr >= _MAX_SYNC_RETRIES and value.timestamp < target_timestamp:
                    raise RuntimeError(
                        "Could not synchronize AST node evaluations after "
                        + f"{_MAX_SYNC_RETRIES} tries"
                    )
                if value.timestamp > target_timestamp:
                    self._latest_values[id(nodes[i])] = value
                    values[i] = Sample(target_timestamp, None)

        self._synchronized_nodes = len(nodes)

        return values
