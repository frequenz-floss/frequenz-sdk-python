# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Formula AST node base class."""

import abc
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Generic

from typing_extensions import override

from ...timeseries import Sample
from ...timeseries._base_types import QuantityT


@dataclass(kw_only=True)
class AstNode(abc.ABC, Generic[QuantityT]):
    """An abstract syntax tree node representing a formula expression."""

    span: tuple[int, int] | None = None

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


class NodeSynchronizer(Generic[QuantityT]):
    """A helper class to synchronize multiple AST nodes."""

    def __init__(self) -> None:
        """Initialize this instance."""
        self._synchronized: bool = False

    async def evaluate(
        self,
        nodes: list[AstNode[QuantityT]],
        target_timestamp: datetime | None = None,
    ) -> list[Sample[QuantityT] | QuantityT | None]:
        """Synchronize and evaluate multiple AST nodes.

        Args:
            nodes: The AST nodes to synchronize and evaluate.
            target_timestamp: An optional maximum timestamp to synchronize to.

        Returns:
            A list containing the evaluated values of the nodes.

        Raises:
            RuntimeError: If synchronization fails after multiple attempts.
        """
        if not self._synchronized or target_timestamp is not None:
            _ = await asyncio.gather(*(node.subscribe() for node in nodes))
            values = [await node.evaluate() for node in nodes]

            target_timestamp = max(
                (value.timestamp for value in values if isinstance(value, Sample)),
                default=None,
            )
            if target_timestamp is None:
                self._synchronized = True
                return values

            for i, value in enumerate(values):
                if isinstance(value, Sample):
                    ctr = 0
                    while ctr < 10 and value.timestamp < target_timestamp:
                        value = await nodes[i].evaluate()
                        if not isinstance(value, Sample):
                            raise RuntimeError(
                                "Subsequent AST node evaluation did not return a Sample"
                            )
                        values[i] = value
                        ctr += 1
                    if ctr >= 10 and value.timestamp < target_timestamp:
                        raise RuntimeError(
                            "Could not synchronize AST node evaluations after 10 tries"
                        )
                    if value.timestamp > target_timestamp:
                        values[i] = Sample(target_timestamp, None)

            self._synchronized = True

            return values

        return [await node.evaluate() for node in nodes]
