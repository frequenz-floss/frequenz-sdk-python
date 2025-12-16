# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Formula AST nodes and evaluation logic."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field

from frequenz.channels import Receiver
from frequenz.quantities import Quantity
from typing_extensions import TypeIs, override

from ..._internal._math import is_close_to_zero
from .._base_types import QuantityT, Sample
from ._base_ast_node import AstNode, NodeSynchronizer

_logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class TelemetryStream(AstNode[QuantityT]):
    """A AST node that retrieves values from a component's telemetry stream."""

    source: str
    """The source formula string."""

    metric_fetcher: (
        Callable[
            [], Coroutine[None, None, Receiver[Sample[QuantityT] | Sample[Quantity]]]
        ]
        | None
    )
    """A callable that fetches the telemetry stream for this component."""

    create_method: Callable[[float], QuantityT]
    """A method to create QuantityT from a base float value."""

    _stream: Receiver[Sample[QuantityT] | Sample[Quantity]] | None = None
    _latest_sample: Sample[QuantityT] | None = None

    @property
    def latest_sample(self) -> Sample[QuantityT] | None:
        """Return the latest fetched sample for this component."""
        return self._latest_sample

    @override
    async def evaluate(self) -> Sample[QuantityT] | None:
        """Return the base value of the latest sample for this component."""
        await self._fetch_next()
        return self._latest_sample

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the telemetry stream node."""
        return f"{self.source}"

    async def _fetch_next(self) -> None:
        """Fetch the next value for this component and store it internally."""
        if self._stream is None:
            await self.subscribe()
        assert self._stream is not None

        latest_sample = await anext(self._stream)
        # pylint: disable-next=fixme
        # TODO: convert to QuantityT if needed only at the end in the evaluator.
        if self._is_quantity_sample(latest_sample):
            assert latest_sample.value is not None
            self._latest_sample = Sample(
                timestamp=latest_sample.timestamp,
                value=self.create_method(latest_sample.value.base_value),
            )
        else:
            self._latest_sample = latest_sample

    @override
    async def subscribe(self) -> None:
        """Subscribe to the telemetry stream for this component."""
        if self._stream is not None:
            return
        _logger.debug("Subscribing to telemetry stream for %s", self.source)
        if self.metric_fetcher is None:
            raise RuntimeError("Metric fetcher is not set for TelemetryStream node.")
        self._stream = await self.metric_fetcher()

    @override
    async def unsubscribe(self) -> None:
        """Unsubscribe from the telemetry stream for this component."""
        if self._stream is None:
            return
        _logger.debug("Unsubscribing from telemetry stream for %s", self.source)
        self._stream.close()
        self._stream = None

    def _is_quantity_sample(
        self, sample: Sample[QuantityT] | Sample[Quantity]
    ) -> TypeIs[Sample[Quantity]]:
        return isinstance(sample.value, Quantity)


@dataclass(kw_only=True)
class Constant(AstNode[QuantityT]):
    """A constant numerical value in the formula."""

    value: QuantityT
    """The constant value."""

    @override
    async def evaluate(self) -> QuantityT | None:
        """Return the constant value."""
        return self.value

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the constant node."""
        return str(self.value.base_value)

    @override
    async def subscribe(self) -> None:
        """No-op for constant node."""

    @override
    async def unsubscribe(self) -> None:
        """No-op for constant node."""


@dataclass(kw_only=True)
class Add(AstNode[QuantityT]):
    """Addition operation node."""

    left: AstNode[QuantityT]
    """The left operand."""

    right: AstNode[QuantityT]
    """The right operand."""

    _synchronizer: NodeSynchronizer[QuantityT] = field(
        init=False, default_factory=NodeSynchronizer
    )

    @override
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the addition of the left and right nodes."""
        left, right = await self._synchronizer.evaluate([self.left, self.right])
        match left, right:
            case Sample(), Sample():
                if left.value is None:
                    return left
                if right.value is None:
                    return right
                return Sample(
                    timestamp=left.timestamp,
                    value=left.value + right.value,
                )
            case Quantity(), Quantity():
                return left + right
            case (Sample(), Quantity()):
                return (
                    left
                    if left.value is None
                    else Sample(
                        timestamp=left.timestamp,
                        value=left.value + right,
                    )
                )
            case (Quantity(), Sample()):
                return (
                    right
                    if right.value is None
                    else Sample(
                        timestamp=right.timestamp,
                        value=left + right.value,
                    )
                )
            case (None, _) | (_, None):
                return None
        return None

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the addition node."""
        expr = f"{self.left} + {self.right}"
        if wrap:
            expr = f"({expr})"
        return expr

    @override
    async def subscribe(self) -> None:
        """Subscribe to any data streams needed by this node."""
        _ = await asyncio.gather(
            self.left.subscribe(),
            self.right.subscribe(),
        )

    @override
    async def unsubscribe(self) -> None:
        """Unsubscribe from any data streams needed by this node."""
        _ = await asyncio.gather(
            self.left.unsubscribe(),
            self.right.unsubscribe(),
        )


@dataclass(kw_only=True)
class Sub(AstNode[QuantityT]):
    """Subtraction operation node."""

    left: AstNode[QuantityT]
    """The left operand."""

    right: AstNode[QuantityT]
    """The right operand."""

    _synchronizer: NodeSynchronizer[QuantityT] = field(
        init=False, default_factory=NodeSynchronizer
    )

    @override
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the subtraction of the right node from the left node."""
        left, right = await self._synchronizer.evaluate([self.left, self.right])
        match left, right:
            case Sample(), Sample():
                if left.value is None:
                    return left
                if right.value is None:
                    return right
                return Sample(
                    timestamp=left.timestamp,
                    value=left.value - right.value,
                )
            case Quantity(), Quantity():
                return left - right
            case (Sample(), Quantity()):
                return (
                    left
                    if left.value is None
                    else Sample(
                        timestamp=left.timestamp,
                        value=left.value - right,
                    )
                )
            case (Quantity(), Sample()):
                return (
                    right
                    if right.value is None
                    else Sample(
                        timestamp=right.timestamp,
                        value=left - right.value,
                    )
                )
            case (None, _) | (_, None):
                return None
        return None

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the subtraction node."""
        expr = f"{self.left} - {self.right.format(True)}"
        if wrap:
            expr = f"({expr})"
        return expr

    @override
    async def subscribe(self) -> None:
        """Subscribe to any data streams needed by this node."""
        _ = await asyncio.gather(
            self.left.subscribe(),
            self.right.subscribe(),
        )

    @override
    async def unsubscribe(self) -> None:
        """Unsubscribe from any data streams needed by this node."""
        _ = await asyncio.gather(
            self.left.unsubscribe(),
            self.right.unsubscribe(),
        )


@dataclass(kw_only=True)
class Mul(AstNode[QuantityT]):
    """Multiplication operation node."""

    left: AstNode[QuantityT]
    """The left operand."""

    right: AstNode[QuantityT]
    """The right operand."""

    _synchronizer: NodeSynchronizer[QuantityT] = field(
        init=False, default_factory=NodeSynchronizer
    )

    @override
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the multiplication of the left and right nodes."""
        left, right = await self._synchronizer.evaluate([self.left, self.right])
        match left, right:
            case Sample(), Sample():
                if left.value is None:
                    return left
                if right.value is None:
                    return right
                return Sample(
                    timestamp=left.timestamp,
                    value=left.value * right.value.base_value,
                )
            case Quantity(), Quantity():
                return left * right.base_value
            case (Sample(), Quantity()):
                return (
                    left
                    if left.value is None
                    else Sample(
                        timestamp=left.timestamp,
                        value=left.value * right.base_value,
                    )
                )
            case (Quantity(), Sample()):
                return (
                    right
                    if right.value is None
                    else Sample(
                        timestamp=right.timestamp,
                        value=right.value * left.base_value,
                    )
                )
            case (None, _) | (_, None):
                return None
        return None

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the multiplication node."""
        return f"{self.left.format(True)} * {self.right.format(True)}"

    @override
    async def subscribe(self) -> None:
        """Subscribe to any data streams needed by this node."""
        _ = await asyncio.gather(
            self.left.subscribe(),
            self.right.subscribe(),
        )

    @override
    async def unsubscribe(self) -> None:
        """Unsubscribe from any data streams needed by this node."""
        _ = await asyncio.gather(
            self.left.unsubscribe(),
            self.right.unsubscribe(),
        )


@dataclass(kw_only=True)
class Div(AstNode[QuantityT]):
    """Division operation node."""

    left: AstNode[QuantityT]
    """The left operand."""

    right: AstNode[QuantityT]
    """The right operand."""

    _synchronizer: NodeSynchronizer[QuantityT] = field(
        init=False, default_factory=NodeSynchronizer
    )

    @override
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the division of the left node by the right node."""
        left, right = await self._synchronizer.evaluate([self.left, self.right])
        match left, right:
            case Sample(), Sample():
                if left.value is None:
                    return left
                if right.value is None:
                    return right
                if is_close_to_zero(right.value.base_value):
                    _logger.warning("Division by zero encountered in formula.")
                    return Sample(left.timestamp, None)
                return Sample(left.timestamp, left.value / right.value.base_value)
            case Quantity(), Quantity():
                if is_close_to_zero(right.base_value):
                    _logger.warning("Division by zero encountered in formula.")
                    return None
                return left / right.base_value
            case (Sample(), Quantity()):
                if is_close_to_zero(right.base_value):
                    _logger.warning("Division by zero encountered in formula.")
                    return None
                return Sample(
                    left.timestamp,
                    None if left.value is None else left.value / right.base_value,
                )
            case (Quantity(), Sample()):
                if right.value and is_close_to_zero(right.value.base_value):
                    _logger.warning("Division by zero encountered in formula.")
                    return Sample(right.timestamp, None)
                return Sample(
                    right.timestamp,
                    None if right.value is None else left / right.value.base_value,
                )
            case (None, _) | (_, None):
                return None
        return None

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the division node."""
        return f"{self.left.format(True)} / {self.right.format(True)}"

    @override
    async def subscribe(self) -> None:
        """Subscribe to any data streams needed by this node."""
        _ = await asyncio.gather(
            self.left.subscribe(),
            self.right.subscribe(),
        )

    @override
    async def unsubscribe(self) -> None:
        """Unsubscribe from any data streams needed by this node."""
        _ = await asyncio.gather(
            self.left.unsubscribe(),
            self.right.unsubscribe(),
        )
