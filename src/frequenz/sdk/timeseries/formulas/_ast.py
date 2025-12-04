# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Formula AST nodes and evaluation logic."""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass

from frequenz.channels import Receiver
from frequenz.quantities import Quantity
from typing_extensions import TypeIs, override

from ..._internal._math import is_close_to_zero
from .._base_types import QuantityT, Sample
from ._base_ast_node import AstNode
from ._functions import Function

_logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class TelemetryStream(AstNode[QuantityT]):
    """A AST node that retrieves values from a component's telemetry stream."""

    source: str
    metric_fetcher: (
        Callable[
            [], Coroutine[None, None, Receiver[Sample[QuantityT] | Sample[Quantity]]]
        ]
        | None
    ) = None
    create_method: Callable[[float], QuantityT]
    _stream: Receiver[Sample[QuantityT] | Sample[Quantity]] | None = None
    _latest_sample: Sample[QuantityT] | None = None

    def __post_init__(self) -> None:
        """Validate at least one of stream or metric_fetcher is set."""
        if self._stream is None and self.metric_fetcher is None:
            raise ValueError(
                "Either stream or metric_fetcher must be provided for "
                + "TelemetryStream node."
            )

    @property
    def latest_sample(self) -> Sample[QuantityT] | None:
        """Return the latest fetched sample for this component."""
        return self._latest_sample

    @override
    async def evaluate(self) -> Sample[QuantityT] | None:
        """Return the base value of the latest sample for this component."""
        if self._latest_sample is None:
            raise ValueError("Next value has not been fetched yet.")
        return self._latest_sample

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the telemetry stream node."""
        return f"{self.source}"

    async def fetch_next(self) -> None:
        """Fetch the next value for this component and store it internally."""
        if self._stream is None:
            await self._fetch_stream()
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

    async def _fetch_stream(self) -> None:
        """Subscribe to the telemetry stream for this component."""
        if self._stream is not None:
            return
        if self.metric_fetcher is None:
            raise RuntimeError("Metric fetcher is not set for TelemetryStream node.")
        self._stream = await self.metric_fetcher()

    def _is_quantity_sample(
        self, sample: Sample[QuantityT] | Sample[Quantity]
    ) -> TypeIs[Sample[Quantity]]:
        return isinstance(sample.value, Quantity)


@dataclass(kw_only=True)
class FunCall(AstNode[QuantityT]):
    """A function call in the formula."""

    function: Function[QuantityT]

    @override
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the function call with its arguments."""
        return await self.function()

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the function call node."""
        return self.function.format()


@dataclass(kw_only=True)
class Constant(AstNode[QuantityT]):
    """A constant numerical value in the formula."""

    value: QuantityT

    @override
    async def evaluate(self) -> QuantityT | None:
        """Return the constant value."""
        return self.value

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the constant node."""
        return str(self.value.base_value)


@dataclass(kw_only=True)
class Add(AstNode[QuantityT]):
    """Addition operation node."""

    left: AstNode[QuantityT]
    right: AstNode[QuantityT]

    @override
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the addition of the left and right nodes."""
        left = await self.left.evaluate()
        right = await self.right.evaluate()
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


@dataclass(kw_only=True)
class Sub(AstNode[QuantityT]):
    """Subtraction operation node."""

    left: AstNode[QuantityT]
    right: AstNode[QuantityT]

    @override
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the subtraction of the right node from the left node."""
        left = await self.left.evaluate()
        right = await self.right.evaluate()
        print("Sub.evaluate:", left, right)
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


@dataclass(kw_only=True)
class Mul(AstNode[QuantityT]):
    """Multiplication operation node."""

    left: AstNode[QuantityT]
    right: AstNode[QuantityT]

    @override
    async def evaluate(self) -> Sample[QuantityT] | QuantityT | None:
        """Evaluate the multiplication of the left and right nodes."""
        left = await self.left.evaluate()
        right = await self.right.evaluate()
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
                return left.__class__._new(  # pylint: disable=protected-access
                    left.base_value * right.base_value
                )
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


@dataclass(kw_only=True)
class Div(AstNode[QuantityT]):
    """Division operation node."""

    left: AstNode[QuantityT]
    right: AstNode[QuantityT]

    @override
    async def evaluate(self) -> QuantityT | None:
        """Evaluate the division of the left node by the right node."""
        left = await self.left.evaluate()
        right = await self.right.evaluate()
        match left, right:
            case Sample(), Sample():
                if left.value is None:
                    return None
                if right.value is None:
                    return None
                if is_close_to_zero(right.value.base_value):
                    _logger.warning("Division by zero encountered in formula.")
                    return None
                return left.value / right.value.base_value
            case Quantity(), Quantity():
                if is_close_to_zero(right.base_value):
                    _logger.warning("Division by zero encountered in formula.")
                    return None
                return left / right.base_value
            case (Sample(), Quantity()):
                if is_close_to_zero(right.base_value):
                    _logger.warning("Division by zero encountered in formula.")
                    return None
                return None if left.value is None else left.value / right.base_value
            case (Quantity(), Sample()):
                if right.value is None:
                    return None
                if is_close_to_zero(right.value.base_value):
                    _logger.warning("Division by zero encountered in formula.")
                    return None
                return left / right.value.base_value
            case (None, _) | (_, None):
                return None
        return None

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of the division node."""
        return f"{self.left.format(True)} / {self.right.format(True)}"
