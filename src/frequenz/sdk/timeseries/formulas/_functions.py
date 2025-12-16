# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Function implementations for evaluating formulas."""

from __future__ import annotations

import abc
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic

from frequenz.quantities import Quantity
from typing_extensions import override

from .._base_types import QuantityT, Sample
from ._ast import Constant
from ._base_ast_node import AstNode, NodeSynchronizer

_logger = logging.getLogger(__name__)


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

    @override
    async def subscribe(self) -> None:
        """Subscribe to any data streams needed by the function."""
        await self.function.subscribe()

    @override
    async def unsubscribe(self) -> None:
        """Unsubscribe from any data streams needed by the function."""
        await self.function.unsubscribe()


@dataclass
class Function(abc.ABC, Generic[QuantityT]):
    """A function that can be called in a formula expression."""

    params: list[AstNode[QuantityT]]
    _synchronizer: NodeSynchronizer[QuantityT] = field(
        init=False, default_factory=NodeSynchronizer
    )

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Return the name of the function."""

    @abc.abstractmethod
    async def __call__(self) -> Sample[QuantityT] | QuantityT | None:
        """Call the function with the given arguments."""

    def format(self) -> str:
        """Return a string representation of the function."""
        params_str = ", ".join(str(param) for param in self.params)
        return f"{self.name}({params_str})"

    async def subscribe(self) -> None:
        """Subscribe to any data streams needed by the function."""
        _ = await asyncio.gather(
            *(param.subscribe() for param in self.params),
        )

    async def unsubscribe(self) -> None:
        """Unsubscribe from any data streams needed by the function."""
        _ = await asyncio.gather(
            *(param.unsubscribe() for param in self.params),
        )

    @classmethod
    def from_string(
        cls, name: str, params: list[AstNode[QuantityT]]
    ) -> Function[QuantityT]:
        """Create a function instance from its name."""
        match name.upper():
            case "COALESCE":
                return Coalesce(params)
            case "MAX":
                return Max(params)
            case "MIN":
                return Min(params)
            case _:
                raise ValueError(f"Unknown function name: {name}")


@dataclass
class Coalesce(Function[QuantityT]):
    """A function that returns the first non-None argument."""

    num_subscribed: int = 0

    @property
    @override
    def name(self) -> str:
        """Return the name of the function."""
        return "COALESCE"

    @override
    async def __call__(self) -> Sample[QuantityT] | QuantityT | None:
        """Return the first non-None argument."""
        ts: datetime | None = None

        if self.num_subscribed == 0:
            await self._subscribe_next_param()

        args = await self._synchronizer.evaluate(
            self.params[: self.num_subscribed], sync_to_first_node=True
        )
        for ctr, arg in enumerate(args, start=1):
            match arg:
                case Sample(timestamp, value):
                    if value is not None:
                        # Found a non-None value, unsubscribe from subsequent params
                        if ctr < self.num_subscribed:
                            await self._unsubscribe_all_params_after(ctr)
                        return arg
                    ts = timestamp
                case Quantity():
                    # Found a non-None value, unsubscribe from subsequent params
                    if ctr < self.num_subscribed:
                        await self._unsubscribe_all_params_after(ctr)
                    if ts is not None:
                        return Sample(timestamp=ts, value=arg)
                    return arg
                case None:
                    continue
        # Don't have a non-None value yet, subscribe to the next parameter for
        # next time and return None for now, unless the next value is a constant.
        next_value: Sample[QuantityT] | QuantityT | None = None
        await self._subscribe_next_param()

        if isinstance(self.params[self.num_subscribed - 1], Constant):
            next_value = await self.params[self.num_subscribed - 1].evaluate()
        if isinstance(next_value, Sample):
            return next_value

        if ts is not None:
            return Sample(timestamp=ts, value=next_value)
        return next_value

    @override
    async def subscribe(self) -> None:
        """Subscribe to the first parameter if not already subscribed."""
        if self.num_subscribed == 0:
            await self._subscribe_next_param()

    async def _subscribe_next_param(self) -> None:
        """Subscribe to the next parameter."""
        if self.num_subscribed < len(self.params):
            _logger.debug(
                "Coalesce subscribing to param %d: %s",
                self.num_subscribed + 1,
                self.params[self.num_subscribed],
            )
            await self.params[self.num_subscribed].subscribe()
            self.num_subscribed += 1

    async def _unsubscribe_all_params_after(self, index: int) -> None:
        """Unsubscribe from parameters after the given index."""
        for param in self.params[index:]:
            _logger.debug(
                "Coalesce unsubscribing from param: %s",
                param,
            )
            await param.unsubscribe()
        self.num_subscribed = index


@dataclass
class Max(Function[QuantityT]):
    """A function that returns the maximum of the arguments."""

    @property
    @override
    def name(self) -> str:
        """Return the name of the function."""
        return "MAX"

    @override
    async def __call__(self) -> Sample[QuantityT] | QuantityT | None:
        """Return the maximum of the arguments."""
        max_value: QuantityT | None = None
        ts: datetime | None = None
        args = await self._synchronizer.evaluate(self.params)
        for arg in args:
            match arg:
                case Sample(value=value, timestamp=timestamp):
                    ts = timestamp
                    if value is None:
                        return arg
                    if max_value is None or value > max_value:
                        max_value = value
                case Quantity():
                    if max_value is None or arg > max_value:
                        max_value = arg
                case None:
                    return None
        if ts is not None:
            return Sample(timestamp=ts, value=max_value)

        return max_value


@dataclass
class Min(Function[QuantityT]):
    """A function that returns the minimum of the arguments."""

    @property
    @override
    def name(self) -> str:
        """Return the name of the function."""
        return "MIN"

    @override
    async def __call__(self) -> Sample[QuantityT] | QuantityT | None:
        """Return the minimum of the arguments."""
        min_value: QuantityT | None = None
        ts: datetime | None = None
        args = await self._synchronizer.evaluate(self.params)
        for arg in args:
            match arg:
                case Sample(value=value, timestamp=timestamp):
                    ts = timestamp
                    if value is None:
                        return arg
                    if min_value is None or value < min_value:
                        min_value = value
                case Quantity():
                    if min_value is None or arg < min_value:
                        min_value = arg
                case None:
                    return None
        if ts is not None:
            return Sample(timestamp=ts, value=min_value)

        return min_value
