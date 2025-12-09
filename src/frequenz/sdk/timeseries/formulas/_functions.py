# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Function implementations for evaluating formulas."""

from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic

from frequenz.quantities import Quantity
from typing_extensions import override

from .._base_types import QuantityT, Sample
from ._base_ast_node import AstNode, NodeSynchronizer


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


class Coalesce(Function[QuantityT]):
    """A function that returns the first non-None argument."""

    @property
    @override
    def name(self) -> str:
        """Return the name of the function."""
        return "COALESCE"

    @override
    async def __call__(self) -> Sample[QuantityT] | QuantityT | None:
        """Return the first non-None argument."""
        ts: datetime | None = None

        args = await self._synchronizer.evaluate(self.params)
        for arg in args:
            match arg:
                case Sample(timestamp, value):
                    if value is not None:
                        return arg
                    ts = timestamp
                case Quantity():
                    if ts is not None:
                        return Sample(timestamp=ts, value=arg)
                    return arg
                case None:
                    continue
        if ts is not None:
            return Sample(timestamp=ts, value=None)
        return None


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
