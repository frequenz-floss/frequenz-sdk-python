# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Steps for building formula engines with."""

from __future__ import annotations

from abc import ABC, abstractmethod
from math import isinf, isnan
from typing import List, Optional

from frequenz.channels import Receiver

from .. import Sample
from ._exceptions import FormulaEngineError


class FormulaStep(ABC):
    """Represents an individual step/stage in a formula.

    Each step, when applied on to an evaluation stack, would pop its input parameters
    from the stack and push its result back in.
    """

    @abstractmethod
    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """

    @abstractmethod
    def apply(self, eval_stack: List[float]) -> None:
        """Apply a formula operation on the eval_stack.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """


class Adder(FormulaStep):
    """A formula step for adding two values."""

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "+"

    def apply(self, eval_stack: List[float]) -> None:
        """Extract two values from the stack, add them, push the result back in.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val2 = eval_stack.pop()
        val1 = eval_stack.pop()
        res = val1 + val2
        eval_stack.append(res)


class Subtractor(FormulaStep):
    """A formula step for subtracting one value from another."""

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "-"

    def apply(self, eval_stack: List[float]) -> None:
        """Extract two values from the stack, subtract them, push the result back in.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val2 = eval_stack.pop()
        val1 = eval_stack.pop()
        res = val1 - val2
        eval_stack.append(res)


class Multiplier(FormulaStep):
    """A formula step for multiplying two values."""

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "*"

    def apply(self, eval_stack: List[float]) -> None:
        """Extract two values from the stack, multiply them, push the result back in.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val2 = eval_stack.pop()
        val1 = eval_stack.pop()
        res = val1 * val2
        eval_stack.append(res)


class Divider(FormulaStep):
    """A formula step for dividing one value by another."""

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "/"

    def apply(self, eval_stack: List[float]) -> None:
        """Extract two values from the stack, divide them, push the result back in.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val2 = eval_stack.pop()
        val1 = eval_stack.pop()
        res = val1 / val2
        eval_stack.append(res)


class OpenParen(FormulaStep):
    """A no-op formula step used while building a prefix formula engine.

    Any OpenParen steps would get removed once a formula is built.
    """

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "("

    def apply(self, _: List[float]) -> None:
        """No-op."""


class Averager(FormulaStep):
    """A formula step for calculating average."""

    def __init__(self, fetchers: List[MetricFetcher]) -> None:
        """Create an `Averager` instance.

        Args:
            fetchers: MetricFetchers for the metrics that need to be averaged.
        """
        self._fetchers = fetchers

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return f"avg({', '.join(repr(f) for f in self._fetchers)})"

    def apply(self, eval_stack: List[float]) -> None:
        """Calculate average of given metrics, push the average to the eval_stack.

        Args:
            eval_stack: An evaluation stack, to append the calculated average to.

        Raises:
            FormulaEngineError: when metric fetchers are unable to fetch values.
        """
        value_count = 0
        total = 0.0
        for fetcher in self._fetchers:
            next_val = fetcher.value
            if next_val is None:
                raise FormulaEngineError(
                    "Unable to fetch a value from the resampling actor."
                )
            if next_val.value is None:
                continue
            value_count += 1
            total += next_val.value
        if value_count == 0:
            avg = 0.0
        else:
            avg = total / value_count

        eval_stack.append(avg)


class MetricFetcher(FormulaStep):
    """A formula step for fetching a value from a metric Receiver."""

    def __init__(
        self, name: str, stream: Receiver[Sample], nones_are_zeros: bool
    ) -> None:
        """Create a `MetricFetcher` instance.

        Args:
            name: The name of the metric.
            stream: A channel receiver from which to fetch samples.
            nones_are_zeros: Whether to treat None values from the stream as 0s.
        """
        self._name = name
        self._stream = stream
        self._next_value: Optional[Sample] = None
        self._nones_are_zeros = nones_are_zeros

    async def fetch_next(self) -> Optional[Sample]:
        """Fetch the next value from the stream.

        To be called before each call to `apply`.

        Returns:
            The fetched Sample.
        """
        self._next_value = await self._stream.receive()
        return self._next_value

    @property
    def value(self) -> Optional[Sample]:
        """Get the next value in the stream.

        Returns:
            Next value in the stream.
        """
        return self._next_value

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return self._name

    def apply(self, eval_stack: List[float]) -> None:
        """Push the latest value from the stream into the evaluation stack.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.

        Raises:
            RuntimeError: No next value available to append.
        """
        if self._next_value is None:
            raise RuntimeError("No next value available to append.")

        next_value = self._next_value.value
        if next_value is None or isnan(next_value) or isinf(next_value):
            if self._nones_are_zeros:
                eval_stack.append(0.0)
            else:
                eval_stack.append(float("NaN"))
        else:
            eval_stack.append(next_value)
