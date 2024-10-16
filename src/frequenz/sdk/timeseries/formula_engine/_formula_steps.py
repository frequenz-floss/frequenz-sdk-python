# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Steps for building formula engines with."""

from __future__ import annotations

import logging
import math
from abc import ABC, abstractmethod
from typing import Any, Generic

from frequenz.channels import Receiver, ReceiverError

from .._base_types import QuantityT, Sample

_logger = logging.getLogger(__name__)


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
    def apply(self, eval_stack: list[float]) -> None:
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

    def apply(self, eval_stack: list[float]) -> None:
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

    def apply(self, eval_stack: list[float]) -> None:
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

    def apply(self, eval_stack: list[float]) -> None:
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

    def apply(self, eval_stack: list[float]) -> None:
        """Extract two values from the stack, divide them, push the result back in.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val2 = eval_stack.pop()
        val1 = eval_stack.pop()
        res = val1 / val2
        eval_stack.append(res)


class Maximizer(FormulaStep):
    """A formula step that represents the max function."""

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "max"

    def apply(self, eval_stack: list[float]) -> None:
        """Extract two values from the stack and pushes back the maximum.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val2 = eval_stack.pop()
        val1 = eval_stack.pop()
        res = max(val1, val2)
        eval_stack.append(res)


class Minimizer(FormulaStep):
    """A formula step that represents the min function."""

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "min"

    def apply(self, eval_stack: list[float]) -> None:
        """Extract two values from the stack and pushes back the minimum.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val2 = eval_stack.pop()
        val1 = eval_stack.pop()
        res = min(val1, val2)
        eval_stack.append(res)


class Consumption(FormulaStep):
    """A formula step that represents the consumption operator.

    The consumption operator is the maximum of the value on top
    of the evaluation stack and 0.
    """

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "consumption"

    def apply(self, eval_stack: list[float]) -> None:
        """
        Apply the consumption formula.

        Replace the top of the eval eval_stack with the same value if the value
        is positive or 0.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val = eval_stack.pop()
        eval_stack.append(max(val, 0))


class Production(FormulaStep):
    """A formula step that represents the production operator.

    The production operator is the maximum of the value times minus one on top
    of the evaluation stack and 0.
    """

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return "production"

    def apply(self, eval_stack: list[float]) -> None:
        """
        Apply the production formula.

        Replace the top of the eval eval_stack with its absolute value if the
        value is negative or 0.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val = eval_stack.pop()
        eval_stack.append(max(-val, 0))


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

    def apply(self, _: list[float]) -> None:
        """No-op."""


class ConstantValue(FormulaStep):
    """A formula step for inserting a constant value."""

    def __init__(self, value: float) -> None:
        """Create a `ConstantValue` instance.

        Args:
            value: The constant value.
        """
        self._value = value

    @property
    def value(self) -> float:
        """Return the constant value.

        Returns:
            The constant value.
        """
        return self._value

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return str(self._value)

    def apply(self, eval_stack: list[float]) -> None:
        """Push the constant value to the eval_stack.

        Args:
            eval_stack: An evaluation stack, to append the constant value to.
        """
        eval_stack.append(self._value)


class Clipper(FormulaStep):
    """A formula step for clipping a value between a minimum and maximum."""

    def __init__(self, min_val: float | None, max_val: float | None) -> None:
        """Create a `Clipper` instance.

        Args:
            min_val: The minimum value.
            max_val: The maximum value.
        """
        self._min_val = min_val
        self._max_val = max_val

    @property
    def min_value(self) -> float | None:
        """Return the minimum value.

        Returns:
            The minimum value.
        """
        return self._min_val

    @property
    def max_value(self) -> float | None:
        """Return the maximum value.

        Returns:
            The maximum value.
        """
        return self._max_val

    def __repr__(self) -> str:
        """Return a string representation of the step.

        Returns:
            A string representation of the step.
        """
        return f"clip({self._min_val}, {self._max_val})"

    def apply(self, eval_stack: list[float]) -> None:
        """Clip the value at the top of the eval_stack.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.
        """
        val = eval_stack.pop()
        if self._min_val is not None:
            val = max(val, self._min_val)
        if self._max_val is not None:
            val = min(val, self._max_val)
        eval_stack.append(val)


class FallbackMetricFetcher(Receiver[Sample[QuantityT]], Generic[QuantityT]):
    """A fallback metric fetcher for formula engines.

    Generates a metric value from the fallback components if the primary metric
    is invalid.

    This class starts running when the primary MetricFetcher starts receiving invalid data.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of the fetcher."""

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """Check whether the metric fetcher is running."""

    @abstractmethod
    def start(self) -> None:
        """Initialize the metric fetcher and start fetching samples."""


class MetricFetcher(Generic[QuantityT], FormulaStep):
    """A formula step for fetching a value from a metric Receiver."""

    def __init__(
        self,
        name: str,
        stream: Receiver[Sample[QuantityT]],
        *,
        nones_are_zeros: bool,
        fallback: FallbackMetricFetcher[QuantityT] | None = None,
    ) -> None:
        """Create a `MetricFetcher` instance.

        Args:
            name: The name of the metric.
            stream: A channel receiver from which to fetch samples.
            nones_are_zeros: Whether to treat None values from the stream as 0s.
            fallback: Metric fetcher to use if primary one start sending
                invalid data (e.g. due to a component stop). If None, the data from
                primary metric fetcher will be used.
        """
        self._name = name
        self._stream: Receiver[Sample[QuantityT]] = stream
        self._next_value: Sample[QuantityT] | None = None
        self._nones_are_zeros = nones_are_zeros
        self._fallback: FallbackMetricFetcher[QuantityT] | None = fallback
        self._latest_fallback_sample: Sample[QuantityT] | None = None

    @property
    def stream(self) -> Receiver[Sample[QuantityT]]:
        """Return the stream from which to fetch values.

        Returns:
            The stream from which to fetch values.
        """
        return self._stream

    def stream_name(self) -> str:
        """Return the name of the stream.

        Returns:
            The name of the stream.
        """
        return str(self._stream.__doc__)

    def _is_value_valid(self, value: QuantityT | None) -> bool:
        return not (value is None or value.isnan() or value.isinf())

    async def _synchronize_and_fetch_fallback(
        self,
        primary_fetcher_sample: Sample[QuantityT],
        fallback_fetcher: FallbackMetricFetcher[QuantityT],
    ) -> Sample[QuantityT] | None:
        """Synchronize the fallback fetcher and return the fallback value.

        Args:
            primary_fetcher_sample: The sample fetched from the primary fetcher.
            fallback_fetcher: The fallback metric fetcher.

        Returns:
            The value from the synchronized stream. Returns None if the primary
            fetcher sample is older than the latest sample from the fallback
            fetcher or if the fallback fetcher fails to fetch the next value.
        """
        # fallback_fetcher was not used, yet. We need to fetch first value.
        if self._latest_fallback_sample is None:
            try:
                self._latest_fallback_sample = await fallback_fetcher.receive()
            except ReceiverError[Any] as err:
                _logger.error(
                    "Fallback metric fetcher %s failed to fetch next value: %s."
                    "Using primary metric fetcher.",
                    fallback_fetcher.name,
                    err,
                )
                return None

        if primary_fetcher_sample.timestamp < self._latest_fallback_sample.timestamp:
            return None

        # Synchronize the fallback fetcher with primary one
        while primary_fetcher_sample.timestamp > self._latest_fallback_sample.timestamp:
            try:
                self._latest_fallback_sample = await fallback_fetcher.receive()
            except ReceiverError[Any] as err:
                _logger.error(
                    "Fallback metric fetcher %s failed to fetch next value: %s."
                    "Using primary metric fetcher.",
                    fallback_fetcher.name,
                    err,
                )
                return None

        return self._latest_fallback_sample

    async def fetch_next_with_fallback(
        self, fallback_fetcher: FallbackMetricFetcher[QuantityT]
    ) -> Sample[QuantityT]:
        """Fetch the next value from the primary and fallback streams.

        Return the value from the stream that returns a valid value.
        If any stream raises an exception, then return the value from
        the other stream.

        Args:
            fallback_fetcher: The fallback metric fetcher.

        Returns:
            The value fetched from either the primary or fallback stream.
        """
        try:
            primary = await self._stream.receive()
        except ReceiverError[Any] as err:
            _logger.error(
                "Primary metric fetcher %s failed to fetch next value: %s."
                "Using fallback metric fetcher.",
                self._name,
                err,
            )
            return await fallback_fetcher.receive()

        fallback = await self._synchronize_and_fetch_fallback(primary, fallback_fetcher)
        if fallback is None:
            return primary

        if self._is_value_valid(primary.value):
            return primary
        return fallback

    async def fetch_next(self) -> Sample[QuantityT] | None:
        """Fetch the next value from the stream.

        To be called before each call to `apply`.

        Returns:
            The fetched Sample.
        """
        self._next_value = await self._fetch_next()
        return self._next_value

    async def _fetch_next(self) -> Sample[QuantityT] | None:
        if self._fallback is None:
            return await self._stream.receive()

        if self._fallback.is_running:
            return await self.fetch_next_with_fallback(self._fallback)

        next_value = None
        try:
            next_value = await self._stream.receive()
        except ReceiverError[Any] as err:
            _logger.error("Failed to fetch next value from %s: %s", self._name, err)
        else:
            if self._is_value_valid(next_value.value):
                return next_value

        _logger.warning(
            "Primary metric %s is invalid. Running fallback metric fetcher: %s",
            self._name,
            self._fallback.name,
        )
        # start fallback formula but don't wait for it because it has to
        # synchronize. Just return invalid value.
        self._fallback.start()
        return next_value

    @property
    def value(self) -> Sample[QuantityT] | None:
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

    def apply(self, eval_stack: list[float]) -> None:
        """Push the latest value from the stream into the evaluation stack.

        Args:
            eval_stack: An evaluation stack, to apply the formula step on.

        Raises:
            RuntimeError: No next value available to append.
        """
        if self._next_value is None:
            raise RuntimeError("No next value available to append.")

        next_value = self._next_value.value
        if next_value is None or next_value.isnan() or next_value.isinf():
            if self._nones_are_zeros:
                eval_stack.append(0.0)
            else:
                eval_stack.append(math.nan)
        else:
            eval_stack.append(next_value.base_value)
