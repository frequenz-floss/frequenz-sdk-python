# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A formula engine that can apply formulas on streaming data."""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID, uuid4

from frequenz.channels import Broadcast, Receiver
from frequenz.channels._broadcast import Receiver as BroadcastReceiver

from ...util.asyncio import cancel_and_await
from .. import Sample
from ._formula_steps import (
    Adder,
    Averager,
    Divider,
    FormulaStep,
    MetricFetcher,
    Multiplier,
    OpenParen,
    Subtractor,
)
from ._tokenizer import TokenType

logger = logging.Logger(__name__)

_operator_precedence = {
    "(": 0,
    "/": 1,
    "*": 2,
    "-": 3,
    "+": 4,
    ")": 5,
}


class FormulaEngine:
    """A post-fix formula engine that operates on `Sample` receivers.

    Use the `FormulaBuilder` to create `FormulaEngine` instances.
    """

    def __init__(
        self,
        name: str,
        steps: List[FormulaStep],
        metric_fetchers: Dict[str, MetricFetcher],
    ) -> None:
        """Create a `FormulaEngine` instance.

        Args:
            name: A name for the formula.
            steps: Steps for the engine to execute, in post-fix order.
            metric_fetchers: Fetchers for each metric stream the formula depends on.
        """
        self._name = name
        self._steps = steps
        self._metric_fetchers = metric_fetchers
        self._first_run = True
        self._channel = FormulaChannel(self._name, self)
        self._task = None

    async def _synchronize_metric_timestamps(
        self, metrics: Set[asyncio.Task[Optional[Sample]]]
    ) -> datetime:
        """Synchronize the metric streams.

        For synchronised streams like data from the `ComponentMetricsResamplingActor`,
        this a call to this function is required only once, before the first set of
        inputs are fetched.

        Args:
            metrics: The finished tasks from the first `fetch_next` calls to all the
                `MetricFetcher`s.

        Returns:
            The timestamp of the latest metric value.

        Raises:
            RuntimeError: when some streams have no value, or when the synchronization
                of timestamps fails.
        """
        metrics_by_ts: Dict[datetime, str] = {}
        for metric in metrics:
            result = metric.result()
            name = metric.get_name()
            if result is None:
                raise RuntimeError(f"Stream closed for component: {name}")
            metrics_by_ts[result.timestamp] = name
        latest_ts = max(metrics_by_ts)

        # fetch the metrics with non-latest timestamps again until we have the values
        # for the same ts for all metrics.
        for metric_ts, name in metrics_by_ts.items():
            if metric_ts == latest_ts:
                continue
            fetcher = self._metric_fetchers[name]
            while metric_ts < latest_ts:
                next_val = await fetcher.fetch_next()
                assert next_val is not None
                metric_ts = next_val.timestamp
            if metric_ts > latest_ts:
                raise RuntimeError(
                    "Unable to synchronize resampled metric timestamps, "
                    f"for formula: {self._name}"
                )
        self._first_run = False
        return latest_ts

    async def _apply(self) -> Sample:
        """Fetch the latest metrics, apply the formula once and return the result.

        Returns:
            The result of the formula.

        Raises:
            RuntimeError: if some samples didn't arrive, or if formula application
                failed.
        """
        eval_stack: List[Optional[float]] = []
        ready_metrics, pending = await asyncio.wait(
            [
                asyncio.create_task(fetcher.fetch_next(), name=name)
                for name, fetcher in self._metric_fetchers.items()
            ],
            return_when=asyncio.ALL_COMPLETED,
        )

        if pending or any(res.result() is None for res in iter(ready_metrics)):
            raise RuntimeError(
                f"Some resampled metrics didn't arrive, for formula: {self._name}"
            )

        if self._first_run:
            metric_ts = await self._synchronize_metric_timestamps(ready_metrics)
        else:
            res = next(iter(ready_metrics)).result()
            assert res is not None
            metric_ts = res.timestamp

        for step in self._steps:
            step.apply(eval_stack)

        # if all steps were applied and the formula was correct, there should only be a
        # single value in the evaluation stack, and that would be the formula result.
        if len(eval_stack) != 1:
            raise RuntimeError(f"Formula application failed: {self._name}")

        return Sample(metric_ts, eval_stack[0])

    async def _run(self) -> None:
        sender = self._channel.new_sender()
        while True:
            try:
                msg = await self._apply()
            except asyncio.CancelledError:
                logger.exception("FormulaEngine task cancelled: %s", self._name)
                break
            except Exception as err:  # pylint: disable=broad-except
                logger.warning(
                    "Formula application failed: %s. Error: %s", self._name, err
                )
            else:
                await sender.send(msg)

    async def _stop(self) -> None:
        """Stop a running formula engine."""
        if self._task is None:
            return
        await cancel_and_await(self._task)

    def new_receiver(
        self, name: Optional[str] = None, max_size: int = 50
    ) -> FormulaReceiver:
        """Create a new receiver that streams the output of the formula engine.

        Args:
            name: An optional name for the receiver.
            max_size: The size of the receiver's buffer.

        Returns:
            A receiver that streams output `Sample`s from the formula engine.
        """
        if self._task is None:
            self._task = asyncio.create_task(self._run())

        return self._channel.new_receiver(name, max_size)


class FormulaBuilder:
    """Builds a post-fix formula engine that operates on `Sample` receivers.

    Operators and metrics need to be pushed in in-fix order, and they get rearranged
    into post-fix order.  This is done using the [Shunting yard
    algorithm](https://en.wikipedia.org/wiki/Shunting_yard_algorithm).

    Example:
        To create an engine that adds the latest entries from two receivers, the
        following calls need to be made:

        ```python
        builder = FormulaBuilder()
        builder.push_metric("metric_1", receiver_1)
        builder.push_oper("+")
        builder.push_metric("metric_2", receiver_2)
        engine = builder.build()
        ```

        and then every call to `engine.apply()` would fetch a value from each receiver,
        add the values and return the result.
    """

    def __init__(self, name: str) -> None:
        """Create a `FormulaBuilder` instance.

        Args:
            name: A name for the formula being built.
        """
        self._name = name
        self._build_stack: List[FormulaStep] = []
        self._steps: List[FormulaStep] = []
        self._metric_fetchers: Dict[str, MetricFetcher] = {}

    def push_oper(self, oper: str) -> None:
        """Push an operator into the engine.

        Args:
            oper: One of these strings - "+", "-", "*", "/", "(", ")"
        """
        if self._build_stack and oper != "(":
            op_prec = _operator_precedence[oper]
            while self._build_stack:
                prev_step = self._build_stack[-1]
                if op_prec < _operator_precedence[repr(prev_step)]:
                    break
                if oper == ")" and repr(prev_step) == "(":
                    self._build_stack.pop()
                    break
                if repr(prev_step) == "(":
                    break
                self._steps.append(prev_step)
                self._build_stack.pop()

        if oper == "+":
            self._build_stack.append(Adder())
        elif oper == "-":
            self._build_stack.append(Subtractor())
        elif oper == "*":
            self._build_stack.append(Multiplier())
        elif oper == "/":
            self._build_stack.append(Divider())
        elif oper == "(":
            self._build_stack.append(OpenParen())

    def push_metric(
        self,
        name: str,
        data_stream: Receiver[Sample],
        nones_are_zeros: bool,
    ) -> None:
        """Push a metric receiver into the engine.

        Args:
            name: A name for the metric.
            data_stream: A receiver to fetch this metric from.
            nones_are_zeros: Whether to treat None values from the stream as 0s.  If
                False, the returned value will be a None.
        """
        fetcher = self._metric_fetchers.setdefault(
            name, MetricFetcher(name, data_stream, nones_are_zeros)
        )
        self._steps.append(fetcher)

    def push_average(self, metrics: List[Tuple[str, Receiver[Sample], bool]]) -> None:
        """Push an average calculator into the engine.

        Args:
            metrics: list of arguments to pass to each `MetricFetcher`.
        """
        fetchers: List[MetricFetcher] = []
        for metric in metrics:
            fetcher = self._metric_fetchers.setdefault(
                metric[0], MetricFetcher(*metric)
            )
            fetchers.append(fetcher)
        self._steps.append(Averager(fetchers))

    def build(self) -> FormulaEngine:
        """Finalize and build the formula engine.

        Returns:
            A `FormulaEngine` instance.
        """
        while self._build_stack:
            self._steps.append(self._build_stack.pop())

        return FormulaEngine(self._name, self._steps, self._metric_fetchers)


class FormulaChannel(Broadcast[Sample]):
    """A broadcast channel implementation for use with formulas."""

    def __init__(
        self, name: str, engine: FormulaEngine, resend_latest: bool = False
    ) -> None:
        """Create a `FormulaChannel` instance.

        Args:
            name: A name for the channel.
            engine: A FormulaEngine instance that produces values for this channel.
            resend_latest: Whether to resend latest channel values to newly created
                receivers, like in `Broadcast` channels.
        """
        self._engine = engine
        super().__init__(name, resend_latest)

    @property
    def engine(self) -> FormulaEngine:
        """Return the formula engine attached to the channel.

        Returns:
            A FormulaEngine instance.
        """
        return self._engine

    def new_receiver(
        self, name: Optional[str] = None, maxsize: int = 50
    ) -> FormulaReceiver:
        """Create a new FormulaReceiver for the channel.

        This implementation is similar to `Broadcast.new_receiver()`, except that it
        creates and returns a `FormulaReceiver`.  The way the default name for the
        receiver is constructed, is also slightly tweaked.

        Args:
            name: An optional name for the receiver.
            maxsize: size of the receiver's buffer.

        Returns:
            A `FormulaReceiver` instance attached to the `FormulaChannel`.
        """
        uuid = uuid4()
        if name is None:
            name = self.name
        recv = FormulaReceiver(uuid, name, maxsize, self)
        self.receivers[uuid] = weakref.ReferenceType(recv)
        if self._resend_latest and self._latest is not None:
            recv.enqueue(self._latest)
        return recv


class FormulaReceiver(BroadcastReceiver[Sample]):
    """A receiver to receive calculated `Sample`s from a Formula channel.

    They function as regular channel receivers, but can be composed to form higher order
    formulas.
    """

    def __init__(
        self,
        uuid: UUID,
        name: str,
        maxsize: int,
        chan: FormulaChannel,
    ) -> None:
        """Create a `FormulaReceiver` instance.

        Args:
            uuid: uuid to uniquely identify the receiver.  Forwarded to
                BroadcastReceiver's `__init__` function.
            name: Name for the receiver.
            maxsize: Buffer size for the receiver.
            chan: The `FormulaChannel` instance that this receiver is attached to.
        """
        self._engine = chan.engine
        super().__init__(uuid, name, maxsize, chan)

    @property
    def name(self) -> str:
        """Name of the receiver.

        Returns:
            Name of the receiver.
        """
        return self._name

    @property
    def engine(self) -> FormulaEngine:
        """Return the formula engine attached to the receiver.

        Returns:
            Formula Engine attached to the receiver.
        """
        return self._engine

    def _deactivate(self) -> None:
        self._active = False

    def clone(self) -> FormulaReceiver:
        """Create a new receiver from the formula engine.

        Returns:
            New `FormulaReceiver` streaming a copy of the formula engine output.
        """
        return self._engine.new_receiver()

    def __add__(
        self, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        """Return a formula builder that adds (data in) `other` to `self`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return HigherOrderFormulaBuilder(self) + other

    def __sub__(
        self, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        """Return a formula builder that subtracts (data in) `other` from `self`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return HigherOrderFormulaBuilder(self) - other

    def __mul__(
        self, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        """Return a formula builder that multiplies (data in) `self` with `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return HigherOrderFormulaBuilder(self) * other

    def __truediv__(
        self, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        """Return a formula builder that divides (data in) `self` by `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return HigherOrderFormulaBuilder(self) / other


class HigherOrderFormulaBuilder:
    """Provides a way to build formulas from the outputs of other formulas."""

    def __init__(self, recv: FormulaReceiver) -> None:
        """Create a `HigherOrderFormulaBuilder` instance.

        Args:
            recv: A first input stream to create a builder with, so that python
                operators `+, -, *, /` can be used directly on newly created instances.
        """
        self._steps: deque[tuple[TokenType, FormulaReceiver | str]] = deque()
        self._steps.append((TokenType.COMPONENT_METRIC, recv.clone()))
        recv._deactivate()  # pylint: disable=protected-access
        self._engine = None

    def _push(
        self, oper: str, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        self._steps.appendleft((TokenType.OPER, "("))
        self._steps.append((TokenType.OPER, ")"))
        self._steps.append((TokenType.OPER, oper))

        # pylint: disable=protected-access
        if isinstance(other, FormulaReceiver):
            self._steps.append((TokenType.COMPONENT_METRIC, other.clone()))
            other._deactivate()
        elif isinstance(other, HigherOrderFormulaBuilder):
            self._steps.append((TokenType.OPER, "("))
            self._steps.extend(other._steps)
            self._steps.append((TokenType.OPER, ")"))
        # pylint: enable=protected-access
        else:
            raise RuntimeError(f"Can't build a formula from: {other}")

        return self

    def __add__(
        self, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        """Return a formula builder that adds (data in) `other` to `self`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("+", other)

    def __sub__(
        self, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        """Return a formula builder that subtracts (data in) `other` from `self`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("-", other)

    def __mul__(
        self, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        """Return a formula builder that multiplies (data in) `self` with `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("*", other)

    def __truediv__(
        self, other: FormulaReceiver | HigherOrderFormulaBuilder
    ) -> HigherOrderFormulaBuilder:
        """Return a formula builder that divides (data in) `self` by `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("/", other)

    def build(self, name: str, nones_are_zeros: bool = False) -> FormulaEngine:
        """Create a formula engine from the builder.

        Args:
            name: A name for the formula being built.
            nones_are_zeros: Whether `None`s in the input streams should be treated as
                zeros.

        Returns:
            A `FormulaEngine` instance.
        """
        if self._engine is not None:
            return self._engine

        builder = FormulaBuilder(name)
        for step in self._steps:
            if step[0] == TokenType.COMPONENT_METRIC:
                assert isinstance(step[1], FormulaReceiver)
                builder.push_metric(step[1].name, step[1], nones_are_zeros)
            elif step[0] == TokenType.OPER:
                assert isinstance(step[1], str)
                builder.push_oper(step[1])
        self._engine = builder.build()

        return self._engine

    def new_receiver(
        self, name: Optional[str] = None, max_size: int = 50
    ) -> FormulaReceiver:
        """Get a new receiver from the corresponding engine.

        Args:
            name: optional name for the receiver.
            max_size: size of the receiver's buffer.

        Returns:
            A FormulaReceiver that streams formula output `Sample`s.

        Raises:
            RuntimeError: If `build` hasn't been called yet.
        """
        if self._engine is None:
            raise RuntimeError(
                "Please call `build()` first, before calls to `new_receiver()`"
            )
        return self._engine.new_receiver(name, max_size)
