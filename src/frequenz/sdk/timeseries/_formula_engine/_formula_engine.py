# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A formula engine that can apply formulas on streaming data."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC
from collections import deque
from datetime import datetime
from math import isinf, isnan
from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from frequenz.channels import Broadcast, Receiver

from ..._internal._asyncio import cancel_and_await
from .. import Sample, Sample3Phase
from .._quantities import QuantityT
from ._formula_steps import (
    Adder,
    Averager,
    Clipper,
    ConstantValue,
    Divider,
    FormulaStep,
    MetricFetcher,
    Multiplier,
    OpenParen,
    Subtractor,
)
from ._tokenizer import TokenType

_logger = logging.Logger(__name__)

_operator_precedence = {
    "(": 0,
    "/": 1,
    "*": 2,
    "-": 3,
    "+": 4,
    ")": 5,
}


class FormulaEvaluator(Generic[QuantityT]):
    """A post-fix formula evaluator that operates on `Sample` receivers."""

    def __init__(
        self,
        name: str,
        steps: List[FormulaStep],
        metric_fetchers: Dict[str, MetricFetcher[QuantityT]],
        create_method: Callable[[float], QuantityT],
    ) -> None:
        """Create a `FormulaEngine` instance.

        Args:
            name: A name for the formula.
            steps: Steps for the engine to execute, in post-fix order.
            metric_fetchers: Fetchers for each metric stream the formula depends on.
            create_method: A method to generate the output `Sample` value with.  If the
                formula is for generating power values, this would be
                `Power.from_watts`, for example.
        """
        self._name = name
        self._steps = steps
        self._metric_fetchers: Dict[str, MetricFetcher[QuantityT]] = metric_fetchers
        self._first_run = True
        self._create_method: Callable[[float], QuantityT] = create_method

    async def _synchronize_metric_timestamps(
        self, metrics: Set[asyncio.Task[Optional[Sample[QuantityT]]]]
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
        metrics_by_ts: Dict[datetime, list[str]] = {}
        for metric in metrics:
            result = metric.result()
            name = metric.get_name()
            if result is None:
                raise RuntimeError(f"Stream closed for component: {name}")
            metrics_by_ts.setdefault(result.timestamp, []).append(name)
        latest_ts = max(metrics_by_ts)

        # fetch the metrics with non-latest timestamps again until we have the values
        # for the same ts for all metrics.
        for metric_ts, names in metrics_by_ts.items():
            if metric_ts == latest_ts:
                continue
            while metric_ts < latest_ts:
                for name in names:
                    fetcher = self._metric_fetchers[name]
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

    async def apply(self) -> Sample[QuantityT]:
        """Fetch the latest metrics, apply the formula once and return the result.

        Returns:
            The result of the formula.

        Raises:
            RuntimeError: if some samples didn't arrive, or if formula application
                failed.
        """
        eval_stack: List[float] = []
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
            sample = next(iter(ready_metrics)).result()
            assert sample is not None
            metric_ts = sample.timestamp

        for step in self._steps:
            step.apply(eval_stack)

        # if all steps were applied and the formula was correct, there should only be a
        # single value in the evaluation stack, and that would be the formula result.
        if len(eval_stack) != 1:
            raise RuntimeError(f"Formula application failed: {self._name}")

        res = eval_stack.pop()
        if isnan(res) or isinf(res):
            return Sample(metric_ts, None)

        return Sample(metric_ts, self._create_method(res))


_CompositionType = Union[
    "FormulaEngine",
    "HigherOrderFormulaBuilder",
    "FormulaEngine3Phase",
    "HigherOrderFormulaBuilder3Phase",
]

_CompositionType1Phase = Union[
    "FormulaEngine",
    "HigherOrderFormulaBuilder",
]

_CompositionType3Phase = Union[
    "FormulaEngine3Phase",
    "HigherOrderFormulaBuilder3Phase",
]

# The `FormulaEngine*` and `HigherOrderFormulaBuilder*` classes are generic, but
# `TypeVar`s can't be defined on generic types, so we need to use `# type: ignore` to
# avoid mypy errors, and they get treated as `FormulaEngine[Any]`, etc.
#
# This is not ideal, but it's the best we can do until mypy supports generic types with
# `TypeVar`s.
_GenericEngine = TypeVar(
    "_GenericEngine",
    "FormulaEngine",  # type: ignore
    "FormulaEngine3Phase",  # type: ignore
)
_GenericHigherOrderBuilder = TypeVar(
    "_GenericHigherOrderBuilder",
    "HigherOrderFormulaBuilder",  # type: ignore
    "HigherOrderFormulaBuilder3Phase",  # type: ignore
)


# This class has multiple generic type parameters that should also restrict each other,
# but mypy doesn't support that, so we need to use `# type: ignore` in several places in
# this, and subsequent classes, to avoid mypy errors.
class _ComposableFormulaEngine(
    ABC, Generic[_GenericEngine, _GenericHigherOrderBuilder, QuantityT]
):
    """A base class for formula engines."""

    _create_method: Callable[[float], QuantityT]
    _higher_order_builder: Type[_GenericHigherOrderBuilder]
    _task: asyncio.Task[None] | None = None

    async def _stop(self) -> None:
        """Stop a running formula engine."""
        if self._task is None:
            return
        await cancel_and_await(self._task)

    def __add__(
        self,
        other: _GenericEngine | _GenericHigherOrderBuilder,
    ) -> _GenericHigherOrderBuilder:
        """Return a formula builder that adds (data in) `other` to `self`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._higher_order_builder(self, self._create_method) + other  # type: ignore

    def __sub__(
        self, other: _GenericEngine | _GenericHigherOrderBuilder
    ) -> _GenericHigherOrderBuilder:
        """Return a formula builder that subtracts (data in) `other` from `self`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._higher_order_builder(self, self._create_method) - other  # type: ignore

    def __mul__(
        self, other: _GenericEngine | _GenericHigherOrderBuilder
    ) -> _GenericHigherOrderBuilder:
        """Return a formula builder that multiplies (data in) `self` with `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._higher_order_builder(self, self._create_method) * other  # type: ignore

    def __truediv__(
        self, other: _GenericEngine | _GenericHigherOrderBuilder
    ) -> _GenericHigherOrderBuilder:
        """Return a formula builder that divides (data in) `self` by `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._higher_order_builder(self, self._create_method) / other  # type: ignore


class FormulaEngine(
    Generic[QuantityT],
    _ComposableFormulaEngine[
        "FormulaEngine",  # type: ignore[type-arg]
        "HigherOrderFormulaBuilder",  # type: ignore[type-arg]
        QuantityT,
    ],
):
    """
    The FormulaEngine evaluates formulas and streams the results.

    Use the `FormulaBuilder` to create `FormulaEngine` instances.
    """

    def __init__(
        self,
        builder: FormulaBuilder[QuantityT],
        create_method: Callable[[float], QuantityT],
    ) -> None:
        """Create a `FormulaEngine` instance.

        Args:
            builder: A `FormulaBuilder` instance to get the formula steps and metric
                fetchers from.
            create_method: A method to generate the output `Sample` value with.  If the
                formula is for generating power values, this would be
                `Power.from_watts`, for example.
        """
        self._higher_order_builder = HigherOrderFormulaBuilder
        self._name: str = builder.name
        self._builder: FormulaBuilder[QuantityT] = builder
        self._create_method = create_method
        self._channel: Broadcast[Sample[QuantityT]] = Broadcast(self._name)

    async def _run(self) -> None:
        await self._builder.subscribe()
        steps, metric_fetchers = self._builder.finalize()
        evaluator = FormulaEvaluator[QuantityT](
            self._name, steps, metric_fetchers, self._create_method
        )
        sender = self._channel.new_sender()
        while True:
            try:
                msg = await evaluator.apply()
            except asyncio.CancelledError:
                _logger.exception("FormulaEngine task cancelled: %s", self._name)
                raise
            except Exception as err:  # pylint: disable=broad-except
                _logger.warning(
                    "Formula application failed: %s. Error: %s", self._name, err
                )
            else:
                await sender.send(msg)

    def new_receiver(
        self, name: Optional[str] = None, max_size: int = 50
    ) -> Receiver[Sample[QuantityT]]:
        """Create a new receiver that streams the output of the formula engine.

        Args:
            name: An optional name for the receiver.
            max_size: The size of the receiver's buffer.

        Returns:
            A receiver that streams output `Sample`s from the formula engine.
        """
        if self._task is None:
            self._task = asyncio.create_task(self._run())

        recv = self._channel.new_receiver(name, max_size)

        # This is a hack to ensure that the lifetime of the engine is tied to the
        # lifetime of the receiver.  This is necessary because the engine is a task that
        # runs forever, and in cases where higher order built for example with the below
        # idiom, the user would hold no references to the engine and it could get
        # garbage collected before the receiver.  This behaviour is explained in the
        # `asyncio.create_task` docs here:
        # https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
        #
        #     formula = (grid_power_engine + bat_power_engine).build().new_receiver()
        recv._engine_reference = self  # type: ignore # pylint: disable=protected-access
        return recv


class FormulaEngine3Phase(
    _ComposableFormulaEngine[
        "FormulaEngine3Phase",  # type: ignore[type-arg]
        "HigherOrderFormulaBuilder3Phase",  # type: ignore[type-arg]
        QuantityT,
    ]
):
    """
    The FormulaEngine evaluates formulas and streams the results.

    Use the `FormulaBuilder` to create `FormulaEngine` instances.
    """

    def __init__(
        self,
        name: str,
        create_method: Callable[[float], QuantityT],
        phase_streams: Tuple[
            FormulaEngine[QuantityT],
            FormulaEngine[QuantityT],
            FormulaEngine[QuantityT],
        ],
    ) -> None:
        """Create a `FormulaEngine3Phase` instance.

        Args:
            name: A name for the formula.
            create_method: A method to generate the output `Sample` value with.  If the
                formula is for generating power values, this would be
                `Power.from_watts`, for example.
            phase_streams: output streams of formula engines running per-phase formulas.
        """
        self._higher_order_builder = HigherOrderFormulaBuilder3Phase
        self._name: str = name
        self._create_method = create_method
        self._channel: Broadcast[Sample3Phase[QuantityT]] = Broadcast(self._name)
        self._task: asyncio.Task[None] | None = None
        self._streams: tuple[
            FormulaEngine[QuantityT],
            FormulaEngine[QuantityT],
            FormulaEngine[QuantityT],
        ] = phase_streams

    async def _run(self) -> None:
        sender = self._channel.new_sender()
        phase_1_rx = self._streams[0].new_receiver()
        phase_2_rx = self._streams[1].new_receiver()
        phase_3_rx = self._streams[2].new_receiver()

        while True:
            try:
                phase_1 = await phase_1_rx.receive()
                phase_2 = await phase_2_rx.receive()
                phase_3 = await phase_3_rx.receive()
                msg = Sample3Phase(
                    phase_1.timestamp,
                    phase_1.value,
                    phase_2.value,
                    phase_3.value,
                )
            except asyncio.CancelledError:
                _logger.exception("FormulaEngine task cancelled: %s", self._name)
                break
            else:
                await sender.send(msg)

    def new_receiver(
        self, name: Optional[str] = None, max_size: int = 50
    ) -> Receiver[Sample3Phase[QuantityT]]:
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


class FormulaBuilder(Generic[QuantityT]):
    """Builds a post-fix formula engine that operates on `Sample` receivers.

    Operators and metrics need to be pushed in in-fix order, and they get rearranged
    into post-fix order.  This is done using the [Shunting yard
    algorithm](https://en.wikipedia.org/wiki/Shunting_yard_algorithm).

    Example:
        To create an engine that adds the latest entries from two receivers, the
        following calls need to be made:

        ```python
        from frequenz.sdk.timeseries import Power

        channel = Broadcast[Sample[Power]]("channel")
        receiver_1 = channel.new_receiver("receiver_1")
        receiver_2 = channel.new_receiver("receiver_2")
        builder = FormulaBuilder("addition", Power)
        builder.push_metric("metric_1", receiver_1, nones_are_zeros=True)
        builder.push_oper("+")
        builder.push_metric("metric_2", receiver_2, nones_are_zeros=True)
        engine = builder.build()
        ```

        and then every call to `engine.apply()` would fetch a value from each receiver,
        add the values and return the result.
    """

    def __init__(self, name: str, create_method: Callable[[float], QuantityT]) -> None:
        """Create a `FormulaBuilder` instance.

        Args:
            name: A name for the formula being built.
            create_method: A method to generate the output `Sample` value with.  If the
                formula is for generating power values, this would be
                `Power.from_watts`, for example.
        """
        self._name = name
        self._create_method: Callable[[float], QuantityT] = create_method
        self._build_stack: List[FormulaStep] = []
        self._steps: List[FormulaStep] = []
        self._metric_fetchers: Dict[str, MetricFetcher[QuantityT]] = {}

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
        data_stream: Receiver[Sample[QuantityT]],
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

    def push_constant(self, value: float) -> None:
        """Push a constant value into the engine.

        Args:
            value: The constant value to push.
        """
        self._steps.append(ConstantValue(value))

    def push_clipper(self, min_value: float | None, max_value: float | None) -> None:
        """Push a clipper step into the engine.

        The clip will be applied on the last value available on the evaluation stack,
        before the clip step is called.

        So if an entire expression needs to be clipped, the expression should be
        enclosed in parentheses, before the clip step is added.

        For example, this clips the output of the entire expression:

        ```python
        from frequenz.sdk.timeseries import Power

        builder = FormulaBuilder("example", Power)
        channel = Broadcast[Sample[Power]]("channel")
        receiver_1 = channel.new_receiver("receiver_1")
        receiver_2 = channel.new_receiver("receiver_2")

        builder.push_oper("(")
        builder.push_metric("metric_1", receiver_1, nones_are_zeros=True)
        builder.push_oper("+")
        builder.push_metric("metric_2", receiver_2, nones_are_zeros=True)
        builder.push_oper(")")
        builder.push_clipper(min_value=0.0, max_value=None)
        ```

        And this clips the output of metric_2 only, and not the final result:

        ```python
        from frequenz.sdk.timeseries import Power

        builder = FormulaBuilder("example", Power)
        channel = Broadcast[Sample[Power]]("channel")
        receiver_1 = channel.new_receiver("receiver_1")
        receiver_2 = channel.new_receiver("receiver_2")

        builder.push_metric("metric_1", receiver_1, nones_are_zeros=True)
        builder.push_oper("+")
        builder.push_metric("metric_2", receiver_2, nones_are_zeros=True)
        builder.push_clipper(min_value=0.0, max_value=None)
        ```

        Args:
            min_value: The minimum value to clip to.
            max_value: The maximum value to clip to.
        """
        self._steps.append(Clipper(min_value, max_value))

    def push_average(
        self, metrics: List[Tuple[str, Receiver[Sample[QuantityT]], bool]]
    ) -> None:
        """Push an average calculator into the engine.

        Args:
            metrics: list of arguments to pass to each `MetricFetcher`.
        """
        fetchers: List[MetricFetcher[QuantityT]] = []
        for metric in metrics:
            fetcher = self._metric_fetchers.setdefault(
                metric[0], MetricFetcher(*metric)
            )
            fetchers.append(fetcher)
        self._steps.append(Averager(fetchers))

    @property
    def name(self) -> str:
        """Return the name of the formula being built.

        Returns:
            The name of the formula being built.
        """
        return self._name

    async def subscribe(self) -> None:
        """Subscribe to metrics if needed.

        This is a no-op for the `FormulaBuilder` class, but is used by the
        `ResampledFormulaBuilder` class.
        """

    def finalize(
        self,
    ) -> tuple[list[FormulaStep], dict[str, MetricFetcher[QuantityT]]]:
        """Finalize and return the steps and fetchers for the formula.

        Returns:
            A tuple of the steps and fetchers for the formula.
        """
        while self._build_stack:
            self._steps.append(self._build_stack.pop())

        return self._steps, self._metric_fetchers

    def build(self) -> FormulaEngine[QuantityT]:
        """Create a formula engine with the steps and fetchers that have been pushed.

        Returns:
            A `FormulaEngine` instance.
        """
        self.finalize()
        return FormulaEngine(self, create_method=self._create_method)


class _BaseHOFormulaBuilder(ABC, Generic[QuantityT]):
    """Provides a way to build formulas from the outputs of other formulas."""

    def __init__(
        self,
        engine: FormulaEngine[QuantityT] | FormulaEngine3Phase[QuantityT],
        create_method: Callable[[float], QuantityT],
    ) -> None:
        """Create a `GenericHigherOrderFormulaBuilder` instance.

        Args:
            engine: A first input stream to create a builder with, so that python
                operators `+, -, *, /` can be used directly on newly created instances.
            create_method: A method to generate the output `Sample` value with.  If the
                formula is for generating power values, this would be
                `Power.from_watts`, for example.
        """
        self._steps: deque[
            tuple[
                TokenType,
                FormulaEngine[QuantityT] | FormulaEngine3Phase[QuantityT] | str,
            ]
        ] = deque()
        self._steps.append((TokenType.COMPONENT_METRIC, engine))
        self._create_method: Callable[[float], QuantityT] = create_method

    @overload
    def _push(
        self, oper: str, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[QuantityT]:
        ...

    @overload
    def _push(
        self, oper: str, other: _CompositionType3Phase
    ) -> HigherOrderFormulaBuilder3Phase[QuantityT]:
        ...

    def _push(
        self, oper: str, other: _CompositionType
    ) -> (
        HigherOrderFormulaBuilder[QuantityT]
        | HigherOrderFormulaBuilder3Phase[QuantityT]
    ):
        self._steps.appendleft((TokenType.OPER, "("))
        self._steps.append((TokenType.OPER, ")"))
        self._steps.append((TokenType.OPER, oper))

        # pylint: disable=protected-access
        if isinstance(other, (FormulaEngine, FormulaEngine3Phase)):
            self._steps.append((TokenType.COMPONENT_METRIC, other))
        elif isinstance(other, _BaseHOFormulaBuilder):
            self._steps.append((TokenType.OPER, "("))
            self._steps.extend(other._steps)
            self._steps.append((TokenType.OPER, ")"))
        # pylint: enable=protected-access
        else:
            raise RuntimeError(f"Can't build a formula from: {other}")
        assert isinstance(
            self, (HigherOrderFormulaBuilder, HigherOrderFormulaBuilder3Phase)
        )
        return self

    @overload
    def __add__(
        self, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[QuantityT]:
        ...

    @overload
    def __add__(
        self, other: _CompositionType3Phase
    ) -> HigherOrderFormulaBuilder3Phase[QuantityT]:
        ...

    def __add__(
        self, other: _CompositionType
    ) -> (
        HigherOrderFormulaBuilder[QuantityT]
        | HigherOrderFormulaBuilder3Phase[QuantityT]
    ):
        """Return a formula builder that adds (data in) `other` to `self`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("+", other)

    @overload
    def __sub__(
        self, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[QuantityT]:
        ...

    @overload
    def __sub__(
        self, other: _CompositionType3Phase
    ) -> HigherOrderFormulaBuilder3Phase[QuantityT]:
        ...

    def __sub__(
        self,
        other: _CompositionType,
    ) -> (
        HigherOrderFormulaBuilder[QuantityT]
        | HigherOrderFormulaBuilder3Phase[QuantityT]
    ):
        """Return a formula builder that subtracts (data in) `other` from `self`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("-", other)

    @overload
    def __mul__(
        self, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[QuantityT]:
        ...

    @overload
    def __mul__(
        self, other: _CompositionType3Phase
    ) -> HigherOrderFormulaBuilder3Phase[QuantityT]:
        ...

    def __mul__(
        self,
        other: _CompositionType,
    ) -> (
        HigherOrderFormulaBuilder[QuantityT]
        | HigherOrderFormulaBuilder3Phase[QuantityT]
    ):
        """Return a formula builder that multiplies (data in) `self` with `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("*", other)

    @overload
    def __truediv__(
        self, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[QuantityT]:
        ...

    @overload
    def __truediv__(
        self, other: _CompositionType3Phase
    ) -> HigherOrderFormulaBuilder3Phase[QuantityT]:
        ...

    def __truediv__(
        self,
        other: _CompositionType,
    ) -> (
        HigherOrderFormulaBuilder[QuantityT]
        | HigherOrderFormulaBuilder3Phase[QuantityT]
    ):
        """Return a formula builder that divides (data in) `self` by `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("/", other)


class HigherOrderFormulaBuilder(Generic[QuantityT], _BaseHOFormulaBuilder[QuantityT]):
    """A specialization of the _BaseHOFormulaBuilder for `FormulaReceiver`."""

    def build(
        self, name: str, nones_are_zeros: bool = False
    ) -> FormulaEngine[QuantityT]:
        """Build a `FormulaEngine` instance from the builder.

        Args:
            name: A name for the newly generated formula.
            nones_are_zeros: whether `None` values in any of the input streams should be
                treated as zeros.

        Returns:
            A `FormulaEngine` instance.
        """
        builder = FormulaBuilder(name, self._create_method)
        for typ, value in self._steps:
            if typ == TokenType.COMPONENT_METRIC:
                assert isinstance(value, FormulaEngine)
                builder.push_metric(
                    value._name,  # pylint: disable=protected-access
                    value.new_receiver(),
                    nones_are_zeros,
                )
            elif typ == TokenType.OPER:
                assert isinstance(value, str)
                builder.push_oper(value)
        return builder.build()


class HigherOrderFormulaBuilder3Phase(
    Generic[QuantityT], _BaseHOFormulaBuilder[QuantityT]
):
    """A specialization of the _BaseHOFormulaBuilder for `FormulaReceiver3Phase`."""

    def build(
        self, name: str, nones_are_zeros: bool = False
    ) -> FormulaEngine3Phase[QuantityT]:
        """Build a `FormulaEngine3Phase` instance from the builder.

        Args:
            name: A name for the newly generated formula.
            nones_are_zeros: whether `None` values in any of the input streams should be
                treated as zeros.

        Returns:
            A `FormulaEngine3Phase` instance.
        """
        builders = [
            FormulaBuilder(name, self._create_method),
            FormulaBuilder(name, self._create_method),
            FormulaBuilder(name, self._create_method),
        ]
        for typ, value in self._steps:
            if typ == TokenType.COMPONENT_METRIC:
                assert isinstance(value, FormulaEngine3Phase)
                for phase in range(3):
                    builders[phase].push_metric(
                        f"{value._name}-{phase+1}",  # pylint: disable=protected-access
                        value._streams[  # pylint: disable=protected-access
                            phase
                        ].new_receiver(),
                        nones_are_zeros,
                    )
            elif typ == TokenType.OPER:
                assert isinstance(value, str)
                for phase in range(3):
                    builders[phase].push_oper(value)
        return FormulaEngine3Phase(
            name,
            self._create_method,
            (
                builders[0].build(),
                builders[1].build(),
                builders[2].build(),
            ),
        )
