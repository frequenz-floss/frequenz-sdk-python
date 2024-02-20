# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

# pylint: disable=too-many-lines

"""A formula engine that can apply formulas on streaming data."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC
from collections import deque
from collections.abc import Callable
from typing import Generic, SupportsFloat, TypeVar, Union, overload

from frequenz.channels import Broadcast, Receiver

from ..._internal._asyncio import cancel_and_await
from .._base_types import Sample, Sample3Phase
from .._quantities import Quantity, SupportsFloatT
from ._formula_evaluator import FormulaEvaluator
from ._formula_formatter import format_formula
from ._formula_steps import (
    Adder,
    Clipper,
    ConstantValue,
    Consumption,
    Divider,
    FormulaStep,
    Maximizer,
    MetricFetcher,
    Minimizer,
    Multiplier,
    OpenParen,
    Production,
    Subtractor,
)
from ._tokenizer import TokenType

_logger = logging.Logger(__name__)

SupportsFloatInputT = TypeVar("SupportsFloatInputT", bound=SupportsFloat)
"""Type variable for inputs that support conversion to float."""

SupportsFloatOutputT = TypeVar("SupportsFloatOutputT", bound=SupportsFloat)
"""Type variable for outputs that support conversion to float."""

_operator_precedence = {
    "max": 0,
    "min": 1,
    "consumption": 2,
    "production": 3,
    "(": 4,
    "/": 5,
    "*": 6,
    "-": 7,
    "+": 8,
    ")": 9,
}
"""The dictionary of operator precedence for the shunting yard algorithm."""


# The `FormulaEngine*` and `HigherOrderFormulaBuilder*` classes are generic, but
# `TypeVar`s can't be defined on generic types, so we need to use `# type: ignore` to
# avoid mypy errors, and they get treated as `FormulaEngine[Any]`, etc.
#
# This is not ideal, but it's the best we can do until mypy supports generic types with
# `TypeVar`s.
_CompositionType = Union[
    "FormulaEngine",  # type: ignore[type-arg]
    "HigherOrderFormulaBuilder",  # type: ignore[type-arg]
    "FormulaEngine3Phase",  # type: ignore[type-arg]
    "HigherOrderFormulaBuilder3Phase",  # type: ignore[type-arg]
]

_CompositionType1Phase = Union[
    "FormulaEngine",  # type: ignore[type-arg]
    "HigherOrderFormulaBuilder",  # type: ignore[type-arg]
]

_CompositionType3Phase = Union[
    "FormulaEngine3Phase",  # type: ignore[type-arg]
    "HigherOrderFormulaBuilder3Phase",  # type: ignore[type-arg]
]

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
    ABC,
    Generic[
        _GenericEngine,
        _GenericHigherOrderBuilder,
        SupportsFloatInputT,
        SupportsFloatOutputT,
    ],
):
    """A base class for formula engines."""

    _create_method: Callable[[float], SupportsFloatOutputT]
    _higher_order_builder: type[_GenericHigherOrderBuilder]
    _task: asyncio.Task[None] | None = None

    async def _stop(self) -> None:
        """Stop a running formula engine."""
        if self._task is None:
            return
        await cancel_and_await(self._task)

    def __add__(
        self, other: _GenericEngine | _GenericHigherOrderBuilder | SupportsFloatInputT
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
        self, other: _GenericEngine | _GenericHigherOrderBuilder | SupportsFloatInputT
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
        self, other: _GenericEngine | _GenericHigherOrderBuilder | float
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
        self, other: _GenericEngine | _GenericHigherOrderBuilder | float
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

    def max(
        self, other: _GenericEngine | _GenericHigherOrderBuilder | SupportsFloatInputT
    ) -> _GenericHigherOrderBuilder:
        """Return a formula engine that outputs the maximum of `self` and `other`.

        Args:
            other: A formula receiver, a formula builder or a SupportsFloatT instance
                corresponding to a sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._higher_order_builder(self, self._create_method).max(other)  # type: ignore

    def min(
        self, other: _GenericEngine | _GenericHigherOrderBuilder | SupportsFloatInputT
    ) -> _GenericHigherOrderBuilder:
        """Return a formula engine that outputs the minimum of `self` and `other`.

        Args:
            other: A formula receiver, a formula builder or a SupportsFloatT instance
                corresponding to a sub-expression.


        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._higher_order_builder(self, self._create_method).min(other)  # type: ignore

    def consumption(self) -> _GenericHigherOrderBuilder:
        """
        Return a formula builder that applies the consumption operator on `self`.

        The consumption operator returns either the identity if the power value is
        positive or 0.
        """
        return self._higher_order_builder(self, self._create_method).consumption()  # type: ignore

    def production(self) -> _GenericHigherOrderBuilder:
        """
        Return a formula builder that applies the production operator on `self`.

        The production operator returns either the absolute value if the power value is
        negative or 0.
        """
        return self._higher_order_builder(self, self._create_method).production()  # type: ignore


class FormulaEngine(
    Generic[SupportsFloatInputT, SupportsFloatOutputT],
    _ComposableFormulaEngine[
        "FormulaEngine",  # type: ignore[type-arg]
        "HigherOrderFormulaBuilder",  # type: ignore[type-arg]
        SupportsFloatOutputT,
    ],
):
    """[`FormulaEngine`][frequenz.sdk.timeseries.formula_engine.FormulaEngine]s are a
    part of the SDK's data pipeline, and provide a way for the SDK to apply formulas on
    resampled data streams.

    They are used in the SDK to calculate and stream metrics like
    [`grid_power`][frequenz.sdk.timeseries.grid.Grid.power],
    [`consumer_power`][frequenz.sdk.timeseries.consumer.Consumer.power],
    etc., which are building blocks of the
    [Frequenz SDK Microgrid Model][frequenz.sdk.microgrid--frequenz-sdk-microgrid-model].

    The SDK creates the formulas by analysing the configuration of components in the
    {{glossary("Component Graph")}}.

    ### Streaming Interface

    The
    [`FormulaEngine.new_receiver()`][frequenz.sdk.timeseries.formula_engine.FormulaEngine.new_receiver]
    method can be used to create a
    [Receiver](https://frequenz-floss.github.io/frequenz-channels-python/latest/reference/frequenz/channels/#frequenz.channels.Receiver)
    that streams the [Sample][frequenz.sdk.timeseries.Sample]s calculated by the formula
    engine.

    ```python
    from frequenz.sdk import microgrid

    battery_pool = microgrid.battery_pool()

    async for power in battery_pool.power.new_receiver():
        print(f"{power=}")
    ```

    ### Composition

    Composite `FormulaEngine`s can be built using arithmetic operations on
    `FormulaEngine`s streaming the same type of data.

    For example, if you're interested in a particular composite metric that can be
    calculated by subtracting
    [`battery_pool().power`][frequenz.sdk.timeseries.battery_pool.BatteryPool.power] and
    [`ev_charger_pool().power`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool]
    from the
    [`grid().power`][frequenz.sdk.timeseries.grid.Grid.power],
    we can build a `FormulaEngine` that provides a stream of this calculated metric as
    follows:

    ```python
    from frequenz.sdk import microgrid

    logical_meter = microgrid.logical_meter()
    battery_pool = microgrid.battery_pool()
    ev_charger_pool = microgrid.ev_charger_pool()
    grid = microgrid.grid()

    # apply operations on formula engines to create a formula engine that would
    # apply these operations on the corresponding data streams.
    net_power = (
        grid.power - (battery_pool.power + ev_charger_pool.power)
    ).build("net_power")

    async for power in net_power.new_receiver():
        print(f"{power=}")
    ```
    """  # noqa: D400, D205

    def __init__(
        self,
        builder: FormulaBuilder[SupportsFloatInputT, SupportsFloatOutputT],
        create_method: Callable[[float], SupportsFloatOutputT],
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
        self._builder: FormulaBuilder[SupportsFloatInputT, SupportsFloatOutputT] = (
            builder
        )
        self._create_method = create_method
        self._channel: Broadcast[Sample[SupportsFloatInputT]] = Broadcast(self._name)

    @classmethod
    def from_receiver(
        cls,
        name: str,
        receiver: Receiver[Sample[SupportsFloatInputT]],
        create_method: Callable[[float], SupportsFloatOutputT],
        *,
        nones_are_zeros: bool = False,
    ) -> FormulaEngine[SupportsFloatInputT, SupportsFloatOutputT]:
        """
        Create a formula engine from a receiver.

        Can be used to compose a formula engine with a receiver. When composing
        the new engine with other engines, make sure that receiver gets data
        from the same resampler and that the `create_method`s match.

        Example:
            ```python
            from frequenz.sdk import microgrid
            from frequenz.sdk.timeseries import Power

            async def run() -> None:
                producer_power_engine = microgrid.producer().power
                consumer_power_recv = microgrid.consumer().power.new_receiver()

                excess_power_recv = (
                    (
                        producer_power_engine
                        + FormulaEngine.from_receiver(
                            "consumer power",
                            consumer_power_recv,
                            Power.from_watts,
                        )
                    )
                    .build("excess power")
                    .new_receiver()
                )

            asyncio.run(run())
            ```

        Args:
            name: A name for the formula engine.
            receiver: A receiver that streams `Sample`s.
            create_method: A method to generate the output `Sample` value with,
                e.g. `Power.from_watts`.
            nones_are_zeros: If `True`, `None` values in the receiver are treated as 0.

        Returns:
            A formula engine that streams the `Sample`s from the receiver.
        """
        builder = FormulaBuilder(name, create_method)
        builder.push_metric(name, receiver, nones_are_zeros=nones_are_zeros)
        return cls(builder, create_method)

    async def _run(self) -> None:
        await self._builder.subscribe()
        steps, metric_fetchers = self._builder.finalize()
        evaluator = FormulaEvaluator[SupportsFloatInputT, SupportsFloatOutputT](
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

    def __str__(self) -> str:
        """Return a string representation of the formula.

        Returns:
            A string representation of the formula.
        """
        steps = (
            self._builder._build_stack
            if len(self._builder._build_stack) > 0
            else self._builder._steps
        )
        return format_formula(steps)

    def new_receiver(
        self, name: str | None = None, max_size: int = 50
    ) -> Receiver[Sample[SupportsFloatT]]:
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
        SupportsFloatT,
    ]
):
    """A
    [`FormulaEngine3Phase`][frequenz.sdk.timeseries.formula_engine.FormulaEngine3Phase]
    is similar to a
    [`FormulaEngine`][frequenz.sdk.timeseries.formula_engine.FormulaEngine], except that
    they stream [3-phase samples][frequenz.sdk.timeseries.Sample3Phase].  All the
    current formulas (like
    [`Grid.current`][frequenz.sdk.timeseries.grid.Grid.current],
    [`EVChargerPool.current`][frequenz.sdk.timeseries.ev_charger_pool.EVChargerPool.current],
    etc.) are implemented as 3-phase formulas.

    ### Streaming Interface

    The
    [`FormulaEngine3Phase.new_receiver()`][frequenz.sdk.timeseries.formula_engine.FormulaEngine3Phase.new_receiver]
    method can be used to create a
    [Receiver](https://frequenz-floss.github.io/frequenz-channels-python/latest/reference/frequenz/channels/#frequenz.channels.Receiver)
    that streams the [Sample3Phase][frequenz.sdk.timeseries.Sample3Phase] values
    calculated by the formula engine.

    ```python
    from frequenz.sdk import microgrid

    ev_charger_pool = microgrid.ev_charger_pool()

    async for sample in ev_charger_pool.current.new_receiver():
        print(f"Current: {sample}")
    ```

    ### Composition

    `FormulaEngine3Phase` instances can be composed together, just like `FormulaEngine`
    instances.

    ```python
    from frequenz.sdk import microgrid

    logical_meter = microgrid.logical_meter()
    ev_charger_pool = microgrid.ev_charger_pool()
    grid = microgrid.grid()

    # Calculate grid consumption current that's not used by the EV chargers
    other_current = (grid.current - ev_charger_pool.current).build("other_current")

    async for sample in other_current.new_receiver():
        print(f"Other current: {sample}")
    ```
    """  # noqa: D205, D400

    def __init__(
        self,
        name: str,
        create_method: Callable[[float], SupportsFloatT],
        phase_streams: tuple[
            FormulaEngine[SupportsFloatT],
            FormulaEngine[SupportsFloatT],
            FormulaEngine[SupportsFloatT],
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
        self._channel: Broadcast[Sample3Phase[SupportsFloatT]] = Broadcast(self._name)
        self._task: asyncio.Task[None] | None = None
        self._streams: tuple[
            FormulaEngine[SupportsFloatT],
            FormulaEngine[SupportsFloatT],
            FormulaEngine[SupportsFloatT],
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
        self, name: str | None = None, max_size: int = 50
    ) -> Receiver[Sample3Phase[SupportsFloatT]]:
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


class FormulaBuilder(Generic[SupportsFloatInputT, SupportsFloatOutputT]):
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

    def __init__(
        self, name: str, create_method: Callable[[float], SupportsFloatOutputT]
    ) -> None:
        """Create a `FormulaBuilder` instance.

        Args:
            name: A name for the formula being built.
            create_method: A method to generate the output `Sample` value with.  If the
                formula is for generating power values, this would be
                `Power.from_watts`, for example.
        """
        self._name = name
        self._create_method: Callable[[float], SupportsFloatOutputT] = create_method
        self._build_stack: list[FormulaStep] = []
        self._steps: list[FormulaStep] = []
        self._metric_fetchers: dict[str, MetricFetcher[SupportsFloatInputT]] = {}

    def push_oper(self, oper: str) -> None:  # pylint: disable=too-many-branches
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
        elif oper == "max":
            self._build_stack.append(Maximizer())
        elif oper == "min":
            self._build_stack.append(Minimizer())
        elif oper == "consumption":
            self._build_stack.append(Consumption())
        elif oper == "production":
            self._build_stack.append(Production())

    def push_metric(
        self,
        name: str,
        data_stream: Receiver[Sample[SupportsFloatInputT]],
        *,
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
            name, MetricFetcher(name, data_stream, nones_are_zeros=nones_are_zeros)
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
    ) -> tuple[list[FormulaStep], dict[str, MetricFetcher[SupportsFloatInputT]]]:
        """Finalize and return the steps and fetchers for the formula.

        Returns:
            A tuple of the steps and fetchers for the formula.
        """
        while self._build_stack:
            self._steps.append(self._build_stack.pop())

        return self._steps, self._metric_fetchers

    def __str__(self) -> str:
        """Return a string representation of the formula.

        Returns:
            A string representation of the formula.
        """
        steps = self._steps if len(self._steps) > 0 else self._build_stack
        return format_formula(steps)

    def build(self) -> FormulaEngine[SupportsFloatInputT, SupportsFloatOutputT]:
        """Create a formula engine with the steps and fetchers that have been pushed.

        Returns:
            A `FormulaEngine` instance.
        """
        self.finalize()
        return FormulaEngine(self, create_method=self._create_method)


class _BaseHOFormulaBuilder(ABC, Generic[SupportsFloatInputT, SupportsFloatOutputT]):
    """Provides a way to build formulas from the outputs of other formulas."""

    def __init__(
        self,
        engine: (
            FormulaEngine[SupportsFloatInputT, SupportsFloatOutputT]
            | FormulaEngine3Phase[SupportsFloatT]
        ),
        create_method: Callable[[float], SupportsFloatOutputT],
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
                FormulaEngine[SupportsFloatInputT, SupportsFloatOutputT]
                | FormulaEngine3Phase[SupportsFloatT]
                | Quantity
                | float
                | str,
            ]
        ] = deque()
        self._steps.append((TokenType.COMPONENT_METRIC, engine))
        self._create_method: Callable[[float], SupportsFloatOutputT] = create_method

    @overload
    def _push(
        self, oper: str, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[SupportsFloatInputT, SupportsFloatOutputT]: ...

    @overload
    def _push(
        self, oper: str, other: _CompositionType3Phase | SupportsFloatT | float
    ) -> HigherOrderFormulaBuilder3Phase[SupportsFloatT]: ...

    def _push(
        self, oper: str, other: _CompositionType | SupportsFloatT | float
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
    ):
        self._steps.appendleft((TokenType.OPER, "("))
        self._steps.append((TokenType.OPER, ")"))
        self._steps.append((TokenType.OPER, oper))

        if isinstance(other, (FormulaEngine, FormulaEngine3Phase)):
            self._steps.append((TokenType.COMPONENT_METRIC, other))
        elif isinstance(other, (Quantity, float, int)):
            match oper:
                case "+" | "-" | "max" | "min":
                    if not isinstance(other, Quantity):
                        raise RuntimeError(
                            "A Quantity must be provided for addition,"
                            f" subtraction, min or max to {other}"
                        )
                case "*" | "/":
                    if not isinstance(other, (float, int)):
                        raise RuntimeError(
                            f"A float must be provided for scalar multiplication to {other}"
                        )
            self._steps.append((TokenType.CONSTANT, other))
        elif isinstance(other, _BaseHOFormulaBuilder):
            self._steps.append((TokenType.OPER, "("))
            self._steps.extend(other._steps)  # pylint: disable=protected-access
            self._steps.append((TokenType.OPER, ")"))
        else:
            raise RuntimeError(f"Can't build a formula from: {other}")
        assert isinstance(
            self, (HigherOrderFormulaBuilder, HigherOrderFormulaBuilder3Phase)
        )
        return self

    @overload
    def __add__(
        self, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[SupportsFloatT]: ...

    @overload
    def __add__(
        self, other: _CompositionType3Phase | SupportsFloatT
    ) -> HigherOrderFormulaBuilder3Phase[SupportsFloatT]: ...

    def __add__(
        self, other: _CompositionType | SupportsFloatT
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
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
    ) -> HigherOrderFormulaBuilder[SupportsFloatT]: ...

    @overload
    def __sub__(
        self, other: _CompositionType3Phase | SupportsFloatT
    ) -> HigherOrderFormulaBuilder3Phase[SupportsFloatT]: ...

    def __sub__(
        self,
        other: _CompositionType | SupportsFloatT,
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
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
    ) -> HigherOrderFormulaBuilder[SupportsFloatT]: ...

    @overload
    def __mul__(
        self, other: _CompositionType3Phase | float
    ) -> HigherOrderFormulaBuilder3Phase[SupportsFloatT]: ...

    def __mul__(
        self,
        other: _CompositionType | float,
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
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
    ) -> HigherOrderFormulaBuilder[SupportsFloatT]: ...

    @overload
    def __truediv__(
        self, other: _CompositionType3Phase | float
    ) -> HigherOrderFormulaBuilder3Phase[SupportsFloatT]: ...

    def __truediv__(
        self,
        other: _CompositionType | float,
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
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

    @overload
    def max(
        self, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[SupportsFloatT]: ...

    @overload
    def max(
        self, other: _CompositionType3Phase | SupportsFloatT
    ) -> HigherOrderFormulaBuilder3Phase[SupportsFloatT]: ...

    def max(
        self, other: _CompositionType | SupportsFloatT
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
    ):
        """Return a formula builder that calculates the maximum of `self` and `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("max", other)

    @overload
    def min(
        self, other: _CompositionType1Phase
    ) -> HigherOrderFormulaBuilder[SupportsFloatT]: ...

    @overload
    def min(
        self, other: _CompositionType3Phase | SupportsFloatT
    ) -> HigherOrderFormulaBuilder3Phase[SupportsFloatT]: ...

    def min(
        self, other: _CompositionType | SupportsFloatT
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
    ):
        """Return a formula builder that calculates the minimum of `self` and `other`.

        Args:
            other: A formula receiver, or a formula builder instance corresponding to a
                sub-expression.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        return self._push("min", other)

    def consumption(
        self,
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
    ):
        """Apply the Consumption Operator.

        The consumption operator returns either the identity if the power value is
        positive or 0.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        self._steps.appendleft((TokenType.OPER, "("))
        self._steps.append((TokenType.OPER, ")"))
        self._steps.append((TokenType.OPER, "consumption"))
        assert isinstance(
            self, (HigherOrderFormulaBuilder, HigherOrderFormulaBuilder3Phase)
        )
        return self

    def production(
        self,
    ) -> (
        HigherOrderFormulaBuilder[SupportsFloatT]
        | HigherOrderFormulaBuilder3Phase[SupportsFloatT]
    ):
        """Apply the Production Operator.

        The production operator returns either the absolute value if the power value is
        negative or 0.

        Returns:
            A formula builder that can take further expressions, or can be built
                into a formula engine.
        """
        self._steps.appendleft((TokenType.OPER, "("))
        self._steps.append((TokenType.OPER, ")"))
        self._steps.append((TokenType.OPER, "production"))
        assert isinstance(
            self, (HigherOrderFormulaBuilder, HigherOrderFormulaBuilder3Phase)
        )
        return self


class HigherOrderFormulaBuilder(
    _BaseHOFormulaBuilder[SupportsFloatInputT, SupportsFloatT]
):
    """A specialization of the _BaseHOFormulaBuilder for `FormulaReceiver`."""

    def build(
        self, name: str, *, nones_are_zeros: bool = False
    ) -> FormulaEngine[SupportsFloatInputT, SupportsFloatT]:
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
                    nones_are_zeros=nones_are_zeros,
                )
            elif typ == TokenType.OPER:
                assert isinstance(value, str)
                builder.push_oper(value)
            elif typ == TokenType.CONSTANT:
                assert isinstance(value, (Quantity, float))
                builder.push_constant(
                    float(value) if isinstance(value, Quantity) else value
                )
        return builder.build()


class HigherOrderFormulaBuilder3Phase(
    _BaseHOFormulaBuilder[SupportsFloatInputT, SupportsFloatT]
):
    """A specialization of the _BaseHOFormulaBuilder for `FormulaReceiver3Phase`."""

    def build(
        self, name: str, *, nones_are_zeros: bool = False
    ) -> FormulaEngine3Phase[SupportsFloatT]:
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
                        nones_are_zeros=nones_are_zeros,
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
