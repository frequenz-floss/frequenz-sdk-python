# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A formula engine that can apply formulas on streaming data."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set

from frequenz.channels import Receiver

from .._sample import Sample
from ._formula_steps import (
    Adder,
    Divider,
    FormulaStep,
    MetricFetcher,
    Multiplier,
    OpenParen,
    Subtractor,
)

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

    Operators and metrics need to be pushed into the engine in in-fix order, and they
    get rearranged into post-fix in the engine.  This is done using the [Shunting yard
    algorithm](https://en.wikipedia.org/wiki/Shunting_yard_algorithm).

    Example:
        To create an engine that adds the latest entries from two receivers, the
        following calls need to be made:

        ```python
        engine = FormulaEngine()
        engine.push_metric("metric_1", receiver_1)
        engine.push_oper("+")
        engine.push_metric("metric_2", receiver_2)
        engine.finalize()
        ```

        and then every call to `engine.apply()` would fetch a value from each receiver,
        add the values and return the result.
    """

    def __init__(
        self,
    ) -> None:
        """Create a `FormulaEngine` instance."""
        self._steps: List[FormulaStep] = []
        self._build_stack: List[FormulaStep] = []
        self._metric_fetchers: Dict[str, MetricFetcher] = {}
        self._first_run = True

    def push_oper(self, oper: str) -> None:
        """Push an operator into the engine.

        Args:
            oper: One of these strings - "+", "-", "*", "/", "(", ")"
        """
        if self._build_stack and oper != "(":
            op_prec = _operator_precedence[oper]
            while self._build_stack:
                prev_step = self._build_stack[-1]
                if op_prec <= _operator_precedence[repr(prev_step)]:
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

    def push_metric(self, name: str, data_stream: Receiver[Sample]) -> None:
        """Push a metric receiver into the engine.

        Args:
            name: A name for the metric.
            data_stream: A receiver to fetch this metric from.
        """
        fetcher = self._metric_fetchers.setdefault(
            name, MetricFetcher(name, data_stream)
        )
        self._steps.append(fetcher)

    def finalize(self) -> None:
        """Finalize the formula engine.

        This function must be called before calls to `apply` can be made.
        """
        while self._build_stack:
            self._steps.append(self._build_stack.pop())

    async def synchronize_metric_timestamps(
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
                    "Unable to synchronize timestamps of resampled metrics"
                )
        self._first_run = False
        return latest_ts

    async def apply(self) -> Sample:
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
            raise RuntimeError("Some resampled metrics didn't arrive")

        if self._first_run:
            metric_ts = await self.synchronize_metric_timestamps(ready_metrics)
        else:
            res = next(iter(ready_metrics)).result()
            assert res is not None
            metric_ts = res.timestamp

        for step in self._steps:
            step.apply(eval_stack)

        # if all steps were applied and the formula was correct, there should only be a
        # single value in the evaluation stack, and that would be the formula result.
        if len(eval_stack) != 1:
            raise RuntimeError("Formula application failed.")

        return Sample(metric_ts, eval_stack[0])
