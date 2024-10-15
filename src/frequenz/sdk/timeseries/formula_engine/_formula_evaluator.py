# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A post-fix formula evaluator that operates on `Sample` receivers."""

import asyncio
from collections.abc import Callable
from datetime import datetime
from math import isinf, isnan
from typing import Generic

from .._base_types import QuantityT, Sample
from ._formula_steps import FormulaStep, MetricFetcher


class FormulaEvaluator(Generic[QuantityT]):
    """A post-fix formula evaluator that operates on `Sample` receivers."""

    def __init__(
        self,
        name: str,
        steps: list[FormulaStep],
        metric_fetchers: dict[str, MetricFetcher[QuantityT]],
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
        self._metric_fetchers: dict[str, MetricFetcher[QuantityT]] = metric_fetchers
        self._first_run = True
        self._create_method: Callable[[float], QuantityT] = create_method

    async def _synchronize_metric_timestamps(
        self, metrics: set[asyncio.Task[Sample[QuantityT] | None]]
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
        metrics_by_ts: dict[datetime, list[str]] = {}
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
        eval_stack: list[float] = []
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
