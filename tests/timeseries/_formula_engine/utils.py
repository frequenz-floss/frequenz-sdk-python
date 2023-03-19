# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utils for testing formula engines."""

from __future__ import annotations

from datetime import datetime
from math import isclose

from frequenz.channels import Receiver

from frequenz.sdk.microgrid import _data_pipeline
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import Sample, Sample3Phase
from frequenz.sdk.timeseries._formula_engine import (
    FormulaReceiver,
    FormulaReceiver3Phase,
    ResampledFormulaBuilder,
)


async def get_resampled_stream(  # pylint: disable=too-many-arguments
    comp_id: int,
    metric_id: ComponentMetricId,
) -> Receiver[Sample]:
    """Return the resampled data stream for the given component."""
    # Create a `FormulaBuilder` instance, just in order to reuse its
    # `_get_resampled_receiver` function implementation.

    # pylint: disable=protected-access
    builder = ResampledFormulaBuilder(
        _data_pipeline._get().logical_meter()._namespace,
        "",
        _data_pipeline._get()._channel_registry,
        _data_pipeline._get()._resampling_request_sender(),
        metric_id,
    )
    return await builder._get_resampled_receiver(
        comp_id,
        metric_id,
    )
    # pylint: enable=protected-access


def equal_float_lists(list1: list[float], list2: list[float]) -> bool:
    """Compare two float lists with `math.isclose()`."""
    return (
        len(list1) > 0
        and len(list1) == len(list2)
        and all(isclose(v1, v2) for v1, v2 in zip(list1, list2))
    )


async def synchronize_receivers(
    receivers: list[FormulaReceiver | FormulaReceiver3Phase | Receiver[Sample]],
) -> None:
    """Check if given receivers are all returning the same timestamp.

    If not, try to synchronize them.
    """
    by_ts: dict[
        datetime, list[FormulaReceiver | FormulaReceiver3Phase | Receiver[Sample]]
    ] = {}
    for recv in receivers:
        while True:
            sample = await recv.receive()
            assert sample is not None
            if isinstance(sample, Sample) and sample.value is None:
                continue
            if isinstance(sample, Sample3Phase) and sample.value_p1 is None:
                continue
            by_ts.setdefault(sample.timestamp, []).append(recv)
            break
    latest_ts = max(by_ts)

    for sample_ts, recvs in by_ts.items():
        if sample_ts == latest_ts:
            continue
        while sample_ts < latest_ts:
            for recv in recvs:
                val = await recv.receive()
                assert val is not None
                sample_ts = val.timestamp
