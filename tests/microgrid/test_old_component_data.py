# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for `frequenz.sdk.microgrid._old_component_data`."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    ComponentDataSamples,
    ComponentStateCode,
    ComponentStateSample,
)

from frequenz.sdk.microgrid._old_component_data import (
    BatteryData,
    ChpData,
    ComponentData,
    EVChargerData,
    InverterData,
    MeterData,
)


@pytest.mark.parametrize(
    "cls",
    [BatteryData, ChpData, EVChargerData, InverterData, MeterData],
)
def test_from_samples_preserves_state_without_metrics(
    cls: type[ComponentData],
) -> None:
    """`from_samples` must not drop a component state when no metrics are present.

    Regression test for
    https://github.com/frequenz-floss/frequenz-sdk-python/issues/1406:
    a `ComponentDataSamples` message that carries only a state sample (for example
    `ComponentStateCode.ERROR`) and no metric samples must still be converted into
    a concrete `ComponentData` instance carrying that state, so downstream code
    learns about the component's error status as soon as it happens.

    Previously, `from_samples` raised `ValueError("No metrics in the samples.")`
    and the state was silently swallowed by `_receive_logging_errors`.
    """
    component_id = ComponentId(1503)
    sampled_at = datetime(2026, 5, 21, 12, 37, 40, 107899, tzinfo=timezone.utc)
    samples = ComponentDataSamples(
        component_id=component_id,
        metric_samples=[],
        states=[
            ComponentStateSample(
                sampled_at=sampled_at,
                states=frozenset({ComponentStateCode.ERROR}),
                warnings=frozenset(),
                errors=frozenset(),
            ),
        ],
    )

    data = cls.from_samples(samples)

    assert isinstance(data, cls)
    assert data.component_id == component_id
    assert data.timestamp == sampled_at
    assert ComponentStateCode.ERROR in data.states
    assert not data.warnings
    assert not data.errors


def test_from_samples_warns_on_multiple_state_timestamps(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Multiple state samples with differing timestamps trigger a warning.

    When falling back to a state sample's timestamp (because there are no metric
    samples), `_from_samples` should warn if the state samples disagree on
    timestamps, mirroring the existing behaviour for metric samples.
    """
    component_id = ComponentId(1503)
    first = datetime(2026, 5, 21, 12, 37, 40, tzinfo=timezone.utc)
    second = first + timedelta(seconds=1)
    samples = ComponentDataSamples(
        component_id=component_id,
        metric_samples=[],
        states=[
            ComponentStateSample(
                sampled_at=first,
                states=frozenset({ComponentStateCode.ERROR}),
                warnings=frozenset(),
                errors=frozenset(),
            ),
            ComponentStateSample(
                sampled_at=second,
                states=frozenset({ComponentStateCode.ERROR}),
                warnings=frozenset(),
                errors=frozenset(),
            ),
        ],
    )

    with caplog.at_level(
        logging.WARNING, logger="frequenz.sdk.microgrid._old_component_data"
    ):
        data = InverterData.from_samples(samples)

    assert data.timestamp == second
    assert any(
        "multiple state sample timestamps" in record.message
        for record in caplog.records
    ), caplog.records
