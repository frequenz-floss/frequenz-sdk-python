# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utils for testing formula engines."""

from __future__ import annotations

from math import isclose
from typing import Type

from frequenz.channels import Receiver

from frequenz.sdk.microgrid import _data_pipeline
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._formula_engine import ResampledFormulaBuilder
from frequenz.sdk.timeseries._quantities import Quantity


def get_resampled_stream(  # pylint: disable=too-many-arguments
    namespace: str,
    comp_id: int,
    metric_id: ComponentMetricId,
    output_type: Type[Quantity],
) -> Receiver[Sample[Quantity]]:
    """Return the resampled data stream for the given component."""
    # Create a `FormulaBuilder` instance, just in order to reuse its
    # `_get_resampled_receiver` function implementation.

    # pylint: disable=protected-access
    builder = ResampledFormulaBuilder(
        namespace,
        "",
        _data_pipeline._get()._channel_registry,
        _data_pipeline._get()._resampling_request_sender(),
        metric_id,
        output_type,
    )
    return builder._get_resampled_receiver(
        comp_id,
        metric_id,
    )
    # pylint: enable=protected-access


def equal_float_lists(list1: list[Quantity], list2: list[Quantity]) -> bool:
    """Compare two float lists with `math.isclose()`."""
    return (
        len(list1) > 0
        and len(list1) == len(list2)
        and all(isclose(v1.base_value, v2.base_value) for v1, v2 in zip(list1, list2))
    )
