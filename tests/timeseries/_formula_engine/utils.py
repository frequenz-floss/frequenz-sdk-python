# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utils for testing formula engines."""


from collections.abc import Callable
from math import isclose

from frequenz.channels import Receiver

from frequenz.sdk.microgrid import _data_pipeline
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._formula_engine import ResampledFormulaBuilder
from frequenz.sdk.timeseries._quantities import QuantityT


def get_resampled_stream(
    namespace: str,
    comp_id: int,
    metric_id: ComponentMetricId,
    create_method: Callable[[float], QuantityT],
) -> Receiver[Sample[QuantityT]]:
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
        create_method,
    )
    # Resampled data is always `Quantity` type, so we need to convert it to the desired
    # output type.
    return builder._get_resampled_receiver(
        comp_id,
        metric_id,
    ).map(
        lambda sample: Sample(
            sample.timestamp,
            None if sample.value is None else create_method(sample.value.base_value),
        )
    )
    # pylint: enable=protected-access


def equal_float_lists(list1: list[QuantityT], list2: list[QuantityT]) -> bool:
    """Compare two float lists with `math.isclose()`."""
    return (
        len(list1) > 0
        and len(list1) == len(list2)
        and all(isclose(v1.base_value, v2.base_value) for v1, v2 in zip(list1, list2))
    )
