# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Benchmark resampling."""

from datetime import datetime, timedelta, timezone
from timeit import timeit
from typing import Sequence

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._quantities import Quantity
from frequenz.sdk.timeseries._resampling import (
    ResamplerConfig,
    SourceProperties,
    _ResamplingHelper,
)


def nop(  # pylint: disable=unused-argument
    samples: Sequence[Sample[Quantity]],
    resampler_config: ResamplerConfig,
    source_properties: SourceProperties,
) -> float:
    """Return 0.0."""
    return 0.0


def _benchmark_resampling_helper(resamples: int, samples: int) -> None:
    """Benchmark the resampling helper."""
    helper = _ResamplingHelper(
        "benchmark",
        ResamplerConfig(
            resampling_period=timedelta(seconds=1.0),
            max_data_age_in_periods=3.0,
            resampling_function=nop,
            initial_buffer_len=samples * 3,
            warn_buffer_len=samples * 3 + 2,
            max_buffer_len=samples * 3 + 3,
        ),
    )
    now = datetime.now(timezone.utc)

    def _do_work() -> None:
        nonlocal now
        for _n_resample in range(resamples):
            for _n_sample in range(samples):
                now = now + timedelta(seconds=1 / samples)
                helper.add_sample(Sample(now, Quantity(0.0)))
            helper.resample(now)

    print(timeit(_do_work, number=5))


def _benchmark() -> None:
    for resamples in [10, 100, 1000]:
        for samples in [10, 100, 1000]:
            print(f"{resamples=} {samples=}")
            _benchmark_resampling_helper(resamples, samples)


if __name__ == "__main__":
    _benchmark()
