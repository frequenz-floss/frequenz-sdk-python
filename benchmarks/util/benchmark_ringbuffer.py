# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Performance test for the `Ringbuffer` class."""

import random
import timeit
from datetime import datetime, timedelta
from typing import TypeVar

import numpy as np

from frequenz.sdk.util.ringbuffer import OrderedRingBuffer

MINUTES_IN_A_DAY = 24 * 60
MINUTES_IN_29_DAYS = 29 * MINUTES_IN_A_DAY


T = TypeVar("T")


def fill_buffer(days: int, buffer: OrderedRingBuffer[T], element_type: type) -> None:
    """Fill the given buffer up to the given amount of days, one sample per minute."""
    random.seed(0)
    basetime = datetime(2022, 1, 1)

    for day in range(days):
        # Push in random order
        for i in random.sample(range(MINUTES_IN_A_DAY), MINUTES_IN_A_DAY):
            buffer.update(
                basetime + timedelta(days=day, minutes=i, seconds=i % 3),
                element_type(i),
            )


def test_days(days: int, buffer: OrderedRingBuffer[int]) -> None:
    """Fills a buffer completely up and then gets the data for each of the 29 days."""
    print(".", end="", flush=True)

    fill_buffer(days, buffer, int)

    basetime = datetime(2022, 1, 1)

    for day in range(days):
        # pylint: disable=unused-variable
        minutes = buffer.window(
            basetime + timedelta(days=day), basetime + timedelta(days=day + 1)
        )


def test_slices(days: int, buffer: OrderedRingBuffer[T]) -> None:
    """Benchmark slicing.

    Takes a buffer, fills it up and then excessively gets
    the data for each day to calculate the average/median.
    """
    print(".", end="", flush=True)
    fill_buffer(days, buffer, float)

    # Chose uneven starting point so that for the first/last window data has to
    # be copied
    basetime = datetime(2022, 1, 1, 0, 5, 13, 88)

    total_avg = 0.0
    total_median = 0.0

    for _ in range(5):
        for day in range(days):
            minutes = buffer.window(
                basetime + timedelta(days=day), basetime + timedelta(days=day + 1)
            )

            total_avg += float(np.average(minutes))
            total_median += float(np.median(minutes))


def test_29_days_list() -> None:
    """Run the 29 day test on the list backend."""
    test_days(29, OrderedRingBuffer([0] * MINUTES_IN_29_DAYS, 60))


def test_29_days_array() -> None:
    """Run the 29 day test on the array backend."""
    test_days(
        29,
        OrderedRingBuffer(
            np.empty(
                shape=MINUTES_IN_29_DAYS,
            ),
            60,
        ),
    )


def test_29_days_slicing_list() -> None:
    """Run slicing tests on list backend."""
    test_slices(29, OrderedRingBuffer([0] * MINUTES_IN_29_DAYS, 60))


def test_29_days_slicing_array() -> None:
    """Run slicing tests on array backend."""
    test_slices(
        29,
        OrderedRingBuffer(
            np.empty(
                shape=MINUTES_IN_29_DAYS,
            ),
            60,
        ),
    )


def main() -> None:
    """Run benchmark.

    Result of previous run:

    Date: Do 22. Dez 15:03:05 CET 2022
    Result:

           =========================================
    Array: ........................................
    List:  ........................................
    Time to fill 29 days with data:
    Array: 0.09411649959984061 seconds
    List:  0.0906366748000437 seconds
    Diff:  0.0034798247997969156
           =========================================
    Array: ........................................
    List:  ........................................
    Filling 29 days and running average & mean on every day:
    Array: 0.09842290654996759 seconds
    List:  0.1316629376997298 seconds
    Diff:  -0.03324003114976222
    """
    num_runs = 40

    print(f"       {''.join(['='] * (num_runs + 1))}")
    print("Array: ", end="")
    duration_array = timeit.Timer(test_29_days_array).timeit(number=num_runs)
    print("\nList:  ", end="")
    duration_list = timeit.Timer(test_29_days_list).timeit(number=num_runs)
    print("")

    print(
        "Time to fill 29 days with data:\n\t"
        + f"Array: {duration_array/num_runs} seconds\n\t"
        + f"List:  {duration_list/num_runs} seconds\n\t"
        + f"Diff:  {duration_array/num_runs -  duration_list/num_runs}"
    )

    print(f"       {''.join(['='] * (num_runs + 1))}")
    print("Array: ", end="")
    duration_array = timeit.Timer(test_29_days_slicing_array).timeit(number=num_runs)
    print("\nList:  ", end="")
    duration_list = timeit.Timer(test_29_days_slicing_list).timeit(number=num_runs)
    print("")

    print(
        "Filling 29 days and running average & mean on every day:\n\t"
        + f"Array: {duration_array/num_runs} seconds\n\t"
        + f"List:  {duration_list/num_runs} seconds\n\t"
        + f"Diff:  {duration_array/num_runs -  duration_list/num_runs}"
    )


if __name__ == "__main__":
    main()
