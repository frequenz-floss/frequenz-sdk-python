# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Performance test for the `Ringbuffer` class."""

import random
import timeit
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, TypeVar

import numpy as np

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._ringbuffer import OrderedRingBuffer

MINUTES_IN_A_DAY = 24 * 60
MINUTES_IN_29_DAYS = 29 * MINUTES_IN_A_DAY


T = TypeVar("T")


def fill_buffer(days: int, buffer: OrderedRingBuffer[Any]) -> None:
    """Fill the given buffer up to the given amount of days, one sample per minute."""
    random.seed(0)
    basetime = datetime(2022, 1, 1, tzinfo=timezone.utc)
    print("..filling", end="", flush=True)

    for day in range(days):
        # Push in random order
        for i in random.sample(range(MINUTES_IN_A_DAY), MINUTES_IN_A_DAY):
            buffer.update(
                Sample(basetime + timedelta(days=day, minutes=i, seconds=i % 3))
            )


def test_days(days: int, buffer: OrderedRingBuffer[Any]) -> None:
    """Gets the data for each of the 29 days."""
    basetime = datetime(2022, 1, 1, tzinfo=timezone.utc)

    for day in range(days):
        # pylint: disable=unused-variable
        minutes = buffer.window(
            basetime + timedelta(days=day), basetime + timedelta(days=day + 1)
        )


def test_slices(days: int, buffer: OrderedRingBuffer[Any], median: bool) -> None:
    """Benchmark slicing.

    Takes a buffer, fills it up and then excessively gets
    the data for each day to calculate the average/median.
    """
    basetime = datetime(2022, 1, 1, tzinfo=timezone.utc)

    total = 0.0

    for _ in range(3):
        for day in range(days):
            minutes = buffer.window(
                basetime + timedelta(days=day), basetime + timedelta(days=day + 1)
            )

            if median:
                total += float(np.median(minutes))
            else:
                total += float(np.average(minutes))


def test_29_days_list(num_runs: int) -> Dict[str, float]:
    """Run the 29 day test on the list backend."""
    days = 29
    buffer = OrderedRingBuffer([0.0] * MINUTES_IN_29_DAYS, timedelta(minutes=1))

    fill_time = timeit.Timer(lambda: fill_buffer(days, buffer)).timeit(number=1)
    test_time = timeit.Timer(lambda: test_days(days, buffer)).timeit(number=num_runs)
    return {"fill": fill_time, "test": test_time}


def test_29_days_array(num_runs: int) -> Dict[str, float]:
    """Run the 29 day test on the array backend."""
    days = 29
    buffer = OrderedRingBuffer(
        np.empty(
            shape=MINUTES_IN_29_DAYS,
        ),
        timedelta(minutes=1),
    )

    fill_time = timeit.Timer(lambda: fill_buffer(days, buffer)).timeit(number=1)
    test_time = timeit.Timer(lambda: test_days(days, buffer)).timeit(number=num_runs)
    return {"fill": fill_time, "test": test_time}


def test_29_days_slicing_list(num_runs: int) -> Dict[str, float]:
    """Run slicing tests on list backend."""
    days = 29
    buffer = OrderedRingBuffer([0.0] * MINUTES_IN_29_DAYS, timedelta(minutes=1))

    fill_time = timeit.Timer(lambda: fill_buffer(days, buffer)).timeit(number=1)
    median_test_time = timeit.Timer(
        lambda: test_slices(days, buffer, median=True)
    ).timeit(number=num_runs)
    avg_test_time = timeit.Timer(
        lambda: test_slices(days, buffer, median=False)
    ).timeit(number=num_runs)

    return {"fill": fill_time, "median": median_test_time, "avg": avg_test_time}


def test_29_days_slicing_array(num_runs: int) -> Dict[str, float]:
    """Run slicing tests on array backend."""
    days = 29
    buffer = OrderedRingBuffer(
        np.empty(
            shape=MINUTES_IN_29_DAYS,
        ),
        timedelta(minutes=1),
    )

    fill_time = timeit.Timer(lambda: fill_buffer(days, buffer)).timeit(number=1)
    median_test_time = timeit.Timer(
        lambda: test_slices(days, buffer, median=True)
    ).timeit(number=num_runs)
    avg_test_time = timeit.Timer(
        lambda: test_slices(days, buffer, median=False)
    ).timeit(number=num_runs)

    return {"fill": fill_time, "median": median_test_time, "avg": avg_test_time}


def main() -> None:
    """Run benchmark.

    Result of previous run:

    Date: Mi 1. Feb 17:19:51 CET 2023
    Result:

           =====================
    Array: ..filling
    List:  ..filling
    Time to fill 29 days with data:
            Array: 7.214875044999644 seconds
            List:  7.174421657982748 seconds
            Diff:  0.04045338701689616
    Day-Slices into 29 days with data:
            Array: 0.0001304591482039541 seconds
            List:  0.00019963659869972616 seconds
            Diff:  -6.917745049577205e-05
           =====================
    Array: ..filling
    List:  ..filling
    Avg of windows of 29 days and running average & mean on every day:
            Array: 0.0007829780981410295 seconds
            List:  0.0042931242496706545 seconds
            Diff:  -0.0035101461515296252
    Median of windows of 29 days and running average & mean on every day:
            Array: 0.0021195551002165304 seconds
            List:  0.00501448459981475 seconds
            Diff:  -0.00289492949959822
    """
    num_runs = 20

    print(f"       {''.join(['='] * (num_runs + 1))}")
    print("Array: ", end="")
    array_times = test_29_days_array(num_runs)

    print("\nList:  ", end="")

    list_times = test_29_days_list(num_runs)
    print("")

    print(
        "Time to fill 29 days with data:\n\t"
        + f"Array: {array_times['fill']} seconds\n\t"
        + f"List:  {list_times['fill']} seconds\n\t"
        + f"Diff:  {array_times['fill'] -  list_times['fill']}"
    )

    print(
        "Day-Slices into 29 days with data:\n\t"
        + f"Array: {array_times['test']/num_runs} seconds\n\t"
        + f"List:  {list_times['test']/num_runs} seconds\n\t"
        + f"Diff:  {array_times['test']/num_runs -  list_times['test']/num_runs}"
    )

    print(f"       {''.join(['='] * (num_runs + 1))}")
    print("Array: ", end="")
    slicing_array_times = test_29_days_slicing_array(num_runs)
    print("\nList:  ", end="")
    slicing_list_times = test_29_days_slicing_list(num_runs)
    print("")

    print(
        "Avg of windows of 29 days and running average & mean on every day:\n\t"
        + f"Array: {slicing_array_times['avg']/num_runs} seconds\n\t"
        + f"List:  {slicing_list_times['avg']/num_runs} seconds\n\t"
        + f"Diff:  {slicing_array_times['avg']/num_runs -  slicing_list_times['avg']/num_runs}"
    )

    print(
        "Median of windows of 29 days and running average & mean on every day:\n\t"
        + f"Array: {slicing_array_times['median']/num_runs} seconds\n\t"
        + f"List:  {slicing_list_times['median']/num_runs} seconds\n\t"
        + "Diff:  "
        + f"{slicing_array_times['median']/num_runs -  slicing_list_times['median']/num_runs}"
    )


if __name__ == "__main__":
    main()
