# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""General purpose async tools."""


import asyncio
import logging
from abc import ABC
from datetime import timedelta
from typing import Any, Callable, Coroutine

_logger = logging.getLogger(__name__)


async def cancel_and_await(task: asyncio.Task[Any]) -> None:
    """Cancel a task and wait for it to finish.

    Exits immediately if the task is already done.

    The `CancelledError` is suppressed, but any other exception will be propagated.

    Args:
        task: The task to be cancelled and waited for.
    """
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def run_forever(
    async_callable: Callable[[], Coroutine[Any, Any, None]],
    interval: timedelta = timedelta(seconds=1),
) -> None:
    """Run a given function forever, restarting it after any exception.

    Args:
        async_callable: The async callable to run.
        interval: The interval between restarts.
    """
    interval_s = interval.total_seconds()
    while True:
        try:
            await async_callable()
        except Exception:  # pylint: disable=broad-except
            _logger.exception("Restarting after exception")
            await asyncio.sleep(interval_s)


class NotSyncConstructible(AssertionError):
    """Raised when object with async constructor is created in sync way."""


class AsyncConstructible(ABC):
    """Parent class for classes where part of the constructor is async."""

    def __init__(self) -> None:
        """Raise error when object is created in sync way.

        Raises:
            NotSyncConstructible: If this method is called.
        """
        raise NotSyncConstructible(
            "This object shouldn't be created with default constructor. ",
            "Check class documentation for more information.",
        )
