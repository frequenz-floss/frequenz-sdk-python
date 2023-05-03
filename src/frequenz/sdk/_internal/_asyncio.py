# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""General purpose async tools."""

from __future__ import annotations

import asyncio
from abc import ABC
from typing import Any


async def cancel_and_await(task: asyncio.Task[Any]) -> None:
    """Cancel a task and wait for it to finish.

    Exits immediately if the task is already done.

    The `CancelledError` is suppresed, but any other exception will be propagated.

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
