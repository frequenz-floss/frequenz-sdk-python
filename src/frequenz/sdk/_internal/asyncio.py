# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""General purpose async tools."""

import asyncio


async def cancel_and_await(task: asyncio.Task) -> None:
    """Cancel a task and wait for it to finish.

    The `CancelledError` is suppresed, but any other exception will be propagated.

    Args:
        task: The task to be cancelled and waited for.
    """
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
