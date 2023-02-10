# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utility functions to run and synchronize the execution of actors."""


import asyncio
import logging
from typing import Any

from ._decorator import BaseActor

_logger = logging.getLogger(__name__)


async def run(*actors: Any) -> None:
    """Await the completion of all actors.

    Args:
        actors: the actors to be awaited.

    Raises:
        AssertionError: if any of the actors is not an instance of BaseActor.
    """
    # Check that each actor is an instance of BaseActor at runtime,
    # due to the indirection created by the actor decorator.
    for actor in actors:
        assert isinstance(actor, BaseActor), f"{actor} is not an instance of BaseActor"

    pending_tasks = set()
    for actor in actors:
        pending_tasks.add(asyncio.create_task(actor.join(), name=str(actor)))

    # Currently the actor decorator manages the life-cycle of the actor tasks
    while pending_tasks:
        done_tasks, pending_tasks = await asyncio.wait(
            pending_tasks, return_when=asyncio.FIRST_COMPLETED
        )

        # This should always be only one task, but we handle many for extra safety
        for task in done_tasks:
            # Cancellation needs to be checked first, otherwise the other methods
            # could raise a CancelledError
            if task.cancelled():
                _logger.info("The actor %s was cancelled", task.get_name())
            elif exception := task.exception():
                _logger.error(
                    "The actor %s was finished due to an uncaught exception",
                    task.get_name(),
                    exc_info=exception,
                )
            else:
                _logger.info("The actor %s finished normally", task.get_name())
