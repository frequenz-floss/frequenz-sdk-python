# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utility functions to run and synchronize the execution of actors."""


import asyncio
from typing import Any

from ._decorator import BaseActor


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

    await asyncio.gather(*(actor.join() for actor in actors))
