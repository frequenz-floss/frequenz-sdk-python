# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Setup for all the tests."""
import contextlib
from collections.abc import Iterator
from datetime import timedelta

import pytest

from frequenz.sdk.actor import _actor

# Used to impose a hard time limit for some async tasks in tests so that tests don't
# run forever in case of a bug
SAFETY_TIMEOUT = timedelta(seconds=10.0)


@contextlib.contextmanager
def actor_restart_limit(limit: int) -> Iterator[None]:
    """Temporarily set the actor restart limit to a given value.

    Example:
        ```python
        with actor_restart_limit(0):  # No restart
            async with MyActor() as actor:
                # Do something with actor
        ```

    Args:
        limit: The new limit.
    """
    # pylint: disable=protected-access
    original_limit = _actor.Actor._restart_limit
    print(
        f"<actor_restart_limit> Changing the restart limit from {original_limit} to {limit}"
    )
    _actor.Actor._restart_limit = limit
    yield
    print(f"<actor_restart_limit> Resetting restart limit to {original_limit}")
    _actor.Actor._restart_limit = original_limit


@pytest.fixture(scope="session", autouse=True)
def disable_actor_auto_restart() -> Iterator[None]:
    """Disable auto-restart of actors while running tests."""
    with actor_restart_limit(0):
        yield


@pytest.fixture
def actor_auto_restart_once() -> Iterator[None]:
    """Make actors restart only once."""
    with actor_restart_limit(1):
        yield
