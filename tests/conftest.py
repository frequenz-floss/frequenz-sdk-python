# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Setup for all the tests."""
import pytest

from frequenz.sdk.actor import _actor

# Used to impose a hard time limit for some async tasks in tests so that tests don't
# run forever in case of a bug
SAFETY_TIMEOUT = 10.0


@pytest.fixture(scope="session", autouse=True)
def disable_actor_auto_restart() -> collections.abc.Iterator[None]:
    """Disable auto-restart of actors while running tests."""
    # pylint: disable=protected-access
    original_limit = _actor.Actor._restart_limit
    _actor.Actor._restart_limit = 0
    yield
    _actor.Actor._restart_limit = original_limit
