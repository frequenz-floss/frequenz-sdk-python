# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Setup for all the tests."""
import pytest

from frequenz.sdk.actor import _decorator

# Used to impose a hard time limit for some async tasks in tests so that tests don't
# run forever in case of a bug
SAFETY_TIMEOUT = 10.0


@pytest.fixture(scope="session", autouse=True)
def disable_actor_auto_restart():  # type: ignore
    """Disable auto-restart of actors while running tests.

    At some point we had a version that would set the limit back to the
    original value but it doesn't work because some actors will keep running
    even after the end of the session and fail after the original value was
    reestablished, getting into an infinite loop again.

    Note: Test class must derive after unittest.IsolatedAsyncioTestCase.
    Otherwise this fixture won't run.
    """
    _decorator.BaseActor.restart_limit = 0
