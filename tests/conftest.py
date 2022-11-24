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

    Since this is auto-use, the yield part (and restore of the variable) is not
    strictly needed, but we leave it as an example.

    Note: Test class must derive after unittest.IsolatedAsyncioTestCase.
    Otherwise this fixture won't run.
    """
    original_restart_limit = _decorator.BaseActor.restart_limit
    _decorator.BaseActor.restart_limit = 0
    yield
    _decorator.BaseActor.restart_limit = original_restart_limit
