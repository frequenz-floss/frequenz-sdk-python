"""Setup for all the tests.

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import pytest

from frequenz.sdk.actor import decorator


@pytest.fixture(scope="session", autouse=True)
def disable_actor_auto_restart():  # type: ignore
    """Disable auto-restart of actors while running tests.

    Since this is auto-use, the yield part (and restore of the variable) is not
    strictly needed, but we leave it as an example.

    Note: Test class must derive after unittest.IsolatedAsyncioTestCase.
    Otherwise this fixture won't run.
    """
    original_restart_limit = decorator.BaseActor.restart_limit
    decorator.BaseActor.restart_limit = 0
    yield
    decorator.BaseActor.restart_limit = original_restart_limit
