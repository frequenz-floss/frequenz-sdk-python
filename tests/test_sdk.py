# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Top level tests for the `frequenz.sdk` pacakge."""

import frequenz.sdk
from frequenz.sdk import config
from frequenz.sdk.actor import ConfigManagingActor


def test_sdk_import() -> None:
    """Checks that `import frequenz.sdk` works."""
    assert frequenz.sdk is not None


def test_sdk_import_config() -> None:
    """Checks that `import frequenz.sdk` works."""
    assert config is not None


def test_sdk_import_config_manager() -> None:
    """Checks that `import frequenz.sdk` works."""
    assert ConfigManagingActor is not None
